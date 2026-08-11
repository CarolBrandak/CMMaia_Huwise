#!/usr/bin/env python3
"""Publica uma tabela da base de dados Aiven no dados.gov.pt."""

from __future__ import annotations

import argparse
import csv
import json
import os
import ssl
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import quote, urlsplit

import pymysql
import requests
from dotenv import dotenv_values


TABLE_CONFIG_PATH = Path(__file__).with_name("dados_gov_tables.json")
REQUEST_TIMEOUT = 120
SPATIAL_TYPES = {
    "geometry",
    "point",
    "linestring",
    "polygon",
    "multipoint",
    "multilinestring",
    "multipolygon",
    "geometrycollection",
}


class ScriptError(Exception):
    """Erro que pode ser apresentado no terminal sem expor credenciais."""


class DadosGovHTTPError(ScriptError):
    def __init__(self, status_code: int, method: str, path: str, detail: str) -> None:
        self.status_code = status_code
        self.method = method.upper()
        self.path = path
        self.detail = detail
        suffix = f": {detail[:500]}" if detail else ""
        super().__init__(
            f"o dados.gov.pt respondeu com HTTP {status_code} "
            f"em {self.method} {path}{suffix}"
        )


@dataclass(frozen=True)
class AivenConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    ca_cert: Path | None


@dataclass(frozen=True)
class Settings:
    aiven: AivenConfig
    api_url: str
    api_key: str
    organization_id: str
    private: bool


@dataclass(frozen=True)
class TableConfig:
    table: str
    dataset_id: str | None
    resource_id: str | None
    title: str
    description: str | None
    tags: tuple[str, ...]
    license_id: str | None
    frequency: str | None
    resource_title: str
    resource_description: str | None
    resource_format: str
    geometry_column: str | None
    spatial: Mapping[str, Any] | None


@dataclass(frozen=True)
class ExportedTable:
    path: Path
    row_count: int
    columns: tuple[Mapping[str, Any], ...]
    catalog_metadata: Mapping[str, Any]
    resource_format: str
    media_type: str


def _is_missing(value: str | None) -> bool:
    if value is None or not value.strip():
        return True
    normalized = value.strip().casefold()
    return normalized in {"changeme", "replace_me", "placeholder", "todo", "..."} or (
        normalized.startswith("<") and normalized.endswith(">")
    )


def _optional_text(entry: Mapping[str, Any], name: str, table: str) -> str | None:
    value = entry.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ScriptError(f"{name} invalido para a tabela {table}")
    return value.strip()


def load_table_config(
    table: str, config_path: str | Path = TABLE_CONFIG_PATH
) -> TableConfig:
    path = Path(config_path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ScriptError(f"nao foi encontrado o ficheiro {path.name}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ScriptError(f"nao foi possivel ler {path.name}") from exc

    if not isinstance(value, dict):
        raise ScriptError(f"{path.name} deve conter um objeto JSON")
    entry = value.get(table)
    if not isinstance(entry, dict):
        raise ScriptError(
            f"a tabela {table} nao esta configurada em {path.name}"
        )

    title = entry.get("title")
    tags = entry.get("tags", [])
    spatial = entry.get("spatial")
    if not isinstance(title, str) or not title.strip():
        raise ScriptError(f"title invalido para a tabela {table}")
    if not isinstance(tags, list) or any(
        not isinstance(tag, str) or not tag.strip() for tag in tags
    ):
        raise ScriptError(f"tags invalidas para a tabela {table}")
    if spatial is not None and not isinstance(spatial, dict):
        raise ScriptError(f"spatial invalido para a tabela {table}")

    resource_format = str(entry.get("resource_format", "csv")).strip().casefold()
    if resource_format not in {"csv", "geojson"}:
        raise ScriptError(
            f"resource_format deve ser csv ou geojson para a tabela {table}"
        )
    geometry_column = _optional_text(entry, "geometry_column", table)
    if resource_format == "geojson" and geometry_column is None:
        raise ScriptError(
            f"geometry_column e obrigatoria para a tabela GeoJSON {table}"
        )

    resource_title = entry.get("resource_title", f"{table}.{resource_format}")
    if not isinstance(resource_title, str) or not resource_title.strip():
        raise ScriptError(f"resource_title invalido para a tabela {table}")

    return TableConfig(
        table=table,
        dataset_id=_optional_text(entry, "dataset_id", table),
        resource_id=_optional_text(entry, "resource_id", table),
        title=title.strip(),
        description=_optional_text(entry, "description", table),
        tags=tuple(tag.strip() for tag in tags),
        license_id=_optional_text(entry, "license", table),
        frequency=_optional_text(entry, "frequency", table),
        resource_title=resource_title.strip(),
        resource_description=_optional_text(entry, "resource_description", table),
        resource_format=resource_format,
        geometry_column=geometry_column,
        spatial=dict(spatial) if spatial is not None else None,
    )


def save_remote_ids(
    table: str,
    *,
    dataset_id: str | None = None,
    resource_id: str | None = None,
    config_path: str | Path = TABLE_CONFIG_PATH,
) -> None:
    path = Path(config_path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        entry = value[table]
        if not isinstance(value, dict) or not isinstance(entry, dict):
            raise ValueError
        if dataset_id is not None:
            entry["dataset_id"] = dataset_id
        if resource_id is not None:
            entry["resource_id"] = resource_id

        temporary_path = path.with_suffix(path.suffix + ".tmp")
        temporary_path.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary_path, path)
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        identifiers = ", ".join(
            value
            for value in (
                f"dataset_id={dataset_id}" if dataset_id else "",
                f"resource_id={resource_id}" if resource_id else "",
            )
            if value
        )
        raise ScriptError(
            f"o objeto foi criado no dados.gov.pt ({identifiers}), mas nao foi "
            f"possivel guardar o identificador em {path.name}"
        ) from exc


def _parse_bool(value: str, variable: str) -> bool:
    normalized = value.strip().casefold()
    if normalized in {"1", "true", "yes", "sim"}:
        return True
    if normalized in {"0", "false", "no", "nao", "não"}:
        return False
    raise ScriptError(f"{variable} deve ter o valor true ou false")


def _api_url(value: str) -> str:
    url = value.strip().rstrip("/")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.path.rstrip("/") != "/api/1"
        or parsed.query
        or parsed.fragment
    ):
        raise ScriptError(
            "DADOS_GOV_API_URL deve ter o formato https://dados.gov.pt/api/1"
        )
    return url


def load_settings(env_path: str | Path = ".env") -> Settings:
    path = Path(env_path)
    file_values = dotenv_values(path) if path.is_file() else {}
    values = {key: str(value) for key, value in file_values.items() if value is not None}
    values.update(os.environ)

    required = (
        "AIVEN_HOST",
        "AIVEN_PORT",
        "AIVEN_DATABASE",
        "AIVEN_USER",
        "AIVEN_PASSWORD",
        "DADOS_GOV_API_KEY",
        "DADOS_GOV_ORGANIZATION_ID",
    )
    missing = [key for key in required if _is_missing(values.get(key))]
    if missing:
        raise ScriptError("faltam variaveis no .env: " + ", ".join(missing))

    try:
        port = int(values["AIVEN_PORT"])
    except (TypeError, ValueError) as exc:
        raise ScriptError("AIVEN_PORT deve ser um numero inteiro") from exc
    if not 1 <= port <= 65535:
        raise ScriptError("AIVEN_PORT deve estar entre 1 e 65535")

    ca_cert_text = values.get("AIVEN_CA_CERT", "").strip()
    ca_cert = Path(ca_cert_text).expanduser() if ca_cert_text else None
    if ca_cert is not None and not ca_cert.is_absolute():
        ca_cert = path.parent / ca_cert
    if ca_cert is not None and not ca_cert.is_file():
        raise ScriptError("AIVEN_CA_CERT nao aponta para um ficheiro")

    aiven = AivenConfig(
        host=values["AIVEN_HOST"].strip(),
        port=port,
        database=values["AIVEN_DATABASE"].strip(),
        user=values["AIVEN_USER"].strip(),
        password=values["AIVEN_PASSWORD"],
        ca_cert=ca_cert,
    )
    return Settings(
        aiven=aiven,
        api_url=_api_url(
            values.get("DADOS_GOV_API_URL", "")
            or "https://dados.gov.pt/api/1"
        ),
        api_key=values["DADOS_GOV_API_KEY"].strip(),
        organization_id=values["DADOS_GOV_ORGANIZATION_ID"].strip(),
        private=_parse_bool(
            values.get("DADOS_GOV_PRIVATE", "") or "true",
            "DADOS_GOV_PRIVATE",
        ),
    )


def make_ssl_context(ca_cert: Path | None) -> ssl.SSLContext:
    try:
        if ca_cert is None:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        else:
            context = ssl.create_default_context(cafile=str(ca_cert))
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = True
        return context
    except (OSError, ssl.SSLError, ValueError) as exc:
        raise ScriptError("nao foi possivel configurar TLS") from exc


def connect_to_aiven(config: AivenConfig) -> Any:
    if config.ca_cert is None:
        print(
            "Aviso: ligacao TLS cifrada sem validacao do certificado do servidor.",
            file=sys.stderr,
        )
    try:
        return pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            ssl=make_ssl_context(config.ca_cert),
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
            charset="utf8mb4",
            autocommit=False,
        )
    except ScriptError:
        raise
    except Exception as exc:
        raise ScriptError(
            "nao foi possivel ligar a base de dados; confirme as credenciais "
            "e, se o servidor usar uma CA privada, configure AIVEN_CA_CERT"
        ) from exc


def _quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _table_columns(
    connection: Any, database: str, table: str
) -> tuple[dict[str, Any], ...]:
    statement = (
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_COMMENT "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION"
    )
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(statement, (database, table))
        rows = cursor.fetchall()
    if not rows:
        raise ScriptError(f"a tabela {table} nao existe na base de dados Aiven")
    return tuple(dict(row) for row in rows)


def _catalog_metadata(connection: Any, database: str, table: str) -> dict[str, Any]:
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='catalog'",
            (database,),
        )
        catalog_columns = [row["COLUMN_NAME"] for row in cursor.fetchall()]
        if not catalog_columns:
            return {}

        actual_by_name = {name.casefold(): name for name in catalog_columns}
        matching = [
            actual_by_name[name]
            for name in ("alias", "source", "uri")
            if name in actual_by_name
        ]
        if not matching:
            return {}

        where = " OR ".join(f"{_quote_identifier(name)}=%s" for name in matching)
        cursor.execute(
            f"SELECT * FROM `catalog` WHERE {where} LIMIT 1",
            tuple(table for _ in matching),
        )
        row = cursor.fetchone()
    return dict(row) if row else {}


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    raise ScriptError("a tabela contem um valor que nao pode ser escrito em GeoJSON")


def _select_expressions(columns: Sequence[Mapping[str, Any]]) -> list[str]:
    expressions = []
    for column in columns:
        name = str(column["COLUMN_NAME"])
        quoted = _quote_identifier(name)
        if str(column["DATA_TYPE"]).casefold() in SPATIAL_TYPES:
            expressions.append(f"ST_AsGeoJSON({quoted}) AS {quoted}")
        else:
            expressions.append(quoted)
    return expressions


def _temporary_export(table: str, suffix: str) -> tuple[Any, Path]:
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        suffix=suffix,
        prefix=f"dados-gov-{table}-",
        delete=False,
    )
    return handle, Path(handle.name)


def _export_csv(
    connection: Any,
    table: str,
    columns: Sequence[Mapping[str, Any]],
) -> tuple[Path, int]:
    column_names = [str(column["COLUMN_NAME"]) for column in columns]
    expressions = _select_expressions(columns)
    handle, path = _temporary_export(table, ".csv")
    row_count = 0
    try:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(column_names)
        with connection.cursor(pymysql.cursors.SSCursor) as cursor:
            cursor.execute(
                f"SELECT {', '.join(expressions)} FROM {_quote_identifier(table)}"
            )
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                writer.writerows(tuple(_csv_value(value) for value in row) for row in rows)
                row_count += len(rows)
    except Exception:
        handle.close()
        path.unlink(missing_ok=True)
        raise
    finally:
        handle.close()

    return path, row_count


def _parse_geometry(value: Any, row_number: int) -> Mapping[str, Any] | None:
    if value is None or value == "":
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="strict")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ScriptError(
                f"geometry invalida no registo {row_number}"
            ) from exc
    if not isinstance(value, Mapping) or not isinstance(value.get("type"), str):
        raise ScriptError(f"geometry invalida no registo {row_number}")
    return dict(value)


def _export_geojson(
    connection: Any,
    table: str,
    columns: Sequence[Mapping[str, Any]],
    geometry_column: str,
) -> tuple[Path, int]:
    column_names = [str(column["COLUMN_NAME"]) for column in columns]
    actual_names = {name.casefold(): name for name in column_names}
    actual_geometry = actual_names.get(geometry_column.casefold())
    if actual_geometry is None:
        raise ScriptError(
            f"a coluna geometry configurada ({geometry_column}) nao existe na tabela"
        )

    expressions = _select_expressions(columns)
    geometry_position = column_names.index(actual_geometry)
    id_position = next(
        (index for index, name in enumerate(column_names) if name.casefold() == "id"),
        None,
    )
    handle, path = _temporary_export(table, ".geojson")
    row_count = 0
    first = True
    try:
        handle.write('{"type":"FeatureCollection","features":[')
        with connection.cursor(pymysql.cursors.SSCursor) as cursor:
            cursor.execute(
                f"SELECT {', '.join(expressions)} FROM {_quote_identifier(table)}"
            )
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    row_count += 1
                    feature: dict[str, Any] = {
                        "type": "Feature",
                        "properties": {
                            name: _json_value(row[index])
                            for index, name in enumerate(column_names)
                            if index not in {geometry_position, id_position}
                        },
                        "geometry": _parse_geometry(
                            row[geometry_position], row_count
                        ),
                    }
                    if id_position is not None:
                        feature["id"] = _json_value(row[id_position])
                    if not first:
                        handle.write(",")
                    json.dump(
                        feature,
                        handle,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    first = False
        handle.write("]}")
    except Exception:
        handle.close()
        path.unlink(missing_ok=True)
        raise
    finally:
        handle.close()
    return path, row_count


def export_table(
    connection: Any,
    database: str,
    table_config: TableConfig,
) -> ExportedTable:
    table = table_config.table
    columns = _table_columns(connection, database, table)
    catalog_metadata = _catalog_metadata(connection, database, table)
    if table_config.resource_format == "geojson":
        if table_config.geometry_column is None:
            raise ScriptError("geometry_column nao foi configurada")
        path, row_count = _export_geojson(
            connection, table, columns, table_config.geometry_column
        )
        media_type = "application/geo+json"
    else:
        path, row_count = _export_csv(connection, table, columns)
        media_type = "text/csv"
    return ExportedTable(
        path,
        row_count,
        columns,
        catalog_metadata,
        table_config.resource_format,
        media_type,
    )


def _metadata_value(row: Mapping[str, Any], name: str) -> Any:
    for key, value in row.items():
        if str(key).casefold() == name.casefold() and value not in (None, "", []):
            return value
    return None


def _metadata_tags(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except json.JSONDecodeError:
        pass
    return [item.strip() for item in value.split(",") if item.strip()]


def build_dataset_payload(
    exported: ExportedTable,
    table_config: TableConfig,
    settings: Settings,
    *,
    private: bool,
) -> dict[str, Any]:
    catalog = exported.catalog_metadata
    description = (
        table_config.description
        or _metadata_value(catalog, "description")
        or (
            f"Dados e metadados da tabela {table_config.table}. "
            f"Contem {exported.row_count} registos e "
            f"{len(exported.columns)} colunas."
        )
    )
    tags = list(table_config.tags) or _metadata_tags(
        _metadata_value(catalog, "keywords")
        or _metadata_value(catalog, "keyword")
    )
    license_id = (
        table_config.license_id
        or _metadata_value(catalog, "license")
        or "notspecified"
    )
    frequency = (
        table_config.frequency
        or _metadata_value(catalog, "frequency")
        or "unknown"
    )

    payload: dict[str, Any] = {
        "title": table_config.title,
        "description": str(description),
        "tags": tags,
        "license": str(license_id),
        "frequency": str(frequency),
        "private": private,
        "organization": settings.organization_id,
        "extras": {"aiven_table": table_config.table},
    }
    if table_config.spatial is not None:
        payload["spatial"] = dict(table_config.spatial)
    return payload


class DadosGovClient:
    def __init__(self, api_url: str, api_key: str) -> None:
        self.base_url = api_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {"X-API-KEY": api_key, "Accept": "application/json"}
        )

    def close(self) -> None:
        self.session.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        expected: Sequence[int] = (200,),
        **kwargs: Any,
    ) -> Any:
        try:
            response = self.session.request(
                method,
                self.base_url + path,
                timeout=REQUEST_TIMEOUT,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise ScriptError("nao foi possivel comunicar com o dados.gov.pt") from exc

        if response.status_code not in expected:
            detail = ""
            try:
                body = response.json()
                if isinstance(body, dict):
                    detail = str(
                        body.get("message")
                        or body.get("detail")
                        or body.get("errors")
                        or body
                    )
            except ValueError:
                detail = response.text.strip()
            raise DadosGovHTTPError(response.status_code, method, path, detail)

        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise ScriptError(
                "o dados.gov.pt devolveu uma resposta JSON invalida"
            ) from exc

    def validate_access(
        self, organization_id: str, license_id: str, frequency: str
    ) -> None:
        me = self.request("GET", "/me/")
        organizations = me.get("organizations", []) if isinstance(me, dict) else []
        organization_ids = {
            str(item.get("id"))
            for item in organizations
            if isinstance(item, Mapping) and item.get("id")
        }
        if organization_id not in organization_ids:
            raise ScriptError(
                "a conta da chave API nao pertence a organizacao "
                f"{organization_id} no dados.gov.pt"
            )

        licenses = self.request("GET", "/datasets/licenses/")
        valid_licenses = {
            str(item.get("id"))
            for item in licenses
            if isinstance(item, Mapping) and item.get("id")
        }
        if license_id not in valid_licenses:
            raise ScriptError(
                f"a licenca {license_id} nao existe no dados.gov.pt"
            )

        frequencies = self.request("GET", "/datasets/frequencies/")
        valid_frequencies = {
            str(item.get("id"))
            for item in frequencies
            if isinstance(item, Mapping) and item.get("id")
        }
        if frequency not in valid_frequencies:
            raise ScriptError(
                f"a frequencia {frequency} nao existe no dados.gov.pt"
            )

    def get_dataset(self, dataset_id: str) -> dict[str, Any]:
        path = f"/datasets/{quote(dataset_id, safe='')}/"
        dataset = self.request("GET", path)
        if not isinstance(dataset, dict):
            raise ScriptError("o dados.gov.pt devolveu um dataset invalido")
        return dict(dataset)

    def get_or_create_dataset(
        self,
        table_config: TableConfig,
        initial_payload: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str]:
        if table_config.dataset_id:
            dataset = self.get_dataset(table_config.dataset_id)
            organization = dataset.get("organization")
            organization_id = (
                str(organization.get("id"))
                if isinstance(organization, Mapping)
                else ""
            )
            expected_organization = str(initial_payload["organization"])
            if organization_id and organization_id != expected_organization:
                raise ScriptError(
                    f"o dataset {table_config.dataset_id} pertence a outra organizacao"
                )
            return dataset, "UPDATE"

        payload = dict(initial_payload)
        payload["private"] = True
        dataset = self.request(
            "POST", "/datasets/", expected=(201,), json=payload
        )
        if not isinstance(dataset, dict) or not dataset.get("id"):
            raise ScriptError("o dados.gov.pt nao devolveu o ID do dataset criado")
        dataset_id = str(dataset["id"])
        save_remote_ids(table_config.table, dataset_id=dataset_id)
        return dict(dataset), "CREATE"

    def _find_resource(
        self, dataset: Mapping[str, Any], table_config: TableConfig
    ) -> dict[str, Any] | None:
        resources = [
            dict(item)
            for item in dataset.get("resources", [])
            if isinstance(item, Mapping)
        ]
        if table_config.resource_id:
            match = next(
                (
                    resource
                    for resource in resources
                    if str(resource.get("id")) == table_config.resource_id
                ),
                None,
            )
            if match is None:
                raise ScriptError(
                    f"o recurso {table_config.resource_id} nao existe no dataset"
                )
            return match

        matches = [
            resource
            for resource in resources
            if resource.get("title") == table_config.resource_title
        ]
        if len(matches) > 1:
            raise ScriptError(
                f"existem varios recursos chamados {table_config.resource_title}"
            )
        if matches:
            resource_id = str(matches[0].get("id", ""))
            if not resource_id:
                raise ScriptError("o dados.gov.pt devolveu um recurso sem ID")
            save_remote_ids(table_config.table, resource_id=resource_id)
            return matches[0]
        return None

    def upload_resource(
        self,
        dataset: Mapping[str, Any],
        table_config: TableConfig,
        exported: ExportedTable,
    ) -> tuple[dict[str, Any], str]:
        dataset_id = str(dataset.get("id", ""))
        if not dataset_id:
            raise ScriptError("o dados.gov.pt devolveu um dataset sem ID")
        resource = self._find_resource(dataset, table_config)
        filename = f"{table_config.table}.{exported.resource_format}"
        with exported.path.open("rb") as handle:
            files = {"file": (filename, handle, exported.media_type)}
            if resource is None:
                uploaded = self.request(
                    "POST",
                    f"/datasets/{quote(dataset_id, safe='')}/upload/",
                    expected=(201,),
                    files=files,
                )
                operation = "CREATE"
            else:
                resource_id = str(resource.get("id", ""))
                uploaded = self.request(
                    "POST",
                    f"/datasets/{quote(dataset_id, safe='')}/resources/"
                    f"{quote(resource_id, safe='')}/upload/",
                    files=files,
                )
                operation = "UPDATE"

        if not isinstance(uploaded, dict) or not uploaded.get("id"):
            raise ScriptError("o dados.gov.pt nao devolveu o ID do recurso enviado")
        uploaded_resource = dict(uploaded)
        resource_id = str(uploaded_resource["id"])
        if table_config.resource_id != resource_id:
            save_remote_ids(table_config.table, resource_id=resource_id)
        return uploaded_resource, operation

    def update_resource_metadata(
        self,
        dataset_id: str,
        resource: Mapping[str, Any],
        table_config: TableConfig,
        exported: ExportedTable,
    ) -> None:
        resource_id = str(resource.get("id", ""))
        resource_url = str(resource.get("url", ""))
        if not resource_id or not resource_url:
            raise ScriptError("o dados.gov.pt devolveu um recurso incompleto")
        payload = {
            "title": table_config.resource_title,
            "description": table_config.resource_description or "",
            "type": "main",
            "filetype": str(resource.get("filetype") or "file"),
            "format": str(
                resource.get("format") or exported.resource_format.upper()
            ),
            "url": resource_url,
        }
        self.request(
            "PUT",
            f"/datasets/{quote(dataset_id, safe='')}/resources/"
            f"{quote(resource_id, safe='')}/",
            json=payload,
        )

    def update_dataset(
        self, dataset_id: str, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        updated = self.request(
            "PUT",
            f"/datasets/{quote(dataset_id, safe='')}/",
            json=dict(payload),
        )
        if not isinstance(updated, dict):
            raise ScriptError("o dados.gov.pt devolveu um dataset invalido")
        return dict(updated)


def publish_table(table_config: TableConfig, settings: Settings) -> None:
    connection = None
    exported: ExportedTable | None = None
    client: DadosGovClient | None = None
    try:
        print(f"A ler a tabela {table_config.table} da Aiven...")
        connection = connect_to_aiven(settings.aiven)
        exported = export_table(connection, settings.aiven.database, table_config)
        print(f"Foram lidos {exported.row_count} registos.")

        final_payload = build_dataset_payload(
            exported, table_config, settings, private=settings.private
        )
        client = DadosGovClient(settings.api_url, settings.api_key)
        client.validate_access(
            settings.organization_id,
            str(final_payload["license"]),
            str(final_payload["frequency"]),
        )

        print("A preparar o dataset no dados.gov.pt...")
        dataset, dataset_operation = client.get_or_create_dataset(
            table_config, final_payload
        )
        dataset_id = str(dataset.get("id", ""))
        if not dataset_id:
            raise ScriptError("o dados.gov.pt devolveu um dataset sem ID")

        print(
            f"A enviar o recurso {exported.resource_format.upper()} "
            "para o dados.gov.pt..."
        )
        resource, resource_operation = client.upload_resource(
            dataset, table_config, exported
        )
        client.update_resource_metadata(
            dataset_id, resource, table_config, exported
        )

        print("A atualizar os metadados e a visibilidade...")
        updated = client.update_dataset(dataset_id, final_payload)
        visibility = "PRIVATE" if bool(updated.get("private")) else "PUBLIC"
        print(
            f"table={table_config.table} | rows={exported.row_count} "
            f"| dataset_id={dataset_id} "
            f"| dataset_operation={dataset_operation} "
            f"| resource_operation={resource_operation} "
            f"| visibility={visibility} | status=SUCCESS"
        )
    finally:
        if client is not None:
            client.close()
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        if exported is not None:
            exported.path.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publica uma tabela da base de dados Aiven no dados.gov.pt."
    )
    parser.add_argument(
        "tabela",
        help="tabela configurada em dados_gov_tables.json",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        print(
            "error: indique a tabela a publicar.\n"
            "Exemplo: python database_to_dados_gov.py nome_da_tabela",
            file=sys.stderr,
        )
        return 2

    args = build_parser().parse_args(arguments)
    try:
        table_config = load_table_config(args.tabela)
        settings = load_settings()
        publish_table(table_config, settings)
        return 0
    except ScriptError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print("error: a publicacao no dados.gov.pt falhou", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
