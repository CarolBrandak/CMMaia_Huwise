#!/usr/bin/env python3
"""Publica uma tabela da base de dados Aiven no HuWise."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import ssl
import sys
import tempfile
import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import quote, urlsplit

import pymysql
import requests
from dotenv import dotenv_values


TABLE_CONFIG_PATH = Path(__file__).with_name("huwise_tables.json")
REQUEST_TIMEOUT = 120
PUBLISH_TIMEOUT = 600


class ScriptError(Exception):
    """Erro que pode ser apresentado no terminal sem expor credenciais."""


class HuwiseHTTPError(ScriptError):
    def __init__(self, status_code: int, method: str, path: str, detail: str) -> None:
        self.status_code = status_code
        self.method = method.upper()
        self.path = path
        self.detail = detail
        suffix = f": {detail[:300]}" if detail else ""
        super().__init__(
            f"o HuWise respondeu com HTTP {status_code} "
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
    huwise_domain_url: str
    huwise_api_key: str
    huwise_is_restricted: bool


@dataclass(frozen=True)
class TableConfig:
    table: str
    dataset_id: str
    title: str
    description: str | None
    keywords: tuple[str, ...]
    publisher: str | None
    theme_ids: tuple[str, ...]

    @property
    def resource_title(self) -> str:
        return f"{self.table}.csv"


@dataclass(frozen=True)
class ExportedTable:
    path: Path
    row_count: int
    columns: tuple[Mapping[str, Any], ...]
    catalog_metadata: Mapping[str, Any]


def _is_missing(value: str | None) -> bool:
    if value is None or not value.strip():
        return True
    normalized = value.strip().casefold()
    return normalized in {"changeme", "replace_me", "placeholder", "todo", "..."} or (
        normalized.startswith("<") and normalized.endswith(">")
    )


def load_table_config(
    table: str, config_path: str | Path = TABLE_CONFIG_PATH
) -> TableConfig:
    path = Path(config_path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ScriptError(f"não foi encontrado o ficheiro {path.name}") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ScriptError(f"não foi possível ler {path.name}") from exc

    if not isinstance(value, dict):
        raise ScriptError(f"{path.name} deve conter um objeto JSON")
    entry = value.get(table)
    if not isinstance(entry, dict):
        raise ScriptError(f"a tabela {table} não está configurada em {path.name}")

    dataset_id = entry.get("dataset_id")
    title = entry.get("title")
    description = entry.get("description")
    keywords = entry.get("keywords", [])
    publisher = entry.get("publisher")
    theme_ids = entry.get("theme_ids", [])
    if not isinstance(dataset_id, str) or not re.fullmatch(
        r"[a-z0-9]+(?:-[a-z0-9]+)*", dataset_id
    ):
        raise ScriptError(f"dataset_id inválido para a tabela {table}")
    if not isinstance(title, str) or not title.strip():
        raise ScriptError(f"title inválido para a tabela {table}")
    if description is not None and (
        not isinstance(description, str) or not description.strip()
    ):
        raise ScriptError(f"description inválida para a tabela {table}")
    if not isinstance(keywords, list) or any(
        not isinstance(keyword, str) or not keyword.strip() for keyword in keywords
    ):
        raise ScriptError(f"keywords inválidas para a tabela {table}")
    if publisher is not None and (
        not isinstance(publisher, str) or not publisher.strip()
    ):
        raise ScriptError(f"publisher inválido para a tabela {table}")
    if not isinstance(theme_ids, list) or any(
        not isinstance(theme_id, str)
        or not re.fullmatch(r"[A-Za-z0-9_-]+", theme_id)
        for theme_id in theme_ids
    ):
        raise ScriptError(f"theme_ids inválidos para a tabela {table}")
    return TableConfig(
        table=table,
        dataset_id=dataset_id,
        title=title.strip(),
        description=description.strip() if description is not None else None,
        keywords=tuple(keyword.strip() for keyword in keywords),
        publisher=publisher.strip() if publisher is not None else None,
        theme_ids=tuple(theme_ids),
    )


def _parse_bool(value: str, variable: str) -> bool:
    normalized = value.strip().casefold()
    if normalized in {"1", "true", "yes", "sim"}:
        return True
    if normalized in {"0", "false", "no", "nao", "não"}:
        return False
    raise ScriptError(f"{variable} deve ter o valor true ou false")


def _domain_url(value: str) -> str:
    url = value.strip().rstrip("/")
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ScriptError("HUWISE_DOMAIN_URL deve ser um endereço HTTPS válido")
    if parsed.path not in {"", "/"}:
        raise ScriptError(
            "HUWISE_DOMAIN_URL deve conter apenas o domínio, sem /api/automation"
        )
    return f"https://{parsed.netloc}"


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
        "HUWISE_DOMAIN_URL",
        "HUWISE_API_KEY",
    )
    missing = [key for key in required if _is_missing(values.get(key))]
    if missing:
        raise ScriptError("faltam variáveis no .env: " + ", ".join(missing))

    try:
        port = int(values["AIVEN_PORT"])
    except (TypeError, ValueError) as exc:
        raise ScriptError("AIVEN_PORT deve ser um número inteiro") from exc
    if not 1 <= port <= 65535:
        raise ScriptError("AIVEN_PORT deve estar entre 1 e 65535")

    ca_cert_text = values.get("AIVEN_CA_CERT", "").strip()
    ca_cert = Path(ca_cert_text).expanduser() if ca_cert_text else None
    if ca_cert is not None and not ca_cert.is_absolute():
        ca_cert = path.parent / ca_cert
    if ca_cert is not None and not ca_cert.is_file():
        raise ScriptError("AIVEN_CA_CERT não aponta para um ficheiro")

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
        huwise_domain_url=_domain_url(values["HUWISE_DOMAIN_URL"]),
        huwise_api_key=values["HUWISE_API_KEY"].strip(),
        huwise_is_restricted=_parse_bool(
            values.get("HUWISE_IS_RESTRICTED", "") or "true",
            "HUWISE_IS_RESTRICTED",
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
        raise ScriptError("não foi possível configurar TLS") from exc


def connect_to_aiven(config: AivenConfig) -> Any:
    if config.ca_cert is None:
        print(
            "Aviso: ligação TLS cifrada sem validação do certificado do servidor.",
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
            "não foi possível ligar à base de dados; confirme as credenciais "
            "e, se o servidor usar uma CA privada, configure AIVEN_CA_CERT"
        ) from exc


def _quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _table_columns(connection: Any, database: str, table: str) -> tuple[dict[str, Any], ...]:
    statement = (
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_COMMENT "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION"
    )
    with connection.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(statement, (database, table))
        rows = cursor.fetchall()
    if not rows:
        raise ScriptError(f"a tabela {table} não existe na base de dados Aiven")
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


def export_table(
    connection: Any, database: str, table: str, dataset_id: str
) -> ExportedTable:
    columns = _table_columns(connection, database, table)
    catalog_metadata = _catalog_metadata(connection, database, table)
    column_names = [str(column["COLUMN_NAME"]) for column in columns]

    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        suffix=".csv",
        prefix=f"{dataset_id}-",
        delete=False,
    )
    path = Path(handle.name)
    row_count = 0
    try:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(column_names)
        with connection.cursor(pymysql.cursors.SSCursor) as cursor:
            cursor.execute(f"SELECT * FROM {_quote_identifier(table)}")
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

    return ExportedTable(path, row_count, columns, catalog_metadata)


def _metadata_value(row: Mapping[str, Any], name: str) -> Any:
    for key, value in row.items():
        if str(key).casefold() == name.casefold() and value not in (None, "", []):
            return value
    return None


def build_metadata(
    exported: ExportedTable, table_config: TableConfig
) -> dict[str, dict[str, dict[str, Any]]]:
    catalog = exported.catalog_metadata
    description = table_config.description or _metadata_value(catalog, "description") or (
        f"Dados e metadados da tabela {table_config.table} da base de dados. "
        f"Contém {exported.row_count} registos e {len(exported.columns)} colunas."
    )
    language = str(_metadata_value(catalog, "language") or "pt").lower()
    if language == "pt-pt":
        language = "pt"

    values: dict[str, Any] = {
        "title": table_config.title,
        "description": str(description),
        "language": language,
    }
    if table_config.keywords:
        values["keyword"] = list(table_config.keywords)
    publisher = table_config.publisher or _metadata_value(catalog, "publisher")
    if publisher is not None:
        values["publisher"] = publisher
    license_value = _metadata_value(catalog, "license")
    if license_value is not None:
        values["license"] = license_value

    metadata = {
        "default": {name: {"value": value} for name, value in values.items()}
    }
    if table_config.theme_ids:
        metadata["internal"] = {
            "theme_id": {"value": list(table_config.theme_ids)}
        }
    return metadata


class HuwiseClient:
    def __init__(
        self, domain_url: str, api_key: str, table_config: TableConfig
    ) -> None:
        self.base_url = f"{domain_url}/api/automation/v1.0"
        self.table_config = table_config
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"apikey {api_key}", "Accept": "application/json"}
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
            raise ScriptError("não foi possível comunicar com o HuWise") from exc
        if response.status_code not in expected:
            detail = ""
            try:
                body = response.json()
                if isinstance(body, dict):
                    detail = str(body.get("message") or body.get("detail") or "")
            except ValueError:
                pass
            raise HuwiseHTTPError(
                response.status_code, method, path, detail
            )
        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise ScriptError("o HuWise devolveu uma resposta JSON inválida") from exc

    def validate_permissions(self, is_restricted: bool) -> None:
        result = self.request("GET", "/apikeys/", params={"limit": 1000})
        api_keys = result.get("results", []) if isinstance(result, dict) else []
        current = next(
            (item for item in api_keys if item.get("key") == self.api_key),
            None,
        )
        if current is None:
            raise ScriptError(
                "não foi possível confirmar as permissões da chave API do HuWise"
            )

        permissions = set(current.get("permissions", []))
        required = {
            "create_dataset": "Create new datasets",
            "publish_dataset": "Publish own datasets",
        }
        if is_restricted:
            required["manage_dataset"] = "Manage own datasets' security"
        missing = [label for name, label in required.items() if name not in permissions]
        if missing:
            raise ScriptError(
                "a chave API do HuWise não tem as permissões necessárias: "
                + ", ".join(missing)
                + ". Gere uma nova chave com estas permissões e atualize "
                "HUWISE_API_KEY no ficheiro .env"
            )

    def update_metadata(
        self,
        dataset_uid: str,
        metadata: Mapping[str, Mapping[str, Any]],
    ) -> None:
        uid = quote(dataset_uid, safe="")
        for template_name, fields in metadata.items():
            template = quote(template_name, safe="")
            for field_name, field_value in fields.items():
                field = quote(field_name, safe="")
                self.request(
                    "PUT",
                    f"/datasets/{uid}/metadata/{template}/{field}/",
                    json=field_value,
                )

    def ensure_owner_access(self, dataset: Mapping[str, Any]) -> None:
        dataset_uid = str(dataset.get("uid", ""))
        created_by = dataset.get("created_by")
        username = (
            str(created_by.get("username", ""))
            if isinstance(created_by, Mapping)
            else ""
        )
        if not dataset_uid or not username:
            raise ScriptError(
                "o HuWise não devolveu o utilizador que criou o dataset"
            )

        uid = quote(dataset_uid, safe="")
        rulesets = self.request(
            "GET", f"/datasets/{uid}/security/users/", params={"limit": 1000}
        )
        results = rulesets.get("results", []) if isinstance(rulesets, dict) else []
        existing = [
            item
            for item in results
            if isinstance(item.get("user"), Mapping)
            and item["user"].get("username") == username
        ]
        if len(existing) > 1:
            raise ScriptError(
                "o HuWise devolveu mais de uma regra de acesso para o criador"
            )

        payload = {
            "user": {"username": username},
            "permissions": [
                "explore_restricted_dataset",
                "edit_dataset",
                "publish_dataset",
                "manage_dataset",
            ],
            "security": {"is_data_visible": True},
        }
        if existing:
            encoded_username = quote(username, safe="")
            self.request(
                "PUT",
                f"/datasets/{uid}/security/users/{encoded_username}/",
                json=payload,
            )
        else:
            self.request(
                "POST",
                f"/datasets/{uid}/security/users/",
                expected=(201,),
                json=payload,
            )

    def configure_access(
        self, dataset: dict[str, Any], is_restricted: bool
    ) -> None:
        dataset_uid = str(dataset.get("uid", ""))
        if not dataset_uid:
            raise ScriptError("o HuWise não devolveu o UID do dataset")

        if is_restricted:
            self.ensure_owner_access(dataset)
        if bool(dataset.get("is_restricted")) != is_restricted:
            self.request(
                "PUT",
                f"/datasets/{quote(dataset_uid, safe='')}/",
                json={"is_restricted": is_restricted},
            )
            dataset["is_restricted"] = is_restricted

    def get_or_create_dataset(
        self,
        metadata: Mapping[str, Mapping[str, Any]],
        is_restricted: bool,
    ) -> tuple[dict[str, Any], str]:
        dataset_id = self.table_config.dataset_id
        result = self.request(
            "GET", "/datasets/", params={"dataset_id": dataset_id, "limit": 2}
        )
        datasets = result.get("results", []) if isinstance(result, dict) else []
        if len(datasets) > 1:
            raise ScriptError(f"o HuWise devolveu mais de um dataset com ID {dataset_id}")
        if datasets:
            dataset = dict(datasets[0])
            self.configure_access(dataset, is_restricted)
            self.update_metadata(str(dataset["uid"]), metadata)
            return dataset, "UPDATE"

        try:
            dataset = self.request(
                "POST",
                "/datasets/",
                expected=(201,),
                json={
                    "dataset_id": dataset_id,
                    "is_restricted": False,
                    "metadata": {},
                },
            )
        except HuwiseHTTPError as exc:
            if exc.status_code == 400:
                raise ScriptError(
                    f"não foi possível criar o dataset {dataset_id}. Pode já existir "
                    "um dataset com este identificador. Verifique-o no Back Office "
                    "do HuWise"
                ) from exc
            raise
        if not isinstance(dataset, dict):
            raise ScriptError("o HuWise devolveu um dataset inválido")
        dataset_uid = str(dataset.get("uid", ""))
        if not dataset_uid:
            raise ScriptError("o HuWise não devolveu o UID do dataset")
        try:
            self.update_metadata(dataset_uid, metadata)
            self.configure_access(dataset, is_restricted)
        except Exception:
            try:
                self.request(
                    "DELETE",
                    f"/datasets/{quote(dataset_uid, safe='')}/",
                    expected=(204,),
                )
            except Exception:
                pass
            raise
        return dataset, "CREATE"

    def upload_csv(self, dataset_uid: str, csv_path: Path) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        filename = f"{self.table_config.dataset_id}-{timestamp}.csv"
        with csv_path.open("rb") as handle:
            uploaded = self.request(
                "POST",
                f"/datasets/{quote(dataset_uid, safe='')}/resources/files/",
                expected=(200, 201),
                files={"file": (filename, handle, "text/csv")},
            )
        file_uid = uploaded.get("uid") if isinstance(uploaded, dict) else None
        if not file_uid:
            raise ScriptError("o HuWise não devolveu o identificador do CSV enviado")
        return str(file_uid)

    def upsert_resource(self, dataset_uid: str, file_uid: str) -> str:
        uid = quote(dataset_uid, safe="")
        listed = self.request(
            "GET", f"/datasets/{uid}/resources/", params={"limit": 1000}
        )
        resources = listed.get("results", []) if isinstance(listed, dict) else []
        resource_title = self.table_config.resource_title
        matches = [item for item in resources if item.get("title") == resource_title]
        if len(matches) > 1:
            raise ScriptError(f"existem vários recursos HuWise chamados {resource_title}")

        payload = {
            "type": "csvfile",
            "title": resource_title,
            "params": {
                "doublequote": True,
                "encoding": "utf-8",
                "first_row_no": 1,
                "headers_first_row": True,
                "separator": ",",
            },
            "datasource": {
                "type": "uploaded_file",
                "file": {"uid": file_uid},
            },
        }
        if matches:
            resource_uid = quote(str(matches[0]["uid"]), safe="")
            self.request(
                "PUT", f"/datasets/{uid}/resources/{resource_uid}/", json=payload
            )
            return "UPDATE"
        self.request(
            "POST",
            f"/datasets/{uid}/resources/",
            expected=(201,),
            json=payload,
        )
        return "CREATE"

    def publish(self, dataset_uid: str) -> str:
        uid = quote(dataset_uid, safe="")
        self.request("POST", f"/datasets/{uid}/publish/")
        deadline = time_module.monotonic() + PUBLISH_TIMEOUT
        while True:
            status = self.request("GET", f"/datasets/{uid}/status/")
            if not isinstance(status, dict):
                raise ScriptError("o HuWise devolveu um estado de publicação inválido")

            state = str(status.get("status", "desconhecido"))
            message = status.get("message")
            record_errors = status.get("records_errors") or []
            if state in {"error", "failed"}:
                detail = f": {message}" if message else ""
                raise ScriptError(f"a publicação no HuWise falhou{detail}")
            if state == "idle":
                if record_errors:
                    details = json.dumps(
                        record_errors[:3], ensure_ascii=False, default=str
                    )
                    raise ScriptError(
                        "o HuWise encontrou erros nos registos: " + details[:500]
                    )
                if not status.get("is_published"):
                    detail = f": {message}" if message else ""
                    raise ScriptError(
                        "o HuWise terminou sem publicar o dataset" + detail
                    )
                return state
            if time_module.monotonic() >= deadline:
                raise ScriptError(
                    "o HuWise não terminou a publicação dentro de 10 minutos"
                )
            time_module.sleep(2)


def publish_table(table_config: TableConfig, settings: Settings) -> None:
    table = table_config.table
    connection = None
    exported: ExportedTable | None = None
    client: HuwiseClient | None = None
    try:
        client = HuwiseClient(
            settings.huwise_domain_url, settings.huwise_api_key, table_config
        )
        client.validate_permissions(settings.huwise_is_restricted)

        print(f"A ler a tabela {table} da Aiven...")
        connection = connect_to_aiven(settings.aiven)
        exported = export_table(
            connection, settings.aiven.database, table, table_config.dataset_id
        )
        print(f"Foram lidos {exported.row_count} registos.")

        metadata = build_metadata(exported, table_config)
        print("A preparar o dataset no HuWise...")
        dataset, dataset_operation = client.get_or_create_dataset(
            metadata, settings.huwise_is_restricted
        )
        dataset_uid = str(dataset.get("uid", ""))
        if not dataset_uid:
            raise ScriptError("o HuWise não devolveu o UID do dataset")

        print("A enviar o CSV para o HuWise...")
        file_uid = client.upload_csv(dataset_uid, exported.path)
        resource_operation = client.upsert_resource(dataset_uid, file_uid)
        print("A publicar e a aguardar pelo processamento...")
        huwise_status = client.publish(dataset_uid)
        print(
            f"table={table} | rows={exported.row_count} "
            f"| dataset={table_config.dataset_id} "
            f"| dataset_operation={dataset_operation} "
            f"| resource_operation={resource_operation} "
            f"| status=SUCCESS | huwise_status={huwise_status}"
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
        description="Publica uma tabela da base de dados Aiven no HuWise."
    )
    parser.add_argument(
        "tabela",
        help="tabela configurada em huwise_tables.json",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        print(
            "error: indique a tabela a publicar.\n"
            "Exemplo: python database_to_HuWise.py baze_cache_energia",
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
        print("error: a publicação falhou", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
