#!/usr/bin/env python3
"""Import local CSV/JSON files into Aiven MySQL, without implicit writes.

The module deliberately keeps the input parser, schema preflight and SQL
builders separate.  This makes the safety properties testable without a live
database and, more importantly, means that parsing all input happens before a
configuration file or a database connection is touched.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import ssl
import sys
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

try:  # Keep --help and pure input validation usable without PyMySQL installed.
    import pymysql
except ImportError:  # pragma: no cover - exercised only in a minimal install.
    pymysql = None  # type: ignore[assignment]


MAX_BATCH = 500
MAX_IDENTIFIER_LENGTH = 64
MAX_ID_LENGTH = 255
LOG_TABLE = "import_log"

CONFIG_KEYS = (
    "AIVEN_HOST",
    "AIVEN_PORT",
    "AIVEN_DATABASE",
    "AIVEN_USER",
    "AIVEN_PASSWORD",
    "AIVEN_CA_CERT",
)

REQUIRED_CONFIG_KEYS = tuple(key for key in CONFIG_KEYS if key != "AIVEN_CA_CERT")


class ImporterError(Exception):
    """Base class for errors safe to display to a CLI user."""


class InputError(ImporterError):
    pass


class ConfigError(ImporterError):
    pass


class SchemaError(ImporterError):
    pass


class RemoteError(ImporterError):
    pass


class DdlError(ImporterError):
    pass


class DmlError(ImporterError):
    pass


@dataclass(frozen=True)
class Column:
    source_name: str
    name: str


@dataclass(frozen=True)
class Record:
    """One normalized row; absent JSON keys are absent from ``values``."""

    values: Mapping[str, str | None]


@dataclass(frozen=True)
class Dataset:
    path: Path
    table: str
    columns: tuple[Column, ...]
    rows: tuple[Record, ...]
    id_source_name: str
    mergeable: bool = False

    @property
    def id_column(self) -> str:
        for column in self.columns:
            if column.source_name == self.id_source_name:
                return column.name
        # Input loading always verifies this.  Keeping this defensive makes
        # malformed hand-built Dataset objects fail safely in callers.
        raise InputError("a coluna de ID não existe no conjunto de dados")

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)


@dataclass(frozen=True)
class AivenConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    ca_cert: Path | None


@dataclass(frozen=True)
class ColumnInfo:
    actual_name: str
    nullable: str
    data_type: str
    extra: str
    generation_expression: str | None


@dataclass(frozen=True)
class IndexInfo:
    name: str
    unique: bool
    columns: tuple[tuple[str, Any], ...]


@dataclass(frozen=True)
class TableState:
    exists: bool
    engine: str | None
    columns: Mapping[str, ColumnInfo]
    indexes: tuple[IndexInfo, ...]


@dataclass(frozen=True)
class ImportGroup:
    table: str
    path: Path
    files: tuple[Path, ...]


class JsonNumber:
    """A JSON number retaining its lexical representation exactly."""

    __slots__ = ("text",)

    def __init__(self, text: str) -> None:
        self.text = text

    def __repr__(self) -> str:  # pragma: no cover - only useful in debugging.
        return f"JsonNumber({self.text!r})"


def sanitize_identifier(value: str) -> str:
    """Return the strictly bounded identifier representation used by SQL."""

    if not isinstance(value, str):
        raise InputError("identificador inválido")
    ascii_value = (
        unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    )
    result = re.sub(r"[^a-zA-Z0-9]+", "_", ascii_value).lower().strip("_")
    if not result:
        raise InputError("identificador vazio após sanitização")
    if len(result) > MAX_IDENTIFIER_LENGTH:
        raise InputError("identificador excede 64 caracteres")
    return result


def _columns_from_source_names(names: Sequence[str]) -> tuple[Column, ...]:
    columns: list[Column] = []
    seen: dict[str, str] = {}
    for source_name in names:
        if not isinstance(source_name, str):
            raise InputError("nome de coluna inválido")
        name = sanitize_identifier(source_name)
        folded = name.casefold()
        if folded in seen:
            raise InputError("colisão de nomes de colunas após sanitização")
        seen[folded] = source_name
        columns.append(Column(source_name, name))
    return tuple(columns)


def _table_name(path: Path) -> str:
    suffix = path.suffix
    if not suffix:
        raise InputError("ficheiro sem extensão suportada")
    return sanitize_identifier(path.name[: -len(suffix)])


def _validate_id_text(value: str, seen: set[str]) -> str:
    if not isinstance(value, str) or not value:
        raise InputError("ID nulo ou vazio")
    if len(value) > MAX_ID_LENGTH:
        raise InputError("ID excede 255 caracteres")
    if value in seen:
        raise InputError("IDs duplicados no mesmo conjunto de dados")
    seen.add(value)
    return value


def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise InputError("número JSON inválido")
    return str(value)


def _compact_json(value: Any) -> str:
    """Compactly serialize nested JSON while retaining parsed number text."""

    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, JsonNumber):
        return value.text
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise InputError("número JSON inválido")
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, list):
        return "[" + ",".join(_compact_json(item) for item in value) + "]"
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            if not isinstance(key, str):
                raise InputError("chave JSON inválida")
            encoded_key = json.dumps(key, ensure_ascii=False, separators=(",", ":"))
            parts.append(encoded_key + ":" + _compact_json(item))
        return "{" + ",".join(parts) + "}"
    raise InputError("valor JSON inválido")


def _json_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, JsonNumber):
        return value.text
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, (int, float)):
        return _compact_json(value)
    if isinstance(value, (list, dict)):
        return _compact_json(value)
    raise InputError("valor JSON inválido")


def _duplicate_rejecting_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InputError("chave JSON duplicada")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise InputError("NaN/Infinity não são permitidos em JSON")


def _load_csv(path: Path, id_source_name: str, table: str) -> Dataset:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            try:
                header = next(reader)
            except StopIteration as exc:
                raise InputError("CSV sem cabeçalho") from exc
            if not header:
                raise InputError("CSV sem cabeçalho")
            columns = _columns_from_source_names(header)
            source_names = {column.source_name for column in columns}
            if id_source_name not in source_names:
                raise InputError("coluna de ID ausente")
            rows: list[Record] = []
            seen_ids: set[str] = set()
            for line_number, row in enumerate(reader, start=2):
                if len(row) != len(header):
                    raise InputError(f"CSV com largura inválida na linha {line_number}")
                values = {column.name: value for column, value in zip(columns, row)}
                id_text = _validate_id_text(values[sanitize_identifier(id_source_name)], seen_ids)
                values[sanitize_identifier(id_source_name)] = id_text
                rows.append(Record(values))
    except InputError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise InputError("não foi possível ler o CSV") from exc
    return Dataset(path, table, columns, tuple(rows), id_source_name)


def _load_json(path: Path, id_source_name: str, table: str) -> Dataset:
    try:
        with path.open("r", encoding="utf-8-sig") as handle:
            value = json.load(
                handle,
                object_pairs_hook=_duplicate_rejecting_object,
                parse_int=JsonNumber,
                parse_float=JsonNumber,
                parse_constant=_reject_json_constant,
            )
    except InputError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise InputError("não foi possível ler o JSON") from exc
    if isinstance(value, dict) and {"t", "NReg", "Consumo", "metadata"} <= value.keys():
        return _load_baze_json(path, id_source_name, table, value)
    if not isinstance(value, list):
        raise InputError("JSON deve ser uma lista de objetos ou um objeto Baze")
    if any(not isinstance(item, dict) for item in value):
        raise InputError("JSON deve ser uma lista de objetos")

    source_names: list[str] = []
    source_seen: set[str] = set()
    for item in value:
        for key in item:
            if not isinstance(key, str):
                raise InputError("chave JSON inválida")
            if key not in source_seen:
                source_seen.add(key)
                source_names.append(key)
    columns = _columns_from_source_names(source_names)
    by_source = {column.source_name: column.name for column in columns}
    if id_source_name not in by_source:
        raise InputError("coluna de ID ausente")

    rows: list[Record] = []
    seen_ids: set[str] = set()
    for item in value:
        if id_source_name not in item:
            raise InputError("ID ausente num registo JSON")
        normalized: dict[str, str | None] = {}
        for source_name, raw_value in item.items():
            normalized[by_source[source_name]] = _json_value(raw_value)
        raw_id = item[id_source_name]
        if isinstance(raw_id, (list, dict)) or raw_id is None:
            raise InputError("ID deve ser escalar e não nulo")
        id_text_value = _json_value(raw_id)
        if id_text_value is None or not isinstance(id_text_value, str):
            raise InputError("ID inválido")
        normalized[by_source[id_source_name]] = _validate_id_text(id_text_value, seen_ids)
        rows.append(Record(normalized))
    return Dataset(path, table, columns, tuple(rows), id_source_name)


def _load_baze_json(
    path: Path, id_source_name: str, table: str, value: Mapping[str, Any]
) -> Dataset:
    metadata = value.get("metadata")
    if not isinstance(metadata, dict):
        raise InputError("JSON Baze sem metadata válida")
    if "cpe" not in metadata:
        raise InputError("JSON Baze sem metadata.cpe")

    cpe = _json_value(metadata["cpe"])
    if not cpe:
        raise InputError("JSON Baze com metadata.cpe vazio")
    timestamps = value["t"]
    record_counts = value["NReg"]
    consumptions = value["Consumo"]
    if not all(isinstance(series, list) for series in (timestamps, record_counts, consumptions)):
        raise InputError("t, NReg e Consumo devem ser listas no JSON Baze")
    if len(timestamps) != len(record_counts) or len(timestamps) != len(consumptions):
        raise InputError("t, NReg e Consumo têm comprimentos diferentes no JSON Baze")

    source_names = [id_source_name, "cpe", "t", "NReg", "Consumo", "ag"]
    source_names.extend(name for name in metadata if name != "cpe")
    columns = _columns_from_source_names(source_names)
    by_source = {column.source_name: column.name for column in columns}
    aggregation = _json_value(value.get("ag"))
    metadata_values = {
        by_source[name]: _json_value(raw_value)
        for name, raw_value in metadata.items()
        if name != "cpe"
    }

    rows: list[Record] = []
    seen_ids: set[str] = set()
    for raw_timestamp, raw_count, raw_consumption in zip(
        timestamps, record_counts, consumptions
    ):
        timestamp = _json_value(raw_timestamp)
        if not timestamp:
            raise InputError("JSON Baze com instante vazio")
        record_id = _validate_id_text(f"{cpe}|{timestamp}", seen_ids)
        normalized = {
            by_source[id_source_name]: record_id,
            by_source["cpe"]: cpe,
            by_source["t"]: timestamp,
            by_source["NReg"]: _json_value(raw_count),
            by_source["Consumo"]: _json_value(raw_consumption),
            by_source["ag"]: aggregation,
            **metadata_values,
        }
        rows.append(Record(normalized))

    return Dataset(
        path,
        table,
        columns,
        tuple(rows),
        id_source_name,
        mergeable=True,
    )


def load_input_file(
    path: str | Path,
    id_source_name: str = "id",
    table_name: str | None = None,
) -> Dataset:
    file_path = Path(path)
    table = table_name or _table_name(file_path)
    if file_path.suffix.lower() == ".csv":
        return _load_csv(file_path, id_source_name, table)
    if file_path.suffix.lower() == ".json":
        return _load_json(file_path, id_source_name, table)
    raise InputError("ficheiro com extensão não suportada")


def _merge_datasets(
    datasets: Sequence[Dataset], source_path: Path | None = None
) -> Dataset:
    first = datasets[0]
    if any(dataset.table.casefold() != first.table.casefold() for dataset in datasets):
        raise InputError("não é possível juntar ficheiros destinados a tabelas diferentes")
    if any(dataset.id_source_name != first.id_source_name for dataset in datasets):
        raise InputError("tabelas Baze com colunas de ID incompatíveis")

    source_names: list[str] = []
    seen_sources: set[str] = set()
    for dataset in datasets:
        for column in dataset.columns:
            if column.source_name not in seen_sources:
                seen_sources.add(column.source_name)
                source_names.append(column.source_name)
    columns = _columns_from_source_names(source_names)

    rows_by_id: dict[str, dict[str, str | None]] = {}
    id_column = first.id_column
    for dataset in datasets:
        for record in dataset.rows:
            record_id = str(record.values[id_column])
            if record_id not in rows_by_id:
                rows_by_id[record_id] = dict(record.values)
                continue
            merged = rows_by_id[record_id]
            for name, value in record.values.items():
                if name not in merged or merged[name] is None:
                    merged[name] = value
                elif value is not None and merged[name] != value:
                    raise InputError("ID Baze duplicado com valores incompatíveis")

    return Dataset(
        source_path or first.path.parent,
        first.table,
        columns,
        tuple(Record(values) for values in rows_by_id.values()),
        first.id_source_name,
        mergeable=True,
    )


def discover_import_groups(data_path: str | Path = "data") -> tuple[ImportGroup, ...]:
    root = Path(data_path)
    if not root.is_dir():
        raise InputError("a pasta data não existe")

    groups: list[ImportGroup] = []
    table_names: set[str] = set()
    children = sorted(root.iterdir(), key=lambda item: (item.name.casefold(), item.name))
    for child in children:
        if child.is_file() and child.suffix.lower() in {".csv", ".json"}:
            table = _table_name(child)
            files = (child,)
        elif child.is_dir():
            files = tuple(
                sorted(
                    (
                        file_path
                        for file_path in child.rglob("*")
                        if file_path.is_file()
                        and file_path.suffix.lower() in {".csv", ".json"}
                    ),
                    key=lambda item: (str(item).casefold(), str(item)),
                )
            )
            if not files:
                continue
            table = sanitize_identifier(child.name)
        else:
            continue

        folded = table.casefold()
        if folded == LOG_TABLE.casefold():
            raise InputError(f"o nome {LOG_TABLE} está reservado para a tabela de log")
        if folded in table_names:
            raise InputError("duas entradas da pasta data originam a mesma tabela")
        table_names.add(folded)
        groups.append(ImportGroup(table, child, files))

    if not groups:
        raise InputError("a pasta data não contém CSV ou JSON")
    return tuple(groups)


def load_import_group(group: ImportGroup, id_source_name: str = "id") -> Dataset:
    datasets = [
        load_input_file(file_path, id_source_name, group.table)
        for file_path in group.files
    ]
    if len(datasets) == 1:
        dataset = datasets[0]
        return Dataset(
            group.path,
            group.table,
            dataset.columns,
            dataset.rows,
            dataset.id_source_name,
            dataset.mergeable,
        )
    return _merge_datasets(datasets, group.path)


def _is_placeholder(value: str) -> bool:
    normalized = value.strip().casefold()
    if not normalized:
        return True
    if normalized in {
        "changeme",
        "change_me",
        "change-this",
        "replace_me",
        "replace-this",
        "placeholder",
        "todo",
        "password",
        "secret",
        "example",
        "...",
        "<value>",
        "<your-value>",
    }:
        return True
    return normalized.startswith("<") and normalized.endswith(">")


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise ConfigError(".env não encontrado")
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ConfigError("não foi possível ler .env") from exc
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ConfigError("linha inválida em .env")
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key not in CONFIG_KEYS:
            continue
        if key in values:
            raise ConfigError("variável repetida em .env")
        values[key] = value
    missing = [key for key in REQUIRED_CONFIG_KEYS if key not in values]
    if missing:
        raise ConfigError(
            ".env sem variáveis Aiven obrigatórias: " + ", ".join(missing)
        )
    return values


def load_config(env_path: str | Path = ".env") -> AivenConfig:
    env_file = Path(env_path)
    values = _parse_env_file(env_file)
    for key in REQUIRED_CONFIG_KEYS:
        if _is_placeholder(values[key]):
            raise ConfigError(f"{key} vazio ou placeholder")
        if "\x00" in values[key]:
            raise ConfigError(f"{key} inválido")
    try:
        port = int(values["AIVEN_PORT"])
    except ValueError as exc:
        raise ConfigError("AIVEN_PORT inválido") from exc
    if not 1 <= port <= 65535:
        raise ConfigError("AIVEN_PORT fora do intervalo")
    ca_cert: Path | None = None
    ca_value = values.get("AIVEN_CA_CERT", "").strip()
    if ca_value:
        if _is_placeholder(ca_value) or "\x00" in ca_value:
            raise ConfigError("AIVEN_CA_CERT inválido")
        ca_cert = Path(ca_value).expanduser()
        if not ca_cert.is_absolute():
            ca_cert = env_file.parent / ca_cert
        if not ca_cert.is_file():
            raise ConfigError("AIVEN_CA_CERT não aponta para um ficheiro")
    return AivenConfig(
        host=values["AIVEN_HOST"],
        port=port,
        database=values["AIVEN_DATABASE"],
        user=values["AIVEN_USER"],
        password=values["AIVEN_PASSWORD"],
        ca_cert=ca_cert,
    )


def make_ssl_context(ca_cert: str | Path | None = None) -> ssl.SSLContext:
    try:
        if ca_cert is None:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        else:
            context = ssl.create_default_context(cafile=str(ca_cert))
            context.verify_mode = ssl.CERT_REQUIRED
            context.check_hostname = True
    except (OSError, ssl.SSLError, ValueError) as exc:
        raise ConfigError("não foi possível configurar TLS") from exc
    return context


def connect_to_aiven(config: AivenConfig) -> Any:
    if pymysql is None:
        raise ConfigError("PyMySQL não está instalado")
    if config.ca_cert is None:
        print(
            "Aviso: ligação TLS cifrada sem validação do certificado do servidor.",
            file=sys.stderr,
        )
    context = make_ssl_context(config.ca_cert)
    try:
        return pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            ssl=context,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=30,
            charset="utf8mb4",
            autocommit=False,
        )
    except Exception as exc:
        # Do not relay driver messages: some drivers include a DSN or secret.
        raise RemoteError(
            "não foi possível ligar à base de dados; confirme as credenciais "
            "e, se o servidor usar uma CA privada, configure AIVEN_CA_CERT"
        ) from exc


def _quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _close_cursor(cursor: Any) -> None:
    try:
        cursor.close()
    except Exception:
        pass


def _read_query(connection: Any, statement: str, parameters: Sequence[Any] = ()) -> list[Any]:
    cursor = connection.cursor()
    try:
        cursor.execute(statement, tuple(parameters))
        rows = cursor.fetchall()
        return list(rows)
    finally:
        _close_cursor(cursor)


def _first_value(row: Any, position: int, *names: str) -> Any:
    if isinstance(row, Mapping):
        for name in names:
            if name in row:
                return row[name]
            folded = name.casefold()
            for key, value in row.items():
                if str(key).casefold() == folded:
                    return value
        return None
    try:
        return row[position]
    except (IndexError, KeyError, TypeError):
        return None


def _ensure_strict_mode(connection: Any) -> None:
    try:
        rows = _read_query(connection, "SELECT @@SESSION.sql_mode")
    except Exception as exc:
        raise RemoteError("não foi possível verificar o modo strict do MySQL") from exc
    if not rows:
        raise SchemaError("não foi possível verificar o modo strict do MySQL")
    mode = _first_value(rows[0], 0, "@@SESSION.sql_mode", "sql_mode")
    mode_text = str(mode or "").upper().split(",")
    if "STRICT_TRANS_TABLES" not in mode_text and "STRICT_ALL_TABLES" not in mode_text:
        raise SchemaError("MySQL strict mode é obrigatório")


def _fetch_table_state(connection: Any, database: str, table: str) -> TableState:
    try:
        table_rows = _read_query(
            connection,
            "SELECT ENGINE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
            (database, table),
        )
        if not table_rows:
            return TableState(False, None, {}, ())
        engine = _first_value(table_rows[0], 0, "ENGINE", "engine")
        column_rows = _read_query(
            connection,
            "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, EXTRA, GENERATION_EXPRESSION "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s "
            "ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        columns: dict[str, ColumnInfo] = {}
        for row in column_rows:
            actual_name = _first_value(row, 0, "COLUMN_NAME", "column_name")
            if not isinstance(actual_name, str):
                raise SchemaError("metadados de coluna inválidos")
            columns[actual_name.casefold()] = ColumnInfo(
                actual_name=actual_name,
                nullable=str(_first_value(row, 1, "IS_NULLABLE", "is_nullable") or ""),
                data_type=str(_first_value(row, 2, "DATA_TYPE", "data_type") or "").casefold(),
                extra=str(_first_value(row, 3, "EXTRA", "extra") or ""),
                generation_expression=_first_value(
                    row, 4, "GENERATION_EXPRESSION", "generation_expression"
                ),
            )
        index_rows = _read_query(
            connection,
            "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART "
            "FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s "
            "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
            (database, table),
        )
        grouped: dict[str, tuple[str, bool, list[tuple[int, str, Any]]]] = {}
        for row in index_rows:
            index_name = _first_value(row, 0, "INDEX_NAME", "index_name")
            column_name = _first_value(row, 3, "COLUMN_NAME", "column_name")
            if not isinstance(index_name, str) or not isinstance(column_name, str):
                raise SchemaError("metadados de índice inválidos")
            seq = _first_value(row, 2, "SEQ_IN_INDEX", "seq_in_index")
            try:
                sequence = int(seq)
            except (TypeError, ValueError) as exc:
                raise SchemaError("metadados de índice inválidos") from exc
            non_unique = _first_value(row, 1, "NON_UNIQUE", "non_unique")
            unique = str(non_unique) in {"0", "False", "false"}
            sub_part = _first_value(row, 4, "SUB_PART", "sub_part")
            current = grouped.setdefault(index_name, (index_name, unique, []))
            current[2].append((sequence, column_name, sub_part))
        indexes = []
        for name, unique, entries in grouped.values():
            entries.sort(key=lambda item: item[0])
            indexes.append(IndexInfo(name, unique, tuple((col, sub) for _, col, sub in entries)))
        return TableState(True, str(engine or ""), columns, tuple(indexes))
    except (SchemaError, RemoteError):
        raise
    except Exception as exc:
        raise RemoteError("não foi possível ler o esquema MySQL") from exc


_INTEGER_TYPES = {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}


def _validate_existing_state(dataset: Dataset, state: TableState) -> None:
    if not state.exists:
        return ()
    if (state.engine or "").casefold() != "innodb":
        raise SchemaError(f"a tabela {dataset.table} não usa InnoDB")
    id_info = state.columns.get(dataset.id_column.casefold())
    if id_info is None:
        raise SchemaError("tabela existente sem a coluna de ID; nunca é acrescentada")
    if id_info.nullable.casefold() != "no":
        raise SchemaError("a coluna de ID existente deve ser NOT NULL")
    if id_info.data_type not in _INTEGER_TYPES | {"char", "varchar"}:
        raise SchemaError("a coluna de ID deve ser CHAR/VARCHAR ou inteira")

    id_folded = dataset.id_column.casefold()
    has_id_unique = False
    for index in state.indexes:
        if not index.unique:
            continue
        index_columns = tuple(column.casefold() for column, _ in index.columns)
        contains_id = id_folded in index_columns
        if not contains_id:
            raise SchemaError("índice UNIQUE sem a coluna de ID não é suportado")
        if (
            len(index.columns) == 1
            and index_columns[0] == id_folded
            and index.columns[0][1] is None
        ):
            has_id_unique = True
    if not has_id_unique:
        raise SchemaError("a coluna de ID precisa de índice PRIMARY/UNIQUE sem prefixo")

    missing: list[str] = []
    for column in dataset.columns:
        info = state.columns.get(column.name.casefold())
        if info is None:
            if column.name.casefold() == id_folded:
                raise SchemaError("não é permitido acrescentar a coluna de ID existente")
            missing.append(column.name)
        elif "generated" in info.extra.casefold() or info.generation_expression:
            raise SchemaError("não é permitido escrever em colunas generated")
    if missing:
        raise SchemaError(
            "a tabela existente não contém as colunas: " + ", ".join(missing)
        )


def build_create_table_sql(dataset: Dataset) -> str:
    definitions = [f"{_quote_identifier(dataset.id_column)} VARCHAR(255) NOT NULL"]
    for column in dataset.columns:
        if column.name != dataset.id_column:
            definitions.append(f"{_quote_identifier(column.name)} TEXT NULL")
    definitions.append(f"PRIMARY KEY ({_quote_identifier(dataset.id_column)})")
    return (
        f"CREATE TABLE {_quote_identifier(dataset.table)} ("
        + ", ".join(definitions)
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    )


def build_upsert_sql(dataset: Dataset, present_columns: Sequence[str]) -> str:
    present = set(present_columns)
    columns = [dataset.id_column]
    columns.extend(column.name for column in dataset.columns if column.name != dataset.id_column and column.name in present)
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    assignments = [
        f"{_quote_identifier(column)}=VALUES({_quote_identifier(column)})"
        for column in columns
        if column != dataset.id_column
    ]
    if not assignments:
        assignments = [
            f"{_quote_identifier(dataset.id_column)}={_quote_identifier(dataset.id_column)}"
        ]
    return (
        f"INSERT INTO {_quote_identifier(dataset.table)} ({quoted_columns}) "
        f"VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {', '.join(assignments)}"
    )


def _group_records(dataset: Dataset) -> Iterator[tuple[tuple[str, ...], list[Record]]]:
    groups: dict[tuple[str, ...], list[Record]] = {}
    order: list[tuple[str, ...]] = []
    for record in dataset.rows:
        present = tuple(
            column.name
            for column in dataset.columns
            if column.name != dataset.id_column and column.name in record.values
        )
        if present not in groups:
            groups[present] = []
            order.append(present)
        groups[present].append(record)
    for present in order:
        yield present, groups[present]


def _execute_upserts(connection: Any, dataset: Dataset) -> None:
    cursor = connection.cursor()
    try:
        for present, records in _group_records(dataset):
            statement = build_upsert_sql(dataset, present)
            columns = [dataset.id_column, *present]
            for start in range(0, len(records), MAX_BATCH):
                batch = records[start : start + MAX_BATCH]
                parameters = [tuple(record.values[column] for column in columns) for record in batch]
                cursor.executemany(statement, parameters)
    finally:
        _close_cursor(cursor)


def _prepare_target_table(
    connection: Any, database: str, dataset: Dataset
) -> str:
    state = _fetch_table_state(connection, database, dataset.table)
    if state.exists:
        _validate_existing_state(dataset, state)
        statement = f"TRUNCATE TABLE {_quote_identifier(dataset.table)}"
        operation = "TRUNCATE"
    else:
        statement = build_create_table_sql(dataset)
        operation = "CREATE"

    cursor = connection.cursor()
    try:
        cursor.execute(statement)
    except Exception as exc:
        raise DdlError(f"falha ao executar {operation} na tabela") from exc
    finally:
        _close_cursor(cursor)
    return operation


def _run_dml(connection: Any, dataset: Dataset) -> None:
    try:
        connection.begin()
        _execute_upserts(connection, dataset)
        connection.commit()
    except Exception as exc:
        try:
            connection.rollback()
        except Exception:
            pass
        raise DmlError("falha no DML; a transação foi revertida") from exc


def _ensure_log_table(connection: Any) -> None:
    statement = (
        f"CREATE TABLE IF NOT EXISTS {_quote_identifier(LOG_TABLE)} ("
        "`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, "
        "`run_id` CHAR(36) NOT NULL, "
        "`table_name` VARCHAR(64) NOT NULL, "
        "`source_path` TEXT NOT NULL, "
        "`operation` VARCHAR(20) NOT NULL, "
        "`status` VARCHAR(20) NOT NULL, "
        "`rows_read` BIGINT UNSIGNED NOT NULL DEFAULT 0, "
        "`rows_inserted` BIGINT UNSIGNED NOT NULL DEFAULT 0, "
        "`error_message` TEXT NULL, "
        "`started_at` DATETIME(6) NOT NULL, "
        "`finished_at` DATETIME(6) NOT NULL, "
        "PRIMARY KEY (`id`), INDEX `idx_import_log_run` (`run_id`), "
        "INDEX `idx_import_log_status` (`status`)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    )
    cursor = connection.cursor()
    try:
        cursor.execute(statement)
        connection.commit()
    except Exception as exc:
        try:
            connection.rollback()
        except Exception:
            pass
        raise DdlError("não foi possível criar a tabela import_log") from exc
    finally:
        _close_cursor(cursor)


def _write_import_log(
    connection: Any,
    *,
    run_id: str,
    table_name: str,
    source_path: Path,
    operation: str,
    status: str,
    rows_read: int,
    rows_inserted: int,
    error_message: str | None,
    started_at: datetime,
) -> None:
    statement = (
        f"INSERT INTO {_quote_identifier(LOG_TABLE)} "
        "(`run_id`, `table_name`, `source_path`, `operation`, `status`, "
        "`rows_read`, `rows_inserted`, `error_message`, `started_at`, `finished_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    cursor = connection.cursor()
    try:
        cursor.execute(
            statement,
            (
                run_id,
                table_name,
                str(source_path),
                operation,
                status,
                rows_read,
                rows_inserted,
                error_message,
                started_at,
                datetime.now(timezone.utc).replace(tzinfo=None),
            ),
        )
        connection.commit()
    except Exception as exc:
        try:
            connection.rollback()
        except Exception:
            pass
        raise RemoteError("não foi possível escrever na tabela import_log") from exc
    finally:
        _close_cursor(cursor)


def _record_log_safely(connection: Any, **values: Any) -> bool:
    try:
        _write_import_log(connection, **values)
        return True
    except RemoteError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return False


def run_import(groups: Sequence[ImportGroup], config: AivenConfig) -> int:
    connection = connect_to_aiven(config)
    run_id = str(uuid.uuid4())
    failed = False
    try:
        _ensure_log_table(connection)
        _ensure_strict_mode(connection)
        for group in groups:
            started_at = datetime.now(timezone.utc).replace(tzinfo=None)
            operation = "LOAD"
            rows_read = 0
            try:
                dataset = load_import_group(group)
                rows_read = len(dataset.rows)
                operation = _prepare_target_table(
                    connection, config.database, dataset
                )
                _run_dml(connection, dataset)
            except (InputError, SchemaError, RemoteError, DdlError, DmlError) as exc:
                failed = True
                try:
                    connection.rollback()
                except Exception:
                    pass
                message = str(exc)
                _record_log_safely(
                    connection,
                    run_id=run_id,
                    table_name=group.table,
                    source_path=group.path,
                    operation=operation,
                    status="ERROR",
                    rows_read=rows_read,
                    rows_inserted=0,
                    error_message=message,
                    started_at=started_at,
                )
                print(f"error: {group.table}: {message}", file=sys.stderr)
                continue
            except Exception:
                failed = True
                try:
                    connection.rollback()
                except Exception:
                    pass
                message = "operação falhou"
                _record_log_safely(
                    connection,
                    run_id=run_id,
                    table_name=group.table,
                    source_path=group.path,
                    operation=operation,
                    status="ERROR",
                    rows_read=rows_read,
                    rows_inserted=0,
                    error_message=message,
                    started_at=started_at,
                )
                print(f"error: {group.table}: {message}", file=sys.stderr)
                continue

            if not _record_log_safely(
                connection,
                run_id=run_id,
                table_name=group.table,
                source_path=group.path,
                operation=operation,
                status="SUCCESS",
                rows_read=rows_read,
                rows_inserted=rows_read,
                error_message=None,
                started_at=started_at,
            ):
                failed = True
            print(
                f"table={group.table} | source={group.path} | rows={rows_read} "
                f"| operation={operation} | status=SUCCESS"
            )
        return 1 if failed else 0
    finally:
        try:
            connection.close()
        except Exception:
            pass


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        description=(
            "Importa automaticamente os CSV/JSON da pasta data para Aiven MySQL. "
            "Cria tabelas ausentes e trunca tabelas existentes."
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    parser.parse_args(argv)
    try:
        groups = discover_import_groups("data")
        config = load_config()
        return run_import(groups, config)
    except (InputError, ConfigError, SchemaError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (RemoteError, DdlError, DmlError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception:
        # Last-resort containment for driver/filesystem errors.  In particular,
        # never print an arbitrary exception that might contain a password.
        print("error: operação falhou", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
