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
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

try:  # Keep --help and pure input validation usable without PyMySQL installed.
    import pymysql
except ImportError:  # pragma: no cover - exercised only in a minimal install.
    pymysql = None  # type: ignore[assignment]


MAX_BATCH = 500
MAX_IDENTIFIER_LENGTH = 64
MAX_ID_LENGTH = 255

CONFIG_KEYS = (
    "AIVEN_HOST",
    "AIVEN_PORT",
    "AIVEN_DATABASE",
    "AIVEN_USER",
    "AIVEN_PASSWORD",
    "AIVEN_CA_CERT",
)


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
    ca_cert: Path


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
class DatasetPlan:
    dataset: Dataset
    state: TableState
    create_table: bool
    add_columns: tuple[str, ...]
    inserts: int = 0
    updates: int = 0


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
    if not isinstance(value, list):
        raise InputError("JSON deve ser uma lista de objetos")
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


def discover_input_files(input_path: str | Path) -> tuple[Path, ...]:
    path = Path(input_path)
    if not path.exists():
        raise InputError("INPUT não existe")
    if path.is_dir():
        files = [
            child
            for child in path.iterdir()
            if child.is_file() and child.suffix.lower() in {".csv", ".json"}
        ]
        files.sort(key=lambda item: (item.name.casefold(), item.name))
        if not files:
            raise InputError("diretório sem CSV/JSON diretamente contido")
        return tuple(files)
    if path.is_file() and path.suffix.lower() in {".csv", ".json"}:
        return (path,)
    raise InputError("INPUT deve ser CSV, JSON ou diretório")


def load_input_file(path: str | Path, id_source_name: str = "id") -> Dataset:
    file_path = Path(path)
    table = _table_name(file_path)
    if file_path.suffix.lower() == ".csv":
        return _load_csv(file_path, id_source_name, table)
    if file_path.suffix.lower() == ".json":
        return _load_json(file_path, id_source_name, table)
    raise InputError("ficheiro com extensão não suportada")


def load_datasets(input_path: str | Path, id_source_name: str = "id") -> tuple[Dataset, ...]:
    if not isinstance(id_source_name, str) or not id_source_name:
        raise InputError("--id-column não pode ser vazio")
    files = discover_input_files(input_path)
    table_names: dict[str, Path] = {}
    for file_path in files:
        table = _table_name(file_path)
        folded = table.casefold()
        if folded in table_names:
            raise InputError("colisão de tabelas após sanitização")
        table_names[folded] = file_path
    datasets = tuple(load_input_file(file_path, id_source_name) for file_path in files)
    return datasets


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
        if key not in CONFIG_KEYS:
            raise ConfigError(".env contém uma variável não permitida")
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key in values:
            raise ConfigError("variável repetida em .env")
        values[key] = value
    missing = [key for key in CONFIG_KEYS if key not in values]
    if missing:
        raise ConfigError(".env sem variáveis Aiven obrigatórias")
    return values


def load_config(env_path: str | Path = ".env") -> AivenConfig:
    env_file = Path(env_path)
    values = _parse_env_file(env_file)
    for key in CONFIG_KEYS:
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
    ca_cert = Path(values["AIVEN_CA_CERT"]).expanduser()
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


def make_ssl_context(ca_cert: str | Path) -> ssl.SSLContext:
    try:
        context = ssl.create_default_context(cafile=str(ca_cert))
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
    except (OSError, ssl.SSLError, ValueError) as exc:
        raise ConfigError("não foi possível configurar TLS com o CA indicado") from exc
    return context


def connect_to_aiven(config: AivenConfig) -> Any:
    if pymysql is None:
        raise ConfigError("PyMySQL não está instalado")
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
        raise RemoteError("não foi possível ligar à base de dados") from exc


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


def _validate_existing_state(dataset: Dataset, state: TableState, add_columns: bool) -> tuple[str, ...]:
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
    if missing and not add_columns:
        raise SchemaError("faltam colunas; use --add-columns para autorizar o DDL")
    return tuple(missing)


def preflight_table(
    connection: Any,
    database: str,
    dataset: Dataset,
    *,
    add_columns: bool = False,
) -> tuple[TableState, tuple[str, ...]]:
    state = _fetch_table_state(connection, database, dataset.table)
    if not state.exists:
        return state, ()
    return state, _validate_existing_state(dataset, state, add_columns)


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


def build_add_columns_sql(dataset: Dataset, columns: Sequence[str]) -> str:
    if not columns:
        raise SchemaError("nenhuma coluna para acrescentar")
    definitions = ", ".join(f"ADD COLUMN {_quote_identifier(name)} TEXT NULL" for name in columns)
    return f"ALTER TABLE {_quote_identifier(dataset.table)} {definitions}"


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


def _input_ids(dataset: Dataset) -> tuple[str, ...]:
    return tuple(str(record.values[dataset.id_column]) for record in dataset.rows)


def _db_id_text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "strict")
    return str(value)


def existing_ids(connection: Any, dataset: Dataset, state: TableState) -> set[str]:
    if not dataset.rows:
        return set()
    ids = _input_ids(dataset)
    actual_id = state.columns[dataset.id_column.casefold()].actual_name
    found: set[str] = set()
    for start in range(0, len(ids), MAX_BATCH):
        batch = ids[start : start + MAX_BATCH]
        placeholders = ", ".join(["%s"] * len(batch))
        statement = (
            f"SELECT {_quote_identifier(actual_id)} FROM {_quote_identifier(dataset.table)} "
            f"WHERE {_quote_identifier(actual_id)} IN ({placeholders})"
        )
        try:
            rows = _read_query(connection, statement, batch)
        except Exception as exc:
            raise RemoteError("não foi possível consultar os IDs existentes") from exc
        for row in rows:
            value = _first_value(row, 0, actual_id)
            if value is not None:
                found.add(_db_id_text(value))
    return found


def _count_predictions(dataset: Dataset, found_ids: set[str]) -> tuple[int, int]:
    updates = sum(1 for value in _input_ids(dataset) if value in found_ids)
    return len(dataset.rows) - updates, updates


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


def _make_plans(
    connection: Any,
    database: str,
    datasets: Sequence[Dataset],
    *,
    create_table: bool,
    add_columns: bool,
) -> list[DatasetPlan]:
    _ensure_strict_mode(connection)
    plans: list[DatasetPlan] = []
    for dataset in datasets:
        state, missing = preflight_table(
            connection, database, dataset, add_columns=add_columns
        )
        if not state.exists and not create_table:
            raise SchemaError("tabela ausente; use --create-table para autorizar o DDL")
        plans.append(DatasetPlan(dataset, state, not state.exists, missing))
    return plans


def _execute_ddl(connection: Any, plans: Sequence[DatasetPlan]) -> None:
    cursor = connection.cursor()
    try:
        for plan in plans:
            statement: str | None = None
            if plan.create_table:
                statement = build_create_table_sql(plan.dataset)
            elif plan.add_columns:
                statement = build_add_columns_sql(plan.dataset, plan.add_columns)
            if statement is not None:
                cursor.execute(statement)
    except Exception as exc:
        raise DdlError("falha no DDL; alterações de DDL podem ter persistido") from exc
    finally:
        _close_cursor(cursor)


def _revalidate_after_ddl(
    connection: Any, database: str, plans: Sequence[DatasetPlan]
) -> list[DatasetPlan]:
    result: list[DatasetPlan] = []
    try:
        for plan in plans:
            state, missing = preflight_table(
                connection, database, plan.dataset, add_columns=False
            )
            if not state.exists or missing:
                raise SchemaError("o esquema não corresponde ao DDL executado")
            result.append(DatasetPlan(plan.dataset, state, plan.create_table, plan.add_columns))
    except SchemaError as exc:
        raise DdlError("falha na revalidação do esquema após DDL") from exc
    return result


def _run_dml(connection: Any, plans: Sequence[DatasetPlan]) -> None:
    try:
        connection.begin()
        for plan in plans:
            _execute_upserts(connection, plan.dataset)
        connection.commit()
    except Exception as exc:
        try:
            connection.rollback()
        except Exception:
            pass
        raise DmlError("falha no DML; a transação foi revertida") from exc


def _display_plan(plan: DatasetPlan) -> None:
    if plan.create_table:
        ddl = "CREATE TABLE"
    elif plan.add_columns:
        ddl = f"ALTER TABLE ADD {len(plan.add_columns)} COLUMN(S)"
    else:
        ddl = "none"
    print(
        f"{plan.dataset.path} | table={plan.dataset.table} | rows={len(plan.dataset.rows)} "
        f"| inserts={plan.inserts} | updates={plan.updates} | ddl={ddl}"
    )


def run_import(
    datasets: Sequence[Dataset],
    config: AivenConfig,
    *,
    create_table: bool = False,
    add_columns: bool = False,
    write: bool = False,
) -> None:
    connection = connect_to_aiven(config)
    try:
        plans = _make_plans(
            connection,
            config.database,
            datasets,
            create_table=create_table,
            add_columns=add_columns,
        )
        if write and any(plan.create_table or plan.add_columns for plan in plans):
            print("Aviso: DDL pode persistir se o DML falhar depois.", file=sys.stderr)
            _execute_ddl(connection, plans)
            plans = _revalidate_after_ddl(connection, config.database, plans)
        elif not write:
            # The original state is deliberately retained: dry-run performs
            # only reads and does not pretend that planned DDL already exists.
            pass

        counted: list[DatasetPlan] = []
        for plan in plans:
            if plan.state.exists:
                found = existing_ids(connection, plan.dataset, plan.state)
            else:
                found = set()
            inserts, updates = _count_predictions(plan.dataset, found)
            counted.append(
                DatasetPlan(
                    plan.dataset,
                    plan.state,
                    plan.create_table,
                    plan.add_columns,
                    inserts,
                    updates,
                )
            )
        for plan in counted:
            _display_plan(plan)
        if write:
            _run_dml(connection, counted)
    finally:
        try:
            connection.close()
        except Exception:
            pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Importa CSV/JSON locais para Aiven MySQL com upsert por ID."
    )
    parser.add_argument("input", metavar="INPUT", help="ficheiro CSV/JSON ou diretório")
    parser.add_argument("--id-column", default="id", help="nome da coluna de ID (default: id)")
    parser.add_argument("--create-table", action="store_true", help="autoriza CREATE de tabela ausente")
    parser.add_argument("--add-columns", action="store_true", help="autoriza ALTER de colunas ausentes")
    parser.add_argument("--write", action="store_true", help="executa DDL/DML; sem isto é dry-run")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        # This call is intentionally before load_config and before connect.
        datasets = load_datasets(args.input, args.id_column)
        config = load_config()
        run_import(
            datasets,
            config,
            create_table=args.create_table,
            add_columns=args.add_columns,
            write=args.write,
        )
        return 0
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


if __name__ == "__main__":  # pragma: no cover - covered by CLI smoke tests.
    raise SystemExit(main())
