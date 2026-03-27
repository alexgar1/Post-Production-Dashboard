import os
from contextlib import contextmanager
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:  # pragma: no cover
    psycopg2 = None
    Json = None

DEFAULT_DB_SETTINGS = {
    "dbname": os.getenv("MONDAY_DB_NAME", "monday_reports"),
    "user": os.getenv("MONDAY_DB_USER", "postgres"),
    "password": os.getenv("MONDAY_DB_PASSWORD", "postgres"),
    "host": os.getenv("MONDAY_DB_HOST", "127.0.0.1"),
    "port": os.getenv("MONDAY_DB_PORT", "5432"),
}

DATABASE_URL_ENV = "DATABASE_URL"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")
_SCHEMA_SQL = None

BOARD_TABLES = {
    "listing": {
        "items": "listing_items",
        "subitems": "listing_subitems",
    },
    "social": {
        "items": "social_items",
        "subitems": "social_subitems",
    },
}


class DatabaseDriverMissing(RuntimeError):
    """Raised when psycopg2 is not installed."""


def _ensure_driver():
    if psycopg2 is None:
        raise DatabaseDriverMissing(
            "psycopg2 is required to sync data to Postgres. "
            "Install it via `pip install psycopg2-binary`."
        )


def _load_schema_sql():
    global _SCHEMA_SQL
    if _SCHEMA_SQL is None:
        _SCHEMA_SQL = SCHEMA_PATH.read_text(encoding="utf-8")
    return _SCHEMA_SQL


def _open_connection():
    _ensure_driver()
    database_url = os.getenv(DATABASE_URL_ENV)
    if database_url:
        return psycopg2.connect(database_url)

    connect_kwargs = dict(DEFAULT_DB_SETTINGS)
    sslmode = os.getenv("MONDAY_DB_SSLMODE")
    if sslmode:
        connect_kwargs["sslmode"] = sslmode
    return psycopg2.connect(**connect_kwargs)


@contextmanager
def get_connection():
    """Yield a Postgres connection using the configured settings."""
    conn = _open_connection()
    try:
        yield conn
    finally:
        conn.close()


def ensure_schema(conn=None):
    """
    Apply the schema with CREATE TABLE IF NOT EXISTS statements.

    This keeps first-run deployments on Vercel from failing before a manual
    migration step has been executed.
    """
    close_after = conn is None
    active_conn = conn or _open_connection()
    prior_autocommit = active_conn.autocommit
    try:
        active_conn.autocommit = True
        with active_conn.cursor() as cur:
            cur.execute(_load_schema_sql())
    finally:
        active_conn.autocommit = prior_autocommit
        if close_after:
            active_conn.close()


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _prepare_column_blob(column_values):
    """Return a dict keyed by Monday column id with the raw column payload."""
    prepared = {}
    for col in column_values or []:
        col_id = col.get("id") or f"col_{len(prepared) + 1}"
        prepared[col_id] = {
            "id": col.get("id"),
            "type": col.get("type"),
            "text": col.get("text"),
            "value": col.get("value"),
            "history": col.get("history"),
        }
    return prepared


def _column_blob_to_list(column_blob):
    if isinstance(column_blob, dict):
        return list(column_blob.values())
    if isinstance(column_blob, list):
        return column_blob
    return []


def _extract_users(raw_users):
    users = {}
    for user in raw_users or []:
        uid = _safe_int(user.get("id"))
        if uid is None:
            continue
        username = user.get("name") or user.get("title") or user.get("email") or ""
        users[str(uid)] = username
    return users


def _build_item_record(board_id, item):
    item_id = _safe_int(item.get("id"))
    if item_id is None:
        return None
    return {
        "item_id": item_id,
        "board_id": _safe_int(board_id),
        "name": item.get("name"),
        "column_values": _prepare_column_blob(item.get("column_values")),
    }


def _build_subitem_record(subitem, parent_item_id):
    subitem_id = _safe_int(subitem.get("id"))
    parent_id = _safe_int(parent_item_id)
    if subitem_id is None or parent_id is None:
        return None
    return {
        "subitem_id": subitem_id,
        "parent_item_id": parent_id,
        "name": subitem.get("name"),
        "column_values": _prepare_column_blob(subitem.get("column_values")),
    }


def format_board_payload(board_id, board_data):
    """Convert a Monday board payload into flat item/subitem lists."""
    board_items = (
        (board_data or {})
        .get("data", {})
        .get("boards", [{}])[0]
        .get("items_page", {})
        .get("items", [])
    )
    formatted_items = []
    formatted_subitems = []
    for item in board_items:
        formatted_item = _build_item_record(board_id, item)
        if formatted_item:
            formatted_items.append(formatted_item)
        for subitem in item.get("subitems") or []:
            formatted_subitem = _build_subitem_record(subitem, item.get("id"))
            if formatted_subitem:
                formatted_subitems.append(formatted_subitem)

    users = _extract_users((board_data or {}).get("data", {}).get("users"))
    return {
        "items": formatted_items,
        "subitems": formatted_subitems,
        "users": users,
    }


def _store_users(conn, users_map):
    if not users_map:
        return
    payload = []
    for raw_uid, username in (users_map or {}).items():
        uid = _safe_int(raw_uid)
        if uid is None:
            continue
        payload.append((uid, username))
    if not payload:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO monday_users (user_id, username)
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username;
            """,
            payload,
        )


def _store_board_data(conn, board_key, formatted_payload):
    tables = BOARD_TABLES.get(board_key)
    if not tables:
        raise ValueError(f"Unsupported board key '{board_key}'.")
    _persist_items(conn, tables["items"], formatted_payload.get("items"))
    _persist_subitems(conn, tables["subitems"], formatted_payload.get("subitems"))
    _delete_missing_rows(
        conn,
        tables["subitems"],
        "subitem_id",
        [subitem["subitem_id"] for subitem in formatted_payload.get("subitems") or []],
    )
    _delete_missing_rows(
        conn,
        tables["items"],
        "item_id",
        [item["item_id"] for item in formatted_payload.get("items") or []],
    )


def _persist_items(conn, table_name, items):
    if not items:
        return
    rows = [
        (
            item["item_id"],
            item["board_id"],
            item["name"],
            Json(item["column_values"]),
        )
        for item in items
    ]
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {table_name} (item_id, board_id, name, column_values)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (item_id)
            DO UPDATE SET
                board_id = EXCLUDED.board_id,
                name = EXCLUDED.name,
                column_values = EXCLUDED.column_values;
            """,
            rows,
        )


def _persist_subitems(conn, table_name, subitems):
    if not subitems:
        return
    rows = [
        (
            subitem["subitem_id"],
            subitem["parent_item_id"],
            subitem["name"],
            Json(subitem["column_values"]),
        )
        for subitem in subitems
    ]
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {table_name} (subitem_id, parent_item_id, name, column_values)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (subitem_id)
            DO UPDATE SET
                parent_item_id = EXCLUDED.parent_item_id,
                name = EXCLUDED.name,
                column_values = EXCLUDED.column_values;
            """,
            rows,
        )


def _delete_missing_rows(conn, table_name, id_column, current_ids):
    with conn.cursor() as cur:
        if current_ids:
            cur.execute(
                f"DELETE FROM {table_name} WHERE NOT ({id_column} = ANY(%s));",
                (current_ids,),
            )
            return
        cur.execute(f"DELETE FROM {table_name};")


def sync_monday_database(board_payloads, users=None):
    """
    Persist Monday data into Postgres.

    board_payloads should be a dict like:
    {
        "listing": {"board_id": <int>, "data": <api_response_dict>},
        "social": {"board_id": <int>, "data": <api_response_dict>},
    }
    """
    if not board_payloads:
        return {"boards": {}, "userCount": len(users or {})}

    formatted_payloads = {}
    aggregated_users = dict(users or {})
    for board_key, payload in board_payloads.items():
        board_id = payload.get("board_id")
        board_data = payload.get("data")
        if not board_data:
            continue
        formatted = format_board_payload(board_id, board_data)
        formatted_payloads[board_key] = formatted
        aggregated_users.update(formatted.get("users", {}))

    if not formatted_payloads:
        return {"boards": {}, "userCount": len(aggregated_users)}

    with get_connection() as conn:
        try:
            ensure_schema(conn)
            _store_users(conn, aggregated_users)
            for board_key, payload in formatted_payloads.items():
                _store_board_data(conn, board_key, payload)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {
        "boards": {
            board_key: {
                "itemCount": len(payload.get("items") or []),
                "subitemCount": len(payload.get("subitems") or []),
            }
            for board_key, payload in formatted_payloads.items()
        },
        "userCount": len(aggregated_users),
    }


def _fetch_table(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table_name};")
        columns = [desc[0] for desc in cur.description]
        rows = []
        for raw in cur.fetchall():
            rows.append(dict(zip(columns, raw)))
    return rows


def _load_users(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, username FROM monday_users;")
        rows = cur.fetchall()
    return {str(row[0]): row[1] for row in rows}


def _build_user_payload(users_map):
    return [{"id": uid, "name": name} for uid, name in users_map.items()]


def _build_graphql_payload(items, subitems, user_payload):
    subitems_by_parent = {}
    for subitem in subitems or []:
        parent_id = subitem.get("parent_item_id")
        if parent_id is None:
            continue
        subitems_by_parent.setdefault(parent_id, []).append(
            {
                "id": str(subitem.get("subitem_id")),
                "name": subitem.get("name"),
                "column_values": _column_blob_to_list(subitem.get("column_values")),
            }
        )

    graphql_items = []
    for item in items or []:
        item_id = item.get("item_id")
        graphql_items.append(
            {
                "id": str(item_id),
                "name": item.get("name"),
                "column_values": _column_blob_to_list(item.get("column_values")),
                "subitems": subitems_by_parent.get(item_id, []),
            }
        )

    return {
        "data": {
            "users": user_payload,
            "boards": [
                {
                    "items_page": {
                        "items": graphql_items,
                        "cursor": None,
                    }
                }
            ],
        }
    }


def load_board_payloads_from_database(board_keys=None):
    """
    Build GraphQL-style payloads from the stored database snapshot.
    Returns (users_map, {board_key: payload})
    """
    board_keys = board_keys or BOARD_TABLES.keys()
    with get_connection() as conn:
        ensure_schema(conn)
        users_map = _load_users(conn)
        user_payload = _build_user_payload(users_map)
        payloads = {}
        for board_key in board_keys:
            tables = BOARD_TABLES.get(board_key)
            if not tables:
                continue
            items = _fetch_table(conn, tables["items"])
            subitems = _fetch_table(conn, tables["subitems"])
            if not items and not subitems:
                continue
            payloads[board_key] = _build_graphql_payload(items, subitems, user_payload)
    return users_map, payloads
