import os
from contextlib import contextmanager

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:  # pragma: no cover
    psycopg2 = None
    Json = None


DB_SETTINGS = {
    "dbname": os.getenv("MONDAY_DB_NAME", "monday_reports"),
    "user": os.getenv("MONDAY_DB_USER", "postgres"),
    "password": os.getenv("MONDAY_DB_PASSWORD", "postgres"),
    "host": os.getenv("MONDAY_DB_HOST", "127.0.0.1"),
    "port": os.getenv("MONDAY_DB_PORT", "5432"),
}

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


@contextmanager
def get_connection():
    """Yield a Postgres connection using the configured settings."""
    _ensure_driver()
    conn = psycopg2.connect(
        dbname=DB_SETTINGS["dbname"],
        user=DB_SETTINGS["user"],
        password=DB_SETTINGS["password"],
        host=DB_SETTINGS["host"],
        port=DB_SETTINGS["port"],
    )
    try:
        yield conn
    finally:
        conn.close()


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
        return

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
        return

    with get_connection() as conn:
        try:
            _store_users(conn, aggregated_users)
            for board_key, payload in formatted_payloads.items():
                _store_board_data(conn, board_key, payload)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
