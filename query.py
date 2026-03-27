"""Interactive CLI to report time tracked per editor for a date range."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import io
import csv
from typing import Dict, Iterable, List, Mapping, Tuple

from db_helpers import DatabaseDriverMissing, get_connection

TIME_TRACKING_TABLES = (
    "listing_items",
    "listing_subitems",
    "social_items",
    "social_subitems",
)

# User to exclude from totals (matches the legacy JSON-based script).
EXCLUDED_USER_ID = "70757984"


def _coerce_period_date(date_str: str, fallback_year: int) -> datetime:
    """Return a naive datetime for the provided date string."""
    sanitized = (date_str or "").strip()
    if not sanitized:
        raise ValueError("missing_date")
    for fmt in ("%Y-%m-%d", "%m-%d"):
        try:
            parsed = datetime.strptime(sanitized, fmt)
            if fmt == "%m-%d":
                parsed = parsed.replace(year=fallback_year)
            return parsed
        except ValueError:
            continue
    raise ValueError("invalid_date_format")


def _resolve_period_bounds(start_input: str, end_input: str) -> Tuple[datetime, datetime]:
    """Return (period_start, period_end_exclusive) datetimes in UTC."""
    current_year = datetime.now(timezone.utc).year
    start_naive = _coerce_period_date(start_input, current_year)
    end_naive = _coerce_period_date(end_input, current_year)
    period_start = datetime(
        start_naive.year, start_naive.month, start_naive.day, tzinfo=timezone.utc
    )
    period_end = datetime(end_naive.year, end_naive.month, end_naive.day, tzinfo=timezone.utc) + timedelta(
        days=1
    )
    if period_start >= period_end:
        raise ValueError("start_after_end")
    return period_start, period_end


def _build_day_sequence(
    period_start: datetime, period_end: datetime
) -> Tuple[List[datetime], List[str]]:
    """Return a list of day start datetimes and their labels."""
    day_starts: List[datetime] = []
    cursor = period_start
    while cursor < period_end:
        day_starts.append(cursor)
        cursor += timedelta(days=1)
    day_labels = [day.strftime("%m-%d") for day in day_starts]
    return day_starts, day_labels


def _load_user_map() -> Dict[str, Dict[str, object]]:
    """Return a map of user_id -> user info from the database."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id::text, username, pay_rate FROM monday_users;")
            rows = cur.fetchall()
    return {row[0]: {"name": row[1], "pay_rate": row[2]} for row in rows}


def _query_time_tracking_user_ids() -> List[str]:
    """Return user ids that have at least one completed time tracking session."""
    columns_union = " UNION ALL ".join(f"SELECT column_values FROM {table}" for table in TIME_TRACKING_TABLES)
    sql = f"""
        WITH raw_columns AS (
            {columns_union}
        ),
        sessions AS (
            SELECT DISTINCT (session->>'started_user_id') AS user_id
            FROM raw_columns rc
            CROSS JOIN LATERAL jsonb_each(COALESCE(rc.column_values, '{{}}'::jsonb)) AS col(col_id, payload)
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(payload->'history', '[]'::jsonb)) AS session
            WHERE payload->>'type' = 'time_tracking'
              AND session->>'started_user_id' IS NOT NULL
              AND (session->>'ended_at') IS NOT NULL
        )
        SELECT DISTINCT user_id
        FROM sessions
        WHERE user_id IS NOT NULL;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return [row[0] for row in rows]


def list_editors_with_sessions() -> List[Dict[str, str]]:
    """Return editors that have logged time tracking at least once."""
    user_ids = _query_time_tracking_user_ids()
    users_map = _load_user_map()
    editors: List[Dict[str, str]] = []
    for raw_id in user_ids:
        if raw_id is None or str(raw_id) == EXCLUDED_USER_ID:
            continue
        user_id = str(raw_id)
        user_info = users_map.get(user_id, {})
        name = user_info.get("name") or f"User {user_id}"
        editors.append({"userId": user_id, "name": name})
    editors.sort(key=lambda editor: editor["name"].lower())
    return editors


def _query_period_daily_seconds(period_start: datetime, period_end: datetime):
    """Fetch per-user, per-day time tracking seconds within the period."""
    columns_union = " UNION ALL ".join(
        f"SELECT column_values FROM {table}" for table in TIME_TRACKING_TABLES
    )
    sql = f"""
        WITH bounds AS (
            SELECT %(start)s::timestamptz AS period_start,
                   %(end)s::timestamptz AS period_end
        ),
        raw_columns AS (
            {columns_union}
        ),
        sessions AS (
            SELECT
                (session->>'started_user_id') AS user_id,
                (session->>'started_at')::timestamptz AS started_at,
                (session->>'ended_at')::timestamptz AS ended_at
            FROM raw_columns rc
            CROSS JOIN LATERAL jsonb_each(COALESCE(rc.column_values, '{{}}'::jsonb)) AS col(col_id, payload)
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(payload->'history', '[]'::jsonb)) AS session
            WHERE payload->>'type' = 'time_tracking'
        ),
        clipped AS (
            SELECT
                user_id,
                GREATEST(started_at, b.period_start) AS clip_start,
                LEAST(ended_at, b.period_end) AS clip_end
            FROM sessions s
            CROSS JOIN bounds b
            WHERE started_at IS NOT NULL
              AND ended_at IS NOT NULL
              AND started_at < b.period_end
              AND ended_at > b.period_start
        ),
        expanded AS (
            SELECT
                user_id,
                day_start::date AS day_date,
                GREATEST(clip_start, day_start) AS segment_start,
                LEAST(clip_end, day_start + INTERVAL '1 day') AS segment_end
            FROM (
                SELECT
                    user_id,
                    clip_start,
                    clip_end,
                    generate_series(
                        date_trunc('day', clip_start),
                        date_trunc('day', clip_end - INTERVAL '1 second'),
                        INTERVAL '1 day'
                    ) AS day_start
                FROM clipped
            ) g
            WHERE clip_end > clip_start
        ),
        ordered AS (
            SELECT
                user_id,
                day_date,
                segment_start,
                segment_end,
                LAG(segment_end) OVER (PARTITION BY user_id, day_date ORDER BY segment_start) AS prev_end
            FROM expanded
        ),
        islands AS (
            SELECT
                user_id,
                day_date,
                segment_start,
                segment_end,
                SUM(
                    CASE WHEN segment_start > COALESCE(prev_end, segment_start - INTERVAL '1 microsecond') THEN 1 ELSE 0 END
                ) OVER (PARTITION BY user_id, day_date ORDER BY segment_start) AS island_id
            FROM ordered
        ),
        merged AS (
            SELECT
                user_id,
                day_date,
                MIN(segment_start) AS island_start,
                MAX(segment_end) AS island_end
            FROM islands
            GROUP BY user_id, day_date, island_id
        )
        SELECT
            user_id,
            day_date,
            SUM(EXTRACT(EPOCH FROM island_end - island_start)) AS seconds
        FROM merged
        WHERE island_end > island_start
        GROUP BY user_id, day_date
        ORDER BY user_id, day_date;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"start": period_start, "end": period_end})
            return cur.fetchall()


def _query_period_sessions(period_start: datetime, period_end: datetime):
    """Fetch per-user sessions clipped to the requested period."""
    columns_union = """
        SELECT column_values, name AS pulse_name, item_id::bigint AS pulse_id, board_id::bigint AS board_id,
               NULL::bigint AS parent_item_id, FALSE AS is_subitem
        FROM listing_items
        UNION ALL
        SELECT column_values, name AS pulse_name, item_id::bigint AS pulse_id, board_id::bigint AS board_id,
               NULL::bigint AS parent_item_id, FALSE AS is_subitem
        FROM social_items
        UNION ALL
        SELECT column_values, name AS pulse_name, subitem_id::bigint AS pulse_id, NULL::bigint AS board_id,
               parent_item_id::bigint AS parent_item_id, TRUE AS is_subitem
        FROM listing_subitems
        UNION ALL
        SELECT column_values, name AS pulse_name, subitem_id::bigint AS pulse_id, NULL::bigint AS board_id,
               parent_item_id::bigint AS parent_item_id, TRUE AS is_subitem
        FROM social_subitems
    """
    sql = f"""
        WITH bounds AS (
            SELECT %(start)s::timestamptz AS period_start,
                   %(end)s::timestamptz AS period_end
        ),
        raw_columns AS (
            {columns_union}
        ),
        parent_items AS (
            SELECT item_id::bigint AS item_id, name AS item_name, board_id::bigint AS board_id FROM listing_items
            UNION ALL
            SELECT item_id::bigint AS item_id, name AS item_name, board_id::bigint AS board_id FROM social_items
        ),
        sessions AS (
            SELECT
                (session->>'started_user_id') AS user_id,
                (session->>'started_at')::timestamptz AS started_at,
                (session->>'ended_at')::timestamptz AS ended_at,
                rc.pulse_name,
                rc.pulse_id,
                COALESCE(rc.board_id, pi.board_id) AS board_id,
                rc.is_subitem,
                pi.item_name AS parent_name
            FROM raw_columns rc
            LEFT JOIN parent_items pi ON rc.parent_item_id = pi.item_id
            CROSS JOIN LATERAL jsonb_each(COALESCE(rc.column_values, '{{}}'::jsonb)) AS col(col_id, payload)
            CROSS JOIN LATERAL jsonb_array_elements(COALESCE(payload->'history', '[]'::jsonb)) AS session
            WHERE payload->>'type' = 'time_tracking'
        )
        SELECT
            user_id,
            GREATEST(started_at, b.period_start) AS clip_start,
            LEAST(ended_at, b.period_end) AS clip_end,
            pulse_name,
            parent_name,
            board_id,
            pulse_id,
            is_subitem
        FROM sessions s
        CROSS JOIN bounds b
        WHERE started_at IS NOT NULL
          AND ended_at IS NOT NULL
          AND started_at < b.period_end
          AND ended_at > b.period_start
          AND GREATEST(started_at, b.period_start) < LEAST(ended_at, b.period_end)
        ORDER BY user_id, clip_start;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"start": period_start, "end": period_end})
            return cur.fetchall()


def _build_summary(
    period_start: datetime,
    period_end: datetime,
    day_starts: List[datetime],
    day_labels: List[str],
    user_daily: Mapping[str, Mapping[date, float]],
    users_map: Mapping[str, Mapping[str, object]],
    user_sessions: Mapping[str, Iterable[Tuple[datetime, datetime, str | None, str | None, int | None, int | None, bool]]] | None = None,
) -> Dict[str, object]:
    editors: List[Dict[str, object]] = []
    day_totals: List[float] = []
    grand_total_seconds = 0.0
    session_map = user_sessions or {}

    for user_id, per_day in user_daily.items():
        if str(user_id) == EXCLUDED_USER_ID:
            continue
        daily_hours = []
        total_seconds = 0.0
        for day_start in day_starts:
            seconds = float(per_day.get(day_start.date(), 0.0))
            total_seconds += seconds
            daily_hours.append(round(seconds / 3600, 2))
        grand_total_seconds += total_seconds
        sessions_payload = []
        for start_dt, end_dt, item_name, parent_name, board_id, pulse_id, is_subitem in session_map.get(
            str(user_id), []
        ):
            duration_hours = round((end_dt - start_dt).total_seconds() / 3600, 2)
            label = item_name or parent_name
            if is_subitem and parent_name and item_name:
                label = f"{parent_name} - {item_name}"
            sessions_payload.append(
                {
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                    "hours": duration_hours,
                    "itemName": label,
                    "boardId": board_id,
                    "pulseId": pulse_id,
                }
            )
        user_info = users_map.get(str(user_id), {})
        name = user_info.get("name") or f"User {user_id}"
        pay_rate_raw = user_info.get("pay_rate")
        pay_rate = float(pay_rate_raw) if pay_rate_raw is not None else None
        total_hours = round(total_seconds / 3600, 2)
        pay_total = round(total_hours * pay_rate, 2) if pay_rate is not None else None
        editors.append(
            {
                "userId": str(user_id),
                "name": name,
                "hours": total_hours,
                "dailyHours": daily_hours,
                "payRate": pay_rate,
                "payTotal": pay_total,
                "sessions": sessions_payload,
            }
        )

    for day_start in day_starts:
        total_seconds = 0.0
        for user_id, per_day in user_daily.items():
            if str(user_id) == EXCLUDED_USER_ID:
                continue
            total_seconds += float(per_day.get(day_start.date(), 0.0))
        day_totals.append(round(total_seconds / 3600, 2))

    editors.sort(key=lambda editor: editor["hours"], reverse=True)
    return {
        "periodStart": period_start.isoformat(),
        "periodEnd": period_end.isoformat(),
        "dayLabels": day_labels,
        "dayTotals": day_totals,
        "editors": editors,
        "grandTotalHours": round(grand_total_seconds / 3600, 2),
        "editorCount": len(editors),
    }


def compute_period_editor_hours(start_date: str, end_date: str) -> Dict[str, object]:
    """
    Summarize editor hours within the requested period using Postgres data.
    Accepts YYYY-MM-DD or MM-DD date strings.
    """
    period_start, period_end = _resolve_period_bounds(start_date, end_date)
    day_starts, day_labels = _build_day_sequence(period_start, period_end)
    rows = _query_period_daily_seconds(period_start, period_end)
    session_rows = _query_period_sessions(period_start, period_end)
    user_daily: Dict[str, Dict[date, float]] = {}
    user_sessions: Dict[str, List[Tuple[datetime, datetime, str | None]]] = {}
    for user_id, day_date, seconds in rows or []:
        if not user_id or day_date is None or seconds is None:
            continue
        per_day = user_daily.setdefault(str(user_id), {})
        per_day[day_date] = per_day.get(day_date, 0.0) + float(seconds)
    for user_id, start_dt, end_dt, item_name, parent_name, board_id, pulse_id, is_subitem in session_rows or []:
        if not user_id or start_dt is None or end_dt is None:
            continue
        if str(user_id) == EXCLUDED_USER_ID:
            continue
        user_sessions.setdefault(str(user_id), []).append(
            (start_dt, end_dt, item_name, parent_name, board_id, pulse_id, bool(is_subitem))
        )
    for session_list in user_sessions.values():
        session_list.sort(key=lambda pair: pair[0])
    users_map = _load_user_map()
    return _build_summary(
        period_start,
        period_end,
        day_starts,
        day_labels,
        user_daily,
        users_map,
        user_sessions,
    )


def build_period_report_csv(summary: Mapping[str, object]) -> str:
    """Convert a period summary dict into CSV text."""
    output = io.StringIO()
    writer = csv.writer(output)
    day_labels = summary.get("dayLabels") or []
    editors = summary.get("editors") or []
    writer.writerow(["Name", "Pay Rate"] + list(day_labels) + ["Total", "Total Pay"])
    total_pay = 0.0
    for editor in editors:
        daily_hours = editor.get("dailyHours") or []
        total_hours = editor.get("hours", 0.0)
        pay_rate = editor.get("payRate")
        pay_rate_value = float(pay_rate) if pay_rate is not None else None
        pay_total = (
            float(editor.get("payTotal"))
            if editor.get("payTotal") is not None
            else (round(float(total_hours) * pay_rate_value, 2) if pay_rate_value is not None else None)
        )
        if pay_total is not None:
            total_pay += float(pay_total)
        writer.writerow(
            [editor.get("name", ""), f"{pay_rate_value:.2f}" if pay_rate_value is not None else ""]
            + [f"{float(hour):.2f}" for hour in daily_hours]
            + [
                f"{float(total_hours):.2f}",
                f"{float(pay_total):.2f}" if pay_total is not None else "",
            ]
        )
    day_totals = summary.get("dayTotals") or [0.0] * len(day_labels)
    grand_total = summary.get("grandTotalHours", 0.0)
    writer.writerow(
        ["Total", ""]
        + [f"{float(hour):.2f}" for hour in day_totals]
        + [f"{float(grand_total):.2f}", f"{float(total_pay):.2f}"]
    )
    return output.getvalue()


def _print_summary(summary: Mapping[str, object]) -> None:
    """Pretty-print the summary to stdout."""
    period_start = datetime.fromisoformat(summary["periodStart"])
    period_end_exclusive = datetime.fromisoformat(summary["periodEnd"])
    display_end = (period_end_exclusive - timedelta(seconds=1)).strftime("%m-%d %I:%M:%S %p")

    print("\nPeriod Report:")
    print(f"Period: {period_start.strftime('%m-%d')} 12:00:00 AM to {display_end}")
    print("-" * 60)

    editors: Iterable[Mapping[str, object]] = summary.get("editors", [])  # type: ignore[arg-type]
    if not editors:
        print("No time tracking found in the specified period.")
        return

    for editor in editors:
        print(f"{editor.get('name', 'Unknown')}: {float(editor.get('hours', 0)):.2f} hours")

    print("-" * 60)
    print(f"Total: {float(summary.get('grandTotalHours', 0.0)):.2f} hours")


def main():
    try:
        start_date = input("Enter start date (YYYY-MM-DD or MM-DD): ").strip()
        end_date = input("Enter end date (YYYY-MM-DD or MM-DD): ").strip()
    except EOFError:
        print("Error: start and end dates must be provided via stdin.")
        raise SystemExit(1)

    if not start_date or not end_date:
        print("Error: both start and end dates are required.")
        raise SystemExit(1)

    try:
        summary = compute_period_editor_hours(start_date, end_date)
    except ValueError as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)
    except DatabaseDriverMissing as exc:
        print(f"Error: {exc}")
        raise SystemExit(1)
    except Exception as exc:
        print(f"Failed to build report: {exc}")
        raise SystemExit(1)

    _print_summary(summary)


if __name__ == "__main__":
    main()
