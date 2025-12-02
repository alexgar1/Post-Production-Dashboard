import requests
import json
import csv
import sys
from datetime import datetime, timedelta
import pytz

from db_helpers import sync_monday_database

# Replace with your monday.com API token
API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjU4MzUxMDMxMiwiYWFpIjoxMSwidWlkIjo3MDc1Nzk4NCwiaWFkIjoiMjAyNS0xMS0wNlQyMTowOToyMi4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6ODAzMDQxOCwicmduIjoidXNlMSJ9.v7lMujo5Yw4EkJjXal2vFDwwlcKPJvem9m_zCvZGtfk"

# monday.com GraphQL endpoint
API_URL = "https://api.monday.com/v2"

# Set up request headers
headers = {
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}

# Target board ID
SOCIAL_BOARD = "18164845624"
LISTING_BOARD = "6786034822"

# GraphQL query template to get all items with subitems and time tracking for a board


QUERY= """
query {
    users {
        id
        name
    }
    boards(ids: [%s]) {
        items_page(limit: 100%s) {
            items {
                id
                name
                subitems {
                    id
                    name
                    column_values {
                        id
                        type
                        text
                        value
                        ... on TimeTrackingValue {
                            history {
                                started_at
                                ended_at
                                started_user_id

                            }
                        }
                    }
                }
                column_values {
                    id
                    type
                    text
                    value
                    ... on TimeTrackingValue {
                        history {
                            started_at
                            ended_at
                            started_user_id

                        }
                    }
                }
            }
            cursor
        }
    }
}
"""

def get_all_items(board_id, query_template):
    """Fetch every page of items for the given board."""
    aggregated_items = []
    seen_ids = set()
    cursor = None
    stored_users = None

    while True:
        cursor_arg = f', cursor: "{cursor}"' if cursor else ""
        query = query_template % (board_id, cursor_arg)
        response = requests.post(API_URL, headers=headers, json={"query": query})
        response.raise_for_status()
        data = response.json()

        # Surface GraphQL errors immediately so they are easy to debug.
        if data.get("errors"):
            out_filename = f"hourly_response_{board_id}.json"
            with open(out_filename, 'w', encoding='utf-8') as fh:
                json.dump(data, fh, indent=2)
            raise RuntimeError(f"GraphQL query failed, see {out_filename} for details.")

        if stored_users is None:
            stored_users = data.get("data", {}).get("users", []) or []

        board = (data.get("data", {}).get("boards") or [{}])[0]
        page = board.get("items_page", {}) or {}
        items = page.get("items", []) or []

        for item in items:
            item_id = str(item.get("id"))
            if item_id in seen_ids:
                continue
            aggregated_items.append(item)
            seen_ids.add(item_id)

        cursor = page.get("cursor")
        if not cursor:
            break

    aggregated_data = {
        "data": {
            "users": stored_users or [],
            "boards": [
                {
                    "items_page": {
                        "items": aggregated_items,
                        "cursor": None
                    }
                }
            ]
        }
    }

    out_filename = f"hourly_response_{board_id}.json"
    with open(out_filename, 'w', encoding='utf-8') as fh:
        json.dump(aggregated_data, fh, indent=2)
    print(f"Wrote API response to: {out_filename}")

    return aggregated_data



def parse_time_tracking(data):

    items = data.get("data", {}).get("boards", [{}])[0].get("items_page", {}).get("items", [])
    # Normalize users into a mapping {id_str: name}
    users_list = data.get("data", {}).get("users", []) or []
    users = {}
    for u in users_list:
        try:
            uid = u.get("id")
            name = u.get("name") or u.get("title") or u.get("email") or ""
            if uid is not None:
                users[str(uid)] = name
        except Exception:
            continue
    # users now maps id -> name

    tt_history = {}

    for item in items:
        item_name = item.get("name", "")
        # Process item-level column values as well (time tracking can be on the item)
        column_values_item = item.get("column_values", [])


        for col in column_values_item:

            # get time tracking from subitems
            if col.get("subitems") != []:
                subitems = item.get("subitems", [])
                for subitem in subitems:
                    subitem_name = subitem.get("name", "")
                    column_values_subitem = subitem.get("column_values", [])
                    for sub_col in column_values_subitem:
                        if sub_col.get("type") == "time_tracking" and sub_col.get("history", []):
                            tt_history[f'{item_name} -  {subitem_name}'] = sub_col.get("history", [])


            # get time tracking from main item
            if col.get("type") == "time_tracking" and col.get("history", []):
                tt_history[item_name] = col.get("history", [])


    return tt_history, users


def load_time_tracking_from_json(filename="time_tracking.json"):
    '''
        Loads time tracking data from JSON file and converts it back to tt_history format.
        The JSON file has the grouped structure (user_id -> item_name -> sessions) along with
        a users mapping so names can be displayed.
        Returns (tt_history, users)
    '''
    try:
        with open(filename, 'r', encoding='utf-8') as fh:
            grouped_data = json.load(fh)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return None, None
    except json.JSONDecodeError as e:
        print(f"Error: Could not parse JSON file '{filename}': {e}")
        return None, None
    
    # Support both new structure {"users": {...}, "user_time_tracking": {...}}
    # and legacy structure {user_id: {item_name: [sessions]}}
    users = grouped_data.get("users") if isinstance(grouped_data, dict) else None
    raw_tt = grouped_data.get("user_time_tracking") if isinstance(grouped_data, dict) else grouped_data

    # Convert grouped structure back to tt_history format
    # raw_tt: {user_id: {item_name: [sessions]}}
    # tt_history: {item_name: [sessions]}
    tt_history = {}
    
    for user_id, items in (raw_tt or {}).items():
        for item_name, sessions in items.items():
            if item_name not in tt_history:
                tt_history[item_name] = []
            # Add all sessions for this item
            tt_history[item_name].extend(sessions)
    
    return tt_history, users


def period_report(tt_history, start_date, end_date, filename, users=None):
    ''' 
        Aggregates total time spent by each editor within the period.
        If a time tracking session overlaps with 12:00:00 AM of start period or 11:59:59 PM of end period,
        splits that time tracking such that it only accounts for time within the actual defined period.
        start date and end date are in the format of 'MM-DD'
        users: optional dictionary mapping user_id (str) to user name.
        Writes the per-user hours (split by day) and overall total to the CSV file named by filename.
    '''
    # Parse the date strings (MM-DD format)
    try:
        start_month, start_day = map(int, start_date.split('-'))
        end_month, end_day = map(int, end_date.split('-'))
    except ValueError:
        print(f"Error: Invalid date format. Expected MM-DD, got start_date={start_date}, end_date={end_date}")
        return
    
    current_year = datetime.now().year
    
    period_start = datetime(current_year, start_month, start_day, 0, 0, 0)
    period_end = datetime(current_year, end_month, end_day, 0, 0, 0) + timedelta(days=1)
    
    period_start = pytz.UTC.localize(period_start)
    period_end = pytz.UTC.localize(period_end)

    day_starts = []
    current_day = period_start
    while current_day < period_end:
        day_starts.append(current_day)
        current_day = current_day + timedelta(days=1)
    day_labels = [dt.strftime("%m-%d") for dt in day_starts]
    
    user_daily = {}
    user_total_time = {}
    
    for item_name, sessions in tt_history.items():
        for session in sessions:
            user_id = session.get("started_user_id")
            if not user_id:
                continue
            
            started_at_str = session.get("started_at")
            ended_at_str = session.get("ended_at")
            
            if not started_at_str or not ended_at_str:
                continue
            
            try:
                started_at = datetime.fromisoformat(started_at_str.replace('Z', '+00:00'))
                ended_at = datetime.fromisoformat(ended_at_str.replace('Z', '+00:00'))
                
                if started_at.tzinfo is None:
                    started_at = pytz.UTC.localize(started_at)
                if ended_at.tzinfo is None:
                    ended_at = pytz.UTC.localize(ended_at)
                
                if started_at < period_end and ended_at > period_start:
                    clip_start = max(started_at, period_start)
                    clip_end = min(ended_at, period_end)
                    
                    day_cursor = clip_start.replace(hour=0, minute=0, second=0, microsecond=0)
                    while day_cursor < clip_end:
                        next_day = day_cursor + timedelta(days=1)
                        segment_start = clip_start if clip_start > day_cursor else day_cursor
                        segment_end = clip_end if clip_end < next_day else next_day
                        time_delta = (segment_end - segment_start).total_seconds()
                        if time_delta > 0:
                            if user_id not in user_total_time:
                                user_total_time[user_id] = 0
                            user_total_time[user_id] += time_delta
                            if user_id not in user_daily:
                                user_daily[user_id] = {}
                            user_daily[user_id][day_cursor.date()] = user_daily[user_id].get(day_cursor.date(), 0) + time_delta
                        day_cursor = next_day
                        
            except (ValueError, TypeError) as e:
                print(f"Warning: Could not parse datetime for session: {e}")
                continue
    
    print("\nPeriod Report:")
    display_end = (period_end - timedelta(seconds=1)).strftime("%m-%d %I:%M:%S %p")
    print(f"Period: {start_date} 12:00:00 AM to {display_end}")
    print("-" * 60)
    
    if not user_total_time:
        print("No time tracking found in the specified period.")
        return
    
    sorted_users = sorted(user_total_time.items(), key=lambda x: x[1], reverse=True)
    
    excluded_user_id = "70757984"
    
    csv_rows = []

    for user_id, total_seconds in sorted_users:
        if str(user_id) == excluded_user_id:
            continue
            
        total_hours = total_seconds / 3600
        
        user_display = users.get(str(user_id), f"User ID {user_id}") if users else f"User ID {user_id}"
        print(f"{user_display}: {total_hours:.2f} hours")
        daily_hours = []
        for day_start in day_starts:
            day_total_seconds = (user_daily.get(user_id, {}) or {}).get(day_start.date(), 0)
            daily_hours.append(day_total_seconds / 3600)
        csv_rows.append((user_display, daily_hours, total_hours))
    
    print("-" * 60)
    total_all = sum(seconds for uid, seconds in user_total_time.items() if str(uid) != excluded_user_id)
    total_hours_all = total_all / 3600
    print(f"Total: {total_hours_all:.2f} hours")

    try:
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            header = ["Name"] + day_labels + ["Total"]
            writer.writerow(header)

            for name, day_hours_list, total_hours in csv_rows:
                row = [name] + [f"{hrs:.2f}" for hrs in day_hours_list] + [f"{total_hours:.2f}"]
                writer.writerow(row)

            day_totals = []
            for day_start in day_starts:
                total_for_day = 0
                for uid, per_day in user_daily.items():
                    if str(uid) == excluded_user_id:
                        continue
                    total_for_day += per_day.get(day_start.date(), 0)
                day_totals.append(total_for_day / 3600)
            writer.writerow(
                ["Total"] + [f"{hrs:.2f}" for hrs in day_totals] + [f"{total_hours_all:.2f}"]
            )
        print(f"Wrote period report to {filename}")
    except Exception as e:
        print(f"Failed to write period report to {filename}: {e}")
    
    return user_total_time



def main():
    args = sys.argv[1:]
    
    # Check for --use-cache flag
    use_cache = False
    if '--use-cache' in args or '--from-file' in args:
        use_cache = True
        args = [arg for arg in args if arg not in ['--use-cache', '--from-file']]
    
    if len(args) != 2:
        print("Usage: python hourlyReport.py [--use-cache] <start_date> <end_date>")
        print("  --use-cache: Load data from time_tracking.json instead of calling API")
        sys.exit(1)
    
    start_date = args[0]
    end_date = args[1]
    
    if use_cache:
        print("Loading time tracking data from time_tracking.json...")
        time_tracking, users = load_time_tracking_from_json()
        if time_tracking is None:
            sys.exit(1)
        print("Data loaded successfully.")
    else:
        # Fetch from API
        print("Fetching data from API...")
        listing_data = get_all_items(LISTING_BOARD, QUERY)
        social_data = get_all_items(SOCIAL_BOARD, QUERY)
        
        listing_time_tracking, listing_users = parse_time_tracking(listing_data)
        social_time_tracking, social_users = parse_time_tracking(social_data)

        # Merge time tracking and user maps from both boards
        time_tracking = {}
        time_tracking.update(listing_time_tracking or {})
        time_tracking.update(social_time_tracking or {})

        users = {}
        users.update(listing_users or {})
        users.update(social_users or {})

        try:
            sync_monday_database(
                {
                    "listing": {"board_id": LISTING_BOARD, "data": listing_data},
                    "social": {"board_id": SOCIAL_BOARD, "data": social_data},
                },
                users=users,
            )
            print("Synced Monday data into Postgres.")
        except Exception as exc:
            print(f"Warning: Failed to sync Monday data to Postgres: {exc}")

    output_filename = f"period_report_{start_date}_to_{end_date}.csv"
    period_report(time_tracking, start_date, end_date, output_filename, users)


    


if __name__ == "__main__":
    main()
