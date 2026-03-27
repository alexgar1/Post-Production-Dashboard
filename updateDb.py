import os
import requests
import sys

from db_helpers import sync_monday_database

# Environment variable used to fetch the monday.com API token.
MONDAY_API_KEY_ENV = "MONDAY_API_KEY"

# monday.com GraphQL endpoint
API_URL = "https://api.monday.com/v2"

_cached_headers = None


def get_api_headers():
    """
    Build monday.com request headers lazily so commands that rely on cached
    data do not require the API key at import time.
    """
    global _cached_headers
    if _cached_headers is None:
        api_key = os.environ.get(MONDAY_API_KEY_ENV)
        if not api_key:
            raise RuntimeError(
                f"{MONDAY_API_KEY_ENV} environment variable is not set. "
                "Set it to a valid monday.com API token before calling the API."
            )
        _cached_headers = {
            "Authorization": api_key,
            "Content-Type": "application/json",
        }
    return _cached_headers

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
        response = requests.post(
            API_URL,
            headers=get_api_headers(),
            json={"query": query},
        )
        response.raise_for_status()
        data = response.json()


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


    return aggregated_data


def sync_from_monday():
    """Fetch listing/social boards from monday.com and persist them to Postgres."""
    print("Fetching latest board data from monday.com...")
    listing_data = get_all_items(LISTING_BOARD, QUERY)
    social_data = get_all_items(SOCIAL_BOARD, QUERY)

    sync_monday_database(
        {
            "listing": {"board_id": LISTING_BOARD, "data": listing_data},
            "social": {"board_id": SOCIAL_BOARD, "data": social_data},
        }
    )
    print("Synced Monday data into Postgres.")


def main():
    args = sys.argv[1:]
    if args:
        print("updateDb no longer generates reports. Run analytics.py for reporting.")
        return 1

    try:
        sync_from_monday()
    except Exception as exc:
        print(f"Error: Failed to sync Monday data to Postgres: {exc}")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
