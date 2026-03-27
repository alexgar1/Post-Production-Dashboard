# Post Production Dashboard

This version is structured for Vercel Hobby instead of a long-running nginx host.

## What Changed

- `server.py` is the Flask entrypoint for Vercel Functions.
- `public/index.html` is served as the static dashboard UI.
- `vercel.json` schedules a daily cron request to `/api/cron/monday-sync`.
- `vercel.json` also sets the Flask function `maxDuration` to `300` seconds for Hobby-plan Fluid compute.
- Postgres schema creation is automatic on first request or sync.
- Monday board syncs now replace stale rows that were removed from the source boards.

## Required Environment Variables

- `DATABASE_URL`: Hosted Postgres connection string for Vercel.
- `MONDAY_API_KEY`: monday.com API token.
- `CRON_SECRET`: Secret used by Vercel Cron when calling the sync endpoint.

## Optional Environment Variables

- `MONDAY_LISTING_BOARD_ID`: Override the listing board id.
- `MONDAY_SOCIAL_BOARD_ID`: Override the social board id.
- `MONDAY_DB_SSLMODE`: Used only when `DATABASE_URL` is not set and discrete Postgres env vars are used.
- `MONDAY_API_TIMEOUT_SECONDS`: monday.com request timeout. Default: `30`.
- `FLASK_DEBUG`: Set to `1` for local debugging only.

## Vercel Deploy

1. Import the repository into Vercel.
2. Set the environment variables above in the Vercel project.
3. Deploy.

The cron schedule is defined in `vercel.json` as `0 13 * * *`, which is one run per day in UTC. On Hobby, Vercel executes cron jobs once per day and may trigger them at any time within the scheduled hour.

## Local Run

Install dependencies and start the Flask app:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python server.py
```

To force a manual sync locally:

```bash
python updateDb.py
```
# Post-Production-Dashboard-Vercel-Edition
