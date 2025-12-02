## Ops dashboard stack

- **Frontend**: `www/index.html` is a React 18 + Tailwind single page app served by nginx. It hits `/api/...` routes for data.
- **Backend**: `server.py` exposes Flask endpoints that talk to PostgreSQL via `psycopg`.
- **Database**: PostgreSQL schema lives in `schema.sql` with the requested `listings` and `social` tables.

## Setup

1. Create and activate a virtualenv (optional but recommended):
   ```bash
   cd /home/alex_g/src
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Provision PostgreSQL (locally or in the cloud) and create the application database, e.g.:
   ```bash
   createdb ops_dashboard
   psql ops_dashboard < schema.sql
   ```
4. Export connection settings so Flask can talk to the database (adjust to your environment). `DATABASE_URL` also works.
   ```bash
   export POSTGRES_DB=ops_dashboard
   export POSTGRES_USER=postgres
   export POSTGRES_PASSWORD=postgres
   export POSTGRES_HOST=localhost
   export POSTGRES_PORT=5432
   ```
5. Start the Flask API:
   ```bash
   python server.py
   ```
6. Serve the frontend. `start_nginx.sh` already points nginx at `www/` and proxies `/api` to port `5000`.

## API quick reference

- `GET /api/status` – confirms Flask ↔ PostgreSQL connectivity.
- `GET /api/listings` / `POST /api/listings` – CRUD entry point for the `listings` table.
- `GET /api/social` / `POST /api/social` – capture social metrics in the `social` table.

Hit `http://<host>/` after nginx is up to interact with the React dashboard. The forms write directly into PostgreSQL, so you can verify rows with `psql` as needed.
