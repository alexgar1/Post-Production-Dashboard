"""Minimal Flask API for the Post Production Views dashboard."""

from __future__ import annotations

import os

from flask import Flask, Response, jsonify, request

from db_helpers import DatabaseDriverMissing, get_connection
from query import build_period_report_csv, compute_period_editor_hours, list_editors_with_sessions

app = Flask(__name__)

DEBUG_MODE = True


def _error_response(message: str, status_code: int = 500):
    return jsonify({"status": "error", "message": message}), status_code


@app.route("/api/status", methods=["GET"])
def status():
    """Confirm Flask can reach Postgres."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
    except DatabaseDriverMissing as exc:
        return _error_response(str(exc), status_code=500)
    except Exception:
        return _error_response("database_unavailable", status_code=500)
    return jsonify({"status": "ok"})


@app.route("/api/period-report", methods=["GET"])
def period_report_endpoint():
    """Generate a period report for the requested date range."""
    start_date = request.args.get("start")
    end_date = request.args.get("end")
    if not start_date or not end_date:
        return _error_response("start_and_end_are_required", status_code=400)

    try:
        summary = compute_period_editor_hours(start_date, end_date)
    except ValueError as exc:
        return _error_response(str(exc), status_code=400)
    except DatabaseDriverMissing as exc:
        return _error_response(str(exc), status_code=500)
    except Exception:
        return _error_response("failed_to_build_period_report", status_code=500)

    if request.args.get("format") == "csv":
        csv_payload = build_period_report_csv(summary)
        filename = f"period_report_{start_date}_to_{end_date}.csv"
        response = Response(csv_payload, mimetype="text/csv")
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    return jsonify({"status": "ok", **summary})


@app.route("/api/editors", methods=["GET"])
def editors_endpoint():
    """Return editors that have logged time tracking."""
    try:
        editors = list_editors_with_sessions()
    except DatabaseDriverMissing as exc:
        return _error_response(str(exc), status_code=500)
    except Exception:
        return _error_response("failed_to_load_editors", status_code=500)

    return jsonify({"status": "ok", "editors": editors})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=DEBUG_MODE)
