# =============================================================================
# Order Desk — Alpaca Options Order Approval System
# =============================================================================
# HOW TO RUN:
# 1. Copy .env.example to .env and fill in your keys
# 2. Place service-account.json in this folder
# 3. pip install -r requirements.txt
# 4. streamlit run app.py
# =============================================================================

import os
import uuid
import requests
import streamlit as st
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config & credential validation
# ---------------------------------------------------------------------------
ALPACA_API_KEY        = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY     = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_BASE_URL       = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
GCP_PROJECT_ID        = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET            = os.getenv("BQ_DATASET", "FOR_EXTERNAL")
BQ_SOURCE_TABLE       = os.getenv("BQ_SOURCE_TABLE", "final_portfolio_optimization_paper")
BQ_LOG_TABLE          = os.getenv("BQ_LOG_TABLE", "order_execution_log")
GOOGLE_CREDS          = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./service-account.json")

MISSING_CREDS = []
if not ALPACA_API_KEY:    MISSING_CREDS.append("ALPACA_API_KEY")
if not ALPACA_SECRET_KEY: MISSING_CREDS.append("ALPACA_SECRET_KEY")
if not GCP_PROJECT_ID:    MISSING_CREDS.append("GCP_PROJECT_ID")

# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------
def get_bq_client():
    from google.cloud import bigquery
    return bigquery.Client(project=GCP_PROJECT_ID)


def ensure_log_table(client):
    from google.cloud import bigquery
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_LOG_TABLE}"
    schema = [
        bigquery.SchemaField("id",              "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("snapshot_date",   "DATE"),
        bigquery.SchemaField("ticker",          "STRING"),
        bigquery.SchemaField("option_type",     "STRING"),
        bigquery.SchemaField("step",            "INT64"),
        bigquery.SchemaField("alpaca_order_id", "STRING"),
        bigquery.SchemaField("status",          "STRING"),
        bigquery.SchemaField("submitted_at",    "TIMESTAMP"),
        bigquery.SchemaField("filled_at",       "TIMESTAMP"),
        bigquery.SchemaField("error_message",   "STRING"),
    ]
    try:
        client.get_table(table_ref)
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)


def fetch_trades(client) -> list[dict]:
    """Read pending trades from BigQuery source table."""
    query = f"""
        SELECT
            option_type,
            lookupvalue        AS ticker,
            snapshot_date,
            calls_strike,
            options_price,
            Close_Price,
            Option_Expiry_Date,
            prediction_prob
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_SOURCE_TABLE}`
        WHERE evaluation_status = 'PENDING_NEXT_DAY_DATA'
          AND snapshot_date = (
              SELECT MAX(snapshot_date)
              FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_SOURCE_TABLE}`
              WHERE evaluation_status = 'PENDING_NEXT_DAY_DATA'
          )
        ORDER BY prediction_prob DESC
    """
    rows = client.query(query).result()
    return [dict(row) for row in rows]


def fetch_log(client) -> list[dict]:
    """Read all log rows for trades in the current session."""
    query = f"""
        SELECT *
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_LOG_TABLE}`
        ORDER BY submitted_at DESC
    """
    try:
        rows = client.query(query).result()
        return [dict(row) for row in rows]
    except Exception:
        return []


def write_log_row(client, row: dict):
    from datetime import date
    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_LOG_TABLE}"
    serializable = {
        k: (v.isoformat() if isinstance(v, (datetime, date)) else v)
        for k, v in row.items()
    }
    errors = client.insert_rows_json(table_ref, [serializable])
    if errors:
        raise RuntimeError(f"BQ insert errors: {errors}")


def append_status_row(client, original_row: dict, updates: dict):
    """Append a new status row instead of updating (avoids streaming buffer DML error)."""
    new_row = {**original_row, **updates, "id": str(uuid.uuid4())}
    write_log_row(client, new_row)


# ---------------------------------------------------------------------------
# Alpaca helpers
# ---------------------------------------------------------------------------
ALPACA_HEADERS = {
    "APCA-API-KEY-ID":     ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    "Content-Type":        "application/json",
}


def alpaca_post(endpoint: str, payload: dict) -> dict:
    url = f"{ALPACA_BASE_URL}{endpoint}"
    resp = requests.post(url, json=payload, headers=ALPACA_HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def alpaca_get_order(order_id: str) -> dict:
    url = f"{ALPACA_BASE_URL}/v2/orders/{order_id}"
    resp = requests.get(url, headers=ALPACA_HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def build_option_symbol(ticker: str, expiry, option_type: str, strike: float) -> str:
    """OCC symbol: TICKER + YYMMDD + C/P + strike*1000 zero-padded to 8 digits."""
    if hasattr(expiry, "strftime"):
        exp_str = expiry.strftime("%y%m%d")
    else:
        exp_str = datetime.strptime(str(expiry), "%Y-%m-%d").strftime("%y%m%d")
    cp = "C" if option_type.lower() == "call" else "P"
    strike_int = int(round(float(strike) * 1000))
    return f"{ticker.upper()}{exp_str}{cp}{strike_int:08d}"


def submit_step1(trade: dict) -> dict:
    """Buy option contract (call or put)."""
    symbol = build_option_symbol(
        trade["ticker"],
        trade["Option_Expiry_Date"],
        trade["option_type"],
        trade["calls_strike"],
    )
    payload = {
        "symbol":      symbol,
        "qty":         "1",
        "side":        "buy",
        "type":        "market",
        "time_in_force": "day",
        "asset_class": "option",
    }
    return alpaca_post("/v2/orders", payload)


def submit_step2(trade: dict) -> dict:
    """Married position: sell 100 shares (call) or buy 100 shares (put)."""
    is_call = trade["option_type"].lower() == "call"
    payload = {
        "symbol":        trade["ticker"].upper(),
        "qty":           "100",
        "side":          "sell" if is_call else "buy",
        "type":          "market",
        "time_in_force": "day",
    }
    return alpaca_post("/v2/orders", payload)


def submit_step3(trade: dict) -> dict:
    """Closing order: reverse of Step 2 @ Close_Price + options_price."""
    is_call = trade["option_type"].lower() == "call"
    limit_price = round(float(trade["Close_Price"]) + float(trade["options_price"]), 2)
    payload = {
        "symbol":        trade["ticker"].upper(),
        "qty":           "100",
        "side":          "buy" if is_call else "sell",  # call: close short; put: close long
        "type":          "limit",
        "limit_price":   str(limit_price),
        "time_in_force": "gtc",
    }
    return alpaca_post("/v2/orders", payload)


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
STATUS_BADGE = {
    "pending_approval": "🟡 Pending Approval",
    "submitted":        "🔵 Submitted",
    "filled":           "🟢 Filled",
    "rejected":         "🔴 Rejected",
    "failed":           "❌ Failed",
}


def get_step_status(log_rows: list[dict], ticker: str, option_type: str, step: int):
    """Return the most-recent log row for this ticker/option_type/step, or None."""
    matches = [
        r for r in log_rows
        if r["ticker"] == ticker
        and r["option_type"] == option_type
        and int(r["step"]) == step
    ]
    if not matches:
        return None
    # Sort by submitted_at descending, handle None
    matches.sort(key=lambda r: r.get("submitted_at") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return matches[0]


# ---------------------------------------------------------------------------
# Refresh: poll Alpaca for fill status and update BQ
# ---------------------------------------------------------------------------
def refresh_all_statuses(client, log_rows: list[dict]) -> int:
    updated = 0
    for row in log_rows:
        if row.get("status") not in ("submitted",):
            continue
        order_id = row.get("alpaca_order_id")
        if not order_id:
            continue
        try:
            order = alpaca_get_order(order_id)
            alpaca_status = order.get("status", "")
            new_status = None
            filled_at  = None
            if alpaca_status in ("filled", "partially_filled"):
                new_status = "filled"
                raw_filled = order.get("filled_at")
                if raw_filled:
                    filled_at = datetime.fromisoformat(raw_filled.replace("Z", "+00:00"))
            elif alpaca_status in ("canceled", "expired", "rejected", "done_for_day"):
                new_status = "rejected"

            if new_status:
                upd: dict = {"status": new_status, "submitted_at": datetime.now(timezone.utc).isoformat()}
                if filled_at:
                    upd["filled_at"] = filled_at.isoformat()
                append_status_row(client, row, upd)
                row["status"] = new_status
                if filled_at:
                    row["filled_at"] = filled_at
                updated += 1
        except Exception as e:
            st.warning(f"Alpaca poll error for order {order_id}: {e}")
    return updated


# ---------------------------------------------------------------------------
# Approve / Reject actions
# ---------------------------------------------------------------------------
def do_approve(client, trade: dict, step: int, log_rows: list[dict]):
    row_id = str(uuid.uuid4())
    base = {
        "id":           row_id,
        "snapshot_date": str(trade["snapshot_date"]),
        "ticker":        trade["ticker"],
        "option_type":   trade["option_type"],
        "step":          step,
        "submitted_at":  datetime.now(timezone.utc).isoformat(),
        "filled_at":     None,
        "error_message": None,
    }
    try:
        if step == 1:
            resp = submit_step1(trade)
        elif step == 2:
            resp = submit_step2(trade)
        else:
            resp = submit_step3(trade)

        row = {**base, "alpaca_order_id": resp.get("id", ""), "status": "submitted"}
        ensure_log_table(client)
        write_log_row(client, row)
        log_rows.append(row)
        st.success(f"Step {step} submitted — Alpaca order ID: {resp.get('id')}")
    except Exception as e:
        row = {**base, "alpaca_order_id": None, "status": "failed", "error_message": str(e)}
        ensure_log_table(client)
        try:
            write_log_row(client, row)
        except Exception:
            pass
        log_rows.append(row)
        st.error(f"Step {step} failed: {e}")


def do_reject(client, trade: dict, step: int, log_rows: list[dict]):
    row_id = str(uuid.uuid4())
    row = {
        "id":            row_id,
        "snapshot_date": str(trade["snapshot_date"]),
        "ticker":        trade["ticker"],
        "option_type":   trade["option_type"],
        "step":          step,
        "alpaca_order_id": None,
        "status":        "rejected",
        "submitted_at":  datetime.now(timezone.utc).isoformat(),
        "filled_at":     None,
        "error_message": None,
    }
    ensure_log_table(client)
    write_log_row(client, row)
    log_rows.append(row)
    st.info(f"Step {step} rejected.")


# ---------------------------------------------------------------------------
# UI rendering
# ---------------------------------------------------------------------------
def render_step_row(
    client,
    trade: dict,
    step: int,
    label: str,
    details: str,
    log_rows: list[dict],
    unlocked: bool,
):
    ticker      = trade["ticker"]
    option_type = trade["option_type"]
    log_row     = get_step_status(log_rows, ticker, option_type, step)
    status      = log_row["status"] if log_row else "pending_approval"
    badge       = STATUS_BADGE.get(status, status)

    col1, col2, col3 = st.columns([4, 2, 2])
    with col1:
        st.markdown(f"**Step {step} — {label}**  \n{details}")
        st.caption(badge)
    with col2:
        if not unlocked:
            st.markdown("🔒 Locked")
        elif status in ("submitted", "filled", "rejected", "failed"):
            st.markdown(f"_{badge}_")
        else:
            key_approve = f"approve_{ticker}_{option_type}_{step}"
            if st.button("✅ Approve", key=key_approve):
                do_approve(client, trade, step, log_rows)
                st.rerun()
    with col3:
        if unlocked and status == "pending_approval":
            key_reject = f"reject_{ticker}_{option_type}_{step}"
            if st.button("❌ Reject", key=key_reject):
                do_reject(client, trade, step, log_rows)
                st.rerun()


def render_trade_card(client, trade: dict, log_rows: list[dict]):
    ticker      = trade["ticker"]
    option_type = trade["option_type"].upper()
    conf        = float(trade.get("prediction_prob", 0)) * 100
    expiry      = trade.get("Option_Expiry_Date", "")
    strike      = trade.get("calls_strike", "")
    premium     = trade.get("options_price", "")
    close       = trade.get("Close_Price", "")
    step3_limit = round(float(close or 0) + float(premium or 0), 2)

    badge_color = "#1a6e3c" if option_type == "CALL" else "#6e1a1a"
    header = (
        f'<span style="background:{badge_color};padding:2px 8px;border-radius:4px;'
        f'font-weight:bold;">{option_type}</span> &nbsp;'
        f'**{ticker}** &nbsp;|&nbsp; Conf: {conf:.1f}% &nbsp;|&nbsp; '
        f'Expiry: {expiry} &nbsp;|&nbsp; Strike: ${strike}'
    )

    with st.expander(f"{option_type}  {ticker}  |  {conf:.1f}%  |  Strike ${strike}  |  Exp {expiry}", expanded=True):
        st.markdown(header, unsafe_allow_html=True)
        st.markdown("---")

        # Determine step statuses
        s1 = get_step_status(log_rows, ticker, trade["option_type"], 1)
        s2 = get_step_status(log_rows, ticker, trade["option_type"], 2)
        s1_status = s1["status"] if s1 else "pending_approval"
        s2_status = s2["status"] if s2 else "pending_approval"

        # Step 1 details
        if option_type == "CALL":
            step1_label   = "Buy CALL Option"
            step1_details = f"Buy 1 CALL contract @ strike ${strike}, expiry {expiry}, premium ~${premium}"
            step2_label   = "Married Put — Sell Shares"
            step2_details = f"Sell 100 shares of {ticker} @ market"
            step3_label   = "Closing — Buy Shares"
            step3_details = f"Buy 100 shares of {ticker} @ limit ${step3_limit} (${close} + ${premium} premium)"
        else:
            step1_label   = "Buy PUT Option"
            step1_details = f"Buy 1 PUT contract @ strike ${strike}, expiry {expiry}, premium ~${premium}"
            step2_label   = "Married Call — Buy Shares"
            step2_details = f"Buy 100 shares of {ticker} @ market"
            step3_label   = "Closing — Sell Shares"
            step3_details = f"Sell 100 shares of {ticker} @ limit ${step3_limit} (${close} + ${premium} premium)"

        render_step_row(client, trade, 1, step1_label, step1_details, log_rows, unlocked=True)
        st.divider()
        render_step_row(client, trade, 2, step2_label, step2_details, log_rows, unlocked=(s1_status == "filled"))
        st.divider()
        render_step_row(client, trade, 3, step3_label, step3_details, log_rows, unlocked=(s2_status == "filled"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Order Desk", layout="wide")
    st.title("Order Desk")

    # Credential check
    if MISSING_CREDS:
        st.error(f"Missing required environment variables: {', '.join(MISSING_CREDS)}. "
                 f"Copy .env.example to .env and fill in the values.")
        st.stop()

    # BQ client
    try:
        client = get_bq_client()
    except Exception as e:
        st.error(f"Failed to connect to BigQuery: {e}")
        st.stop()

    # Session state init
    if "trades" not in st.session_state:
        st.session_state.trades   = None
        st.session_state.log_rows = None

    # Top bar
    col_title, col_btn = st.columns([8, 2])
    with col_btn:
        refresh_clicked = st.button("🔄 Refresh Status", use_container_width=True)

    # Load / refresh data
    if st.session_state.trades is None or refresh_clicked:
        with st.spinner("Loading trades from BigQuery…"):
            try:
                st.session_state.trades = fetch_trades(client)
            except Exception as e:
                st.error(f"BigQuery read error: {e}")
                st.stop()

        with st.spinner("Loading order log…"):
            try:
                ensure_log_table(client)
                st.session_state.log_rows = fetch_log(client)
            except Exception as e:
                st.error(f"Log table error: {e}")
                st.session_state.log_rows = []

        if refresh_clicked:
            updated = refresh_all_statuses(client, st.session_state.log_rows)
            if updated:
                st.success(f"Updated {updated} order(s) from Alpaca.")
            else:
                st.info("No status changes from Alpaca.")

    trades   = st.session_state.trades
    log_rows = st.session_state.log_rows

    if not trades:
        st.info("No trades with evaluation_status = 'PENDING_NEXT_DAY_DATA' found.")
        return

    st.caption(f"{len(trades)} trade(s) loaded for snapshot date: {trades[0].get('snapshot_date', 'N/A')}")
    st.markdown("---")

    for trade in trades:
        render_trade_card(client, trade, log_rows)
        st.markdown("<br>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
