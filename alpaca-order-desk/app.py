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
# Config
# ---------------------------------------------------------------------------
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")
GCP_PROJECT_ID  = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET      = os.getenv("BQ_DATASET", "FOR_EXTERNAL")
BQ_SOURCE_TABLE = os.getenv("BQ_SOURCE_TABLE", "final_portfolio_optimization_paper")
BQ_LOG_TABLE    = os.getenv("BQ_LOG_TABLE", "order_execution_log")
GOOGLE_CREDS    = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "./service-account.json")

ACCOUNTS = {
    "paper95k": {
        "label":       "Paper 95K",
        "budget":      95_000,
        "api_key_env": "ALPACA_95K_API_KEY",
        "secret_env":  "ALPACA_95K_SECRET_KEY",
    },
    "paper9k": {
        "label":       "Paper 9K",
        "budget":       9_000,
        "api_key_env": "ALPACA_PAPER9K_API_KEY",
        "secret_env":  "ALPACA_PAPER9K_SECRET_KEY",
    },
}

MISSING_CREDS = [] if GCP_PROJECT_ID else ["GCP_PROJECT_ID"]


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
        bigquery.SchemaField("account_id",      "STRING"),
    ]
    try:
        table = client.get_table(table_ref)
        existing_names = {f.name for f in table.schema}
        if "account_id" not in existing_names:
            table.schema = list(table.schema) + [
                bigquery.SchemaField("account_id", "STRING")
            ]
            client.update_table(table, ["schema"])
    except Exception:
        client.create_table(bigquery.Table(table_ref, schema=schema))


def fetch_trades(client) -> list[dict]:
    query = f"""
        SELECT
            option_type,
            lookupvalue        AS ticker,
            snapshot_date,
            calls_strike,
            options_price,
            Close_Price,
            Option_Expiry_Date,
            Earnings_Date,
            prediction_prob,
            calls_OpenInterest,
            Volume
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_SOURCE_TABLE}`
        WHERE CAST(evaluation_status AS STRING) = 'PENDING_NEXT_DAY_DATA'
          AND snapshot_date = (
              SELECT MAX(snapshot_date)
              FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_SOURCE_TABLE}`
              WHERE CAST(evaluation_status AS STRING) = 'PENDING_NEXT_DAY_DATA'
          )
        ORDER BY prediction_prob DESC
    """
    rows = client.query(query).result()
    return [dict(row) for row in rows]


def fetch_log(client, account_id: str) -> list[dict]:
    query = f"""
        SELECT *
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_LOG_TABLE}`
        WHERE account_id = '{account_id}'
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
    new_row = {**original_row, **updates, "id": str(uuid.uuid4())}
    write_log_row(client, new_row)


# ---------------------------------------------------------------------------
# Alpaca helpers
# ---------------------------------------------------------------------------
ALPACA_DATA_URL = "https://data.alpaca.markets"


def _selected_account() -> str:
    return st.session_state.get("selected_account", "paper95k")


def _acct_headers() -> dict:
    cfg = ACCOUNTS[_selected_account()]
    return {
        "APCA-API-KEY-ID":     os.getenv(cfg["api_key_env"], ""),
        "APCA-API-SECRET-KEY": os.getenv(cfg["secret_env"], ""),
        "Content-Type":        "application/json",
    }


def alpaca_post(endpoint: str, payload: dict) -> dict:
    url  = f"{ALPACA_BASE_URL}{endpoint}"
    resp = requests.post(url, json=payload, headers=_acct_headers(), timeout=10)
    if not resp.ok:
        raise RuntimeError(f"{resp.status_code} {resp.reason}: {resp.text} | Payload: {payload}")
    return resp.json()


def alpaca_get_order(order_id: str) -> dict:
    url  = f"{ALPACA_BASE_URL}/v2/orders/{order_id}"
    resp = requests.get(url, headers=_acct_headers(), timeout=10)
    resp.raise_for_status()
    return resp.json()


def alpaca_cancel_order(order_id: str) -> None:
    url  = f"{ALPACA_BASE_URL}/v2/orders/{order_id}"
    resp = requests.delete(url, headers=_acct_headers(), timeout=10)
    if resp.status_code not in (204, 422):
        resp.raise_for_status()


def build_option_symbol(ticker: str, expiry, option_type: str, strike: float) -> str:
    if hasattr(expiry, "strftime"):
        exp_str = expiry.strftime("%y%m%d")
    else:
        exp_str = datetime.strptime(str(expiry), "%Y-%m-%d").strftime("%y%m%d")
    cp = "C" if option_type.lower() == "call" else "P"
    strike_int = int(round(float(strike) * 1000))
    return f"{ticker.upper()}{exp_str}{cp}{strike_int:08d}"


def get_option_midpoint(symbol: str, fallback_price: float) -> float:
    try:
        url  = f"{ALPACA_DATA_URL}/v1beta1/options/quotes/latest"
        resp = requests.get(url, params={"symbols": symbol},
                            headers=_acct_headers(), timeout=10)
        resp.raise_for_status()
        quote = resp.json().get("quotes", {}).get(symbol, {})
        bid   = float(quote.get("bp", 0) or 0)
        ask   = float(quote.get("ap", 0) or 0)
        if bid > 0 and ask > 0:
            return max(round((bid + ask) / 2, 2), 0.01)
    except Exception:
        pass
    return round(float(fallback_price), 2)


LIQUIDITY_THRESHOLDS = {
    "spread_pct":    {"green": 5.0,  "yellow": 15.0},
    "open_interest": {"green": 1000, "yellow": 500},
    "volume":        {"green": 100,  "yellow": 50},
}


def get_option_liquidity(symbol: str, oi, volume) -> dict:
    result = {
        "bid": None, "ask": None, "mid": None, "spread_pct": None,
        "open_interest": int(oi or 0),
        "volume":        int(volume or 0),
        "ratings": {},
        "verdict": "UNKNOWN",
        "error": None,
    }
    try:
        url  = f"{ALPACA_DATA_URL}/v1beta1/options/snapshots"
        resp = requests.get(url, params={"symbols": symbol},
                            headers=_acct_headers(), timeout=10)
        resp.raise_for_status()
        snap = resp.json().get("snapshots", {}).get(symbol, {})

        quote = snap.get("latestQuote", {})
        bid   = float(quote.get("bp", 0) or 0)
        ask   = float(quote.get("ap", 0) or 0)
        if bid > 0 and ask > 0:
            mid                  = (bid + ask) / 2
            result["bid"]        = round(bid, 2)
            result["ask"]        = round(ask, 2)
            result["mid"]        = round(mid, 2)
            result["spread_pct"] = round((ask - bid) / mid * 100, 1)

        live_oi = snap.get("openInterest") or snap.get("open_interest")
        if live_oi is not None:
            result["open_interest"] = int(live_oi)

        live_vol = snap.get("dailyBar", {}).get("v")
        if live_vol is not None:
            result["volume"] = int(live_vol)

    except Exception as e:
        result["error"] = str(e)

    def rate(key, value):
        if value is None:
            return "grey"
        t = LIQUIDITY_THRESHOLDS[key]
        if key == "spread_pct":
            return "green" if value <= t["green"] else ("yellow" if value <= t["yellow"] else "red")
        return "green" if value >= t["green"] else ("yellow" if value >= t["yellow"] else "red")

    result["ratings"]["spread_pct"]    = rate("spread_pct",    result["spread_pct"])
    result["ratings"]["open_interest"] = rate("open_interest", result["open_interest"])
    result["ratings"]["volume"]        = rate("volume",        result["volume"])

    ratings = list(result["ratings"].values())
    if "red" in ratings:       result["verdict"] = "ILLIQUID"
    elif "yellow" in ratings:  result["verdict"] = "CAUTION"
    elif "grey" in ratings:    result["verdict"] = "UNKNOWN"
    else:                      result["verdict"] = "LIQUID"

    return result


def get_option_ask(symbol: str, fallback_price: float) -> float:
    try:
        url  = f"{ALPACA_DATA_URL}/v1beta1/options/quotes/latest"
        resp = requests.get(url, params={"symbols": symbol},
                            headers=_acct_headers(), timeout=10)
        resp.raise_for_status()
        ask = float(resp.json().get("quotes", {}).get(symbol, {}).get("ap", 0) or 0)
        if ask > 0:
            return round(ask, 2)
    except Exception:
        pass
    return round(float(fallback_price), 2)


def get_stock_quote(ticker: str) -> tuple[float, float]:
    try:
        url  = f"{ALPACA_DATA_URL}/v2/stocks/{ticker}/quotes/latest"
        resp = requests.get(url, headers=_acct_headers(), timeout=10)
        resp.raise_for_status()
        q = resp.json().get("quote", {})
        return float(q.get("bp", 0) or 0), float(q.get("ap", 0) or 0)
    except Exception:
        return 0.0, 0.0


def submit_step1(trade: dict) -> dict:
    symbol = build_option_symbol(
        trade["ticker"], trade["Option_Expiry_Date"],
        trade["option_type"], trade["calls_strike"],
    )
    limit_price = get_option_midpoint(symbol, trade.get("options_price", 1.00))
    return alpaca_post("/v2/orders", {
        "symbol":        symbol,
        "qty":           "1",
        "side":          "buy",
        "type":          "limit",
        "limit_price":   str(limit_price),
        "time_in_force": "day",
        "asset_class":   "option",
    })


def submit_step2(trade: dict) -> dict:
    is_call = trade["option_type"].lower() == "call"
    return alpaca_post("/v2/orders", {
        "symbol":        trade["ticker"].upper(),
        "qty":           "100",
        "side":          "sell" if is_call else "buy",
        "type":          "market",
        "time_in_force": "day",
    })


def _use_extended_hours(trade: dict) -> bool:
    from datetime import date, timedelta
    raw = trade.get("Earnings_Date")
    if raw is None:
        return True
    try:
        earnings = raw if isinstance(raw, date) else datetime.strptime(str(raw), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return True
    today = date.today()
    return earnings not in (today, today + timedelta(days=1))


def submit_step3(trade: dict, step1_avg_fill: float, step2_avg_fill: float) -> dict:
    is_call     = trade["option_type"].lower() == "call"
    limit_price = round(step2_avg_fill - step1_avg_fill if is_call else step2_avg_fill + step1_avg_fill, 2)
    return alpaca_post("/v2/orders", {
        "symbol":         trade["ticker"].upper(),
        "qty":            "100",
        "side":           "buy" if is_call else "sell",
        "type":           "limit",
        "limit_price":    str(limit_price),
        "time_in_force":  "gtc",
        "extended_hours": _use_extended_hours(trade),
    })


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
STATUS_BADGE = {
    "pending_approval":  "🟡 Pending Approval",
    "submitted":         "🔵 Submitted",
    "filled":            "🟢 Filled",
    "rejected":          "🔴 Rejected",
    "failed":            "❌ Failed",
    "cancelled_by_user": "🚫 Cancelled by user",
}


def get_step_status(log_rows: list[dict], ticker: str, option_type: str, step: int):
    matches = [
        r for r in log_rows
        if r["ticker"] == ticker
        and r["option_type"] == option_type
        and int(r["step"]) == step
    ]
    if not matches:
        return None

    def _sort_key(r):
        v = r.get("submitted_at")
        if isinstance(v, datetime):
            return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                pass
        return datetime.min.replace(tzinfo=timezone.utc)

    matches.sort(key=_sort_key, reverse=True)
    return matches[0]


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------
def refresh_all_statuses(client, log_rows: list[dict]) -> int:
    updated = 0
    seen, rows_to_poll = set(), []
    for row in sorted(log_rows, key=lambda r: r.get("submitted_at") or "", reverse=True):
        key = (row.get("ticker"), row.get("option_type"), row.get("step"))
        if key in seen:
            continue
        seen.add(key)
        if row.get("status") == "submitted":
            rows_to_poll.append(row)

    for row in rows_to_poll:
        order_id = row.get("alpaca_order_id")
        if not order_id:
            continue
        try:
            order         = alpaca_get_order(order_id)
            alpaca_status = order.get("status", "")
            new_status    = None
            filled_at     = None
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
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                pass
            else:
                st.warning(f"Alpaca poll error for order {order_id}: {e}")
        except Exception as e:
            st.warning(f"Alpaca poll error for order {order_id}: {e}")
    return updated


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------
def _base_log_row(trade: dict, step: int) -> dict:
    return {
        "id":            str(uuid.uuid4()),
        "snapshot_date": str(trade["snapshot_date"]),
        "ticker":        trade["ticker"],
        "option_type":   trade["option_type"],
        "step":          step,
        "submitted_at":  datetime.now(timezone.utc).isoformat(),
        "filled_at":     None,
        "error_message": None,
        "account_id":    _selected_account(),
    }


def do_approve(client, trade: dict, step: int, log_rows: list[dict]):
    base = _base_log_row(trade, step)
    try:
        if step == 1:
            resp = submit_step1(trade)
        elif step == 2:
            resp = submit_step2(trade)
        else:
            s1_row = get_step_status(log_rows, trade["ticker"], trade["option_type"], 1)
            s2_row = get_step_status(log_rows, trade["ticker"], trade["option_type"], 2)
            if not s1_row or not s2_row:
                raise RuntimeError("Cannot find Step 1 or Step 2 log entries.")
            s1_order  = alpaca_get_order(s1_row["alpaca_order_id"])
            s2_order  = alpaca_get_order(s2_row["alpaca_order_id"])
            step1_avg = float(s1_order.get("filled_avg_price") or s1_order.get("limit_price") or trade["options_price"])
            step2_avg = float(s2_order.get("filled_avg_price") or s2_order.get("limit_price") or trade["Close_Price"])
            resp = submit_step3(trade, step1_avg, step2_avg)

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


def do_cancel_step1(client, trade: dict, log_rows: list[dict]):
    ticker      = trade["ticker"]
    option_type = trade["option_type"]
    log_row     = get_step_status(log_rows, ticker, option_type, 1)
    if not log_row or not log_row.get("alpaca_order_id"):
        st.error("No Step 1 order ID found to cancel.")
        return
    try:
        alpaca_cancel_order(log_row["alpaca_order_id"])
    except Exception as e:
        st.warning(f"Alpaca cancel returned: {e} — logging as cancelled anyway.")
    row = {
        **_base_log_row(trade, 1),
        "alpaca_order_id": log_row["alpaca_order_id"],
        "status":          "cancelled_by_user",
    }
    ensure_log_table(client)
    write_log_row(client, row)
    log_rows.append(row)
    st.info(f"Step 1 cancelled for {ticker}.")


def do_reject(client, trade: dict, step: int, log_rows: list[dict]):
    row = {**_base_log_row(trade, step), "alpaca_order_id": None, "status": "rejected"}
    ensure_log_table(client)
    write_log_row(client, row)
    log_rows.append(row)
    st.info(f"Step {step} rejected.")


def do_retry_step1(client, trade: dict, log_rows: list[dict], mode: str):
    ticker      = trade["ticker"]
    option_type = trade["option_type"]
    log_row     = get_step_status(log_rows, ticker, option_type, 1)
    if not log_row or not log_row.get("alpaca_order_id"):
        st.error("No Step 1 order found to cancel.")
        return

    symbol = build_option_symbol(ticker, trade["Option_Expiry_Date"], option_type, trade["calls_strike"])
    base   = _base_log_row(trade, 1)
    try:
        alpaca_cancel_order(log_row["alpaca_order_id"])

        if mode == "ask":
            ask_price = get_option_ask(symbol, trade.get("options_price", 1.00))
            payload   = {"symbol": symbol, "qty": "1", "side": "buy", "type": "limit",
                         "limit_price": str(ask_price), "time_in_force": "day", "asset_class": "option"}
            label = f"limit at ask ${ask_price}"
        else:
            payload = {"symbol": symbol, "qty": "1", "side": "buy", "type": "market",
                       "time_in_force": "day", "asset_class": "option"}
            label = "market order"

        resp = alpaca_post("/v2/orders", payload)
        row  = {**base, "alpaca_order_id": resp.get("id", ""), "status": "submitted"}
        ensure_log_table(client)
        write_log_row(client, row)
        log_rows.append(row)
        st.success(f"Retried Step 1 as {label} — new order ID: {resp.get('id')}")
    except Exception as e:
        row = {**base, "alpaca_order_id": None, "status": "failed", "error_message": str(e)}
        ensure_log_table(client)
        try:
            write_log_row(client, row)
        except Exception:
            pass
        log_rows.append(row)
        st.error(f"Retry failed: {e}")


# ---------------------------------------------------------------------------
# Close all positions
# ---------------------------------------------------------------------------
def get_active_positions(log_rows, trades, snapshot_dates, current_tickers):
    trade_map = {(str(t.get("ticker", "")), str(t.get("option_type", ""))): t for t in trades}

    def latest_for_step(step):
        rows = [
            r for r in log_rows
            if int(r.get("step") or 0) == step
            and str(r.get("snapshot_date", "")) in snapshot_dates
            and (str(r.get("ticker", "")), str(r.get("option_type", ""))) in current_tickers
        ]
        result = {}
        for r in sorted(rows, key=lambda r: str(r.get("submitted_at") or ""), reverse=True):
            key = (str(r.get("ticker", "")), str(r.get("option_type", "")))
            if key not in result:
                result[key] = r
        return result

    s1_latest = latest_for_step(1)
    s2_latest = latest_for_step(2)
    s3_latest = latest_for_step(3)

    positions = []
    for key, s1_row in s1_latest.items():
        if s1_row.get("status") != "filled":
            continue
        s2_row    = s2_latest.get(key)
        s3_row    = s3_latest.get(key)
        has_stock = s2_row is not None and s2_row.get("status") == "filled"
        s3_open   = s3_row is not None and s3_row.get("status") == "submitted" and s3_row.get("alpaca_order_id")
        positions.append({
            "ticker":      key[0],
            "option_type": key[1],
            "trade":       trade_map.get(key, {}),
            "s1_order_id": s1_row.get("alpaca_order_id"),
            "s2_order_id": s2_row.get("alpaca_order_id") if s2_row else None,
            "s3_order_id": s3_row.get("alpaca_order_id") if s3_open else None,
            "has_stock":   has_stock,
        })
    return positions


def do_close_all_positions(client, positions, log_rows):
    results = []
    for pos in positions:
        ticker      = pos["ticker"]
        option_type = pos["option_type"]
        trade       = pos["trade"]
        result      = {"ticker": ticker, "option_type": option_type, "steps": [], "errors": []}
        base_log    = {**_base_log_row(trade, 0), "step": 0}  # step overridden below

        if pos["s3_order_id"]:
            try:
                alpaca_cancel_order(pos["s3_order_id"])
                result["steps"].append("Step 3 limit cancelled")
            except Exception as e:
                result["errors"].append(f"Cancel Step 3: {e}")

        if pos["has_stock"]:
            try:
                bid, ask = get_stock_quote(ticker)
                is_call  = option_type.lower() == "call"
                price    = ask if is_call else bid
                if price <= 0:
                    raise ValueError(f"No live quote for {ticker}")
                resp = alpaca_post("/v2/orders", {
                    "symbol": ticker.upper(), "qty": "100",
                    "side": "buy" if is_call else "sell",
                    "type": "limit", "limit_price": str(round(price, 2)),
                    "time_in_force": "day",
                })
                row = {**base_log, "id": str(uuid.uuid4()), "step": 4,
                       "alpaca_order_id": resp.get("id", ""), "status": "submitted"}
                ensure_log_table(client)
                write_log_row(client, row)
                log_rows.append(row)
                result["steps"].append(f"Stock close submitted @ ${price:.2f}")
            except Exception as e:
                result["errors"].append(f"Close stock: {e}")

        try:
            opt_sym = build_option_symbol(ticker, trade.get("Option_Expiry_Date"),
                                          option_type, trade.get("calls_strike", 0))
            mid = get_option_midpoint(opt_sym, trade.get("options_price", 1.00))
            resp = alpaca_post("/v2/orders", {
                "symbol": opt_sym, "qty": "1", "side": "sell",
                "type": "limit", "limit_price": str(mid),
                "time_in_force": "day", "asset_class": "option",
            })
            row = {**base_log, "id": str(uuid.uuid4()), "step": 5,
                   "alpaca_order_id": resp.get("id", ""), "status": "submitted"}
            ensure_log_table(client)
            write_log_row(client, row)
            log_rows.append(row)
            result["steps"].append(f"Option close submitted @ ${mid:.2f} (mid)")
        except Exception as e:
            result["errors"].append(f"Close option: {e}")

        results.append(result)
    return results


# ---------------------------------------------------------------------------
# Investment summary
# ---------------------------------------------------------------------------
def _get_alpaca_positions() -> list[dict]:
    try:
        resp = requests.get(f"{ALPACA_BASE_URL}/v2/positions",
                            headers=_acct_headers(), timeout=10)
        resp.raise_for_status()
        return resp.json() or []
    except Exception:
        return []


def _get_alpaca_open_orders() -> list[dict]:
    try:
        resp = requests.get(f"{ALPACA_BASE_URL}/v2/orders",
                            params={"status": "open", "limit": 500},
                            headers=_acct_headers(), timeout=10)
        resp.raise_for_status()
        return resp.json() or []
    except Exception:
        return []


def render_investment_summary(log_rows: list[dict], trades: list[dict]):
    account_id = _selected_account()
    budget     = ACCOUNTS[account_id]["budget"]

    current_tickers = {str(t.get("ticker", "")).upper() for t in trades}
    current_option_symbols = {
        build_option_symbol(t["ticker"], t["Option_Expiry_Date"], t["option_type"], t["calls_strike"])
        for t in trades if t.get("ticker") and t.get("Option_Expiry_Date") and t.get("calls_strike")
    }

    # ── All live data from Alpaca ──────────────────────────────────────────
    positions   = _get_alpaca_positions()
    open_orders = _get_alpaca_open_orders()

    # Match only on option symbols (OCC format) — stock tickers are too broad
    # and would catch pre-existing positions the app didn't place.
    option_positions = [
        p for p in positions
        if p.get("symbol", "").upper() in current_option_symbols
    ]
    pending_orders = [
        o for o in open_orders
        if o.get("symbol", "").upper() in current_option_symbols
        or any(
            leg.get("symbol", "").upper() in current_option_symbols
            for leg in (o.get("legs") or [])
        )
    ]

    filled_count = len(option_positions)
    deployed     = sum(abs(float(p.get("market_value") or 0)) for p in option_positions)
    remaining    = budget - deployed

    # Cancelled has no Alpaca source — log is the only record
    snapshot_dates  = {str(t.get("snapshot_date", "")) for t in trades}
    current_tk_type = {(str(t.get("ticker", "")), str(t.get("option_type", ""))) for t in trades}
    step1_rows = [
        r for r in log_rows
        if int(r.get("step") or 0) == 1
        and str(r.get("snapshot_date", "")) in snapshot_dates
        and (str(r.get("ticker", "")), str(r.get("option_type", ""))) in current_tk_type
    ]

    def _sat(r):
        v = r.get("submitted_at")
        if isinstance(v, datetime):
            return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                pass
        return datetime.min.replace(tzinfo=timezone.utc)

    seen, latest = set(), []
    for r in sorted(step1_rows, key=_sat, reverse=True):
        key = (r.get("ticker"), r.get("option_type"))
        if key not in seen:
            seen.add(key)
            latest.append(r)

    cancelled_count = sum(1 for r in latest if r.get("status") == "cancelled_by_user")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("🟢 Filled",    filled_count)
    c2.metric("🔵 Pending",   len(pending_orders))
    c3.metric("🚫 Cancelled", cancelled_count)
    c4.metric("🔴 Rejected",  0)
    c5.metric("💰 Deployed",  f"${deployed:,.0f}")
    c6.metric("🏦 Remaining", f"${remaining:,.0f}",
              delta=f"-${deployed:,.0f}" if deployed > 0 else None,
              delta_color="inverse")


# ---------------------------------------------------------------------------
# Account status bar
# ---------------------------------------------------------------------------
def render_account_status(account_id: str):
    cfg = ACCOUNTS[account_id]
    try:
        resp = requests.get(f"{ALPACA_BASE_URL}/v2/account",
                            headers=_acct_headers(), timeout=10)
        resp.raise_for_status()
        acct      = resp.json()
        equity    = float(acct.get("equity", 0) or 0)
        buying    = float(acct.get("buying_power", 0) or 0)
        acct_type = "PAPER" if "paper" in ALPACA_BASE_URL else "LIVE"
        st.markdown(
            f'<div style="background:#1a1a2e;border:1px solid #333;border-radius:6px;'
            f'padding:8px 16px;font-size:13px;margin-bottom:8px;">'
            f'<b>{cfg["label"]}</b>&nbsp;'
            f'<span style="background:#1a4a7a;padding:2px 6px;border-radius:3px;font-size:11px;">{acct_type}</span>'
            f'&nbsp;&nbsp;|&nbsp;&nbsp;<b>Equity:</b> ${equity:,.0f}'
            f'&nbsp;&nbsp;|&nbsp;&nbsp;<b>Buying Power:</b> ${buying:,.0f}'
            f'&nbsp;&nbsp;|&nbsp;&nbsp;<b>Budget:</b> ${cfg["budget"]:,}'
            f'</div>',
            unsafe_allow_html=True,
        )
    except Exception as e:
        st.warning(f"Could not fetch account info for {cfg['label']}: {e}")


# ---------------------------------------------------------------------------
# UI rendering
# ---------------------------------------------------------------------------
VERDICT_STYLE = {
    "LIQUID":   ("🟢", "normal"),
    "CAUTION":  ("🟡", "off"),
    "ILLIQUID": ("🔴", "inverse"),
    "UNKNOWN":  ("⚪", "off"),
}
RATING_ICON = {"green": "🟢", "yellow": "🟡", "red": "🔴", "grey": "⚪"}


def render_liquidity_panel(trade: dict):
    symbol    = build_option_symbol(trade["ticker"], trade["Option_Expiry_Date"],
                                    trade["option_type"], trade["calls_strike"])
    cache_key = f"liq_{symbol}"
    if cache_key not in st.session_state:
        with st.spinner("Checking liquidity…"):
            st.session_state[cache_key] = get_option_liquidity(
                symbol, trade.get("calls_OpenInterest"), trade.get("Volume"))
    liq = st.session_state[cache_key]

    verdict_icon, _ = VERDICT_STYLE.get(liq["verdict"], ("⚪", "off"))
    with st.expander(f"{verdict_icon} Liquidity: **{liq['verdict']}**  `{symbol}`",
                     expanded=(liq["verdict"] != "LIQUID")):
        if liq["error"] and liq["bid"] is None:
            st.caption(f"Could not fetch live quote: {liq['error']}")

        c1, c2, c3 = st.columns(3)
        spread_val = f"{liq['spread_pct']}%" if liq["spread_pct"] is not None else "N/A"
        spread_sub = f"Bid ${liq['bid']} / Ask ${liq['ask']}" if liq["bid"] else "No live quote"
        c1.metric(f"{RATING_ICON[liq['ratings']['spread_pct']]} Spread",
                  spread_val, spread_sub, delta_color="off")
        c2.metric(f"{RATING_ICON[liq['ratings']['open_interest']]} Open Interest",
                  f"{liq['open_interest']:,}", "≥1000 liquid", delta_color="off")
        c3.metric(f"{RATING_ICON[liq['ratings']['volume']]} Volume",
                  f"{liq['volume']:,}", "≥100 liquid", delta_color="off")

        if st.button("↻ Refresh quote", key=f"liq_refresh_{symbol}"):
            del st.session_state[cache_key]
            st.rerun()


def render_step_cell(client, trade, step, details, log_rows, unlocked):
    ticker      = trade["ticker"]
    option_type = trade["option_type"]
    log_row     = get_step_status(log_rows, ticker, option_type, step)
    status      = log_row["status"] if log_row else "pending_approval"

    if not unlocked:
        st.caption("🔒 Locked")
        return

    st.caption(details)
    st.caption(STATUS_BADGE.get(status, status))

    if status == "pending_approval":
        if step == 1:
            render_liquidity_panel(trade)
            acct_id = _selected_account()
            st.info(f"Trading on: **{ACCOUNTS[acct_id]['label']}** | Budget: **${ACCOUNTS[acct_id]['budget']:,}**")

        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅", key=f"approve_{ticker}_{option_type}_{step}",
                         help="Approve", use_container_width=True):
                do_approve(client, trade, step, log_rows)
                st.rerun()
        with b2:
            if st.button("❌", key=f"reject_{ticker}_{option_type}_{step}",
                         help="Reject", use_container_width=True):
                do_reject(client, trade, step, log_rows)
                st.rerun()

    if status == "submitted" and step == 1:
        submitted_at = log_row.get("submitted_at") if log_row else None
        if submitted_at:
            if isinstance(submitted_at, str):
                try:
                    submitted_at = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
                except ValueError:
                    submitted_at = None
            if submitted_at:
                if submitted_at.tzinfo is None:
                    submitted_at = submitted_at.replace(tzinfo=timezone.utc)
                elapsed    = datetime.now(timezone.utc) - submitted_at
                total_secs = int(elapsed.total_seconds())
                elapsed_str = (f"{total_secs // 60}m {total_secs % 60}s ago" if total_secs < 3600
                               else f"{total_secs // 3600}h {(total_secs % 3600) // 60}m ago")
                st.caption(f"⏱ {elapsed_str}")

        with st.expander("Order not filling? Retry / Cancel"):
            st.caption("All actions cancel the current order first.")
            r1, r2, r3 = st.columns(3)
            with r1:
                if st.button("Retry at Ask", key=f"retry_ask_{ticker}_{option_type}",
                             use_container_width=True, help="Limit at current ask"):
                    do_retry_step1(client, trade, log_rows, mode="ask")
                    st.rerun()
            with r2:
                if st.button("Retry at Market", key=f"retry_mkt_{ticker}_{option_type}",
                             use_container_width=True, type="secondary",
                             help="Last resort — fills at any price"):
                    do_retry_step1(client, trade, log_rows, mode="market")
                    st.rerun()
            with r3:
                if st.button("Cancel", key=f"cancel_s1_{ticker}_{option_type}",
                             use_container_width=True, type="secondary"):
                    do_cancel_step1(client, trade, log_rows)
                    st.rerun()


def render_table_header():
    cols   = st.columns([1, 1, 1, 1, 1, 4, 2, 2])
    labels = ["Type", "Ticker", "Conf%", "Strike", "Expiry", "Step 1", "Step 2", "Step 3"]
    for col, label in zip(cols, labels):
        col.markdown(f"**{label}**")
    st.divider()


def render_trade_row(client, trade, log_rows, running_cost, budget):
    ticker      = trade["ticker"]
    option_type = trade["option_type"].upper()
    conf        = float(trade.get("prediction_prob", 0)) * 100
    expiry      = trade.get("Option_Expiry_Date", "")
    strike      = trade.get("calls_strike", "")
    est_cost    = float(trade.get("options_price", 0)) * 100

    s1        = get_step_status(log_rows, ticker, trade["option_type"], 1)
    s2        = get_step_status(log_rows, ticker, trade["option_type"], 2)
    s1_status = s1["status"] if s1 else "pending_approval"
    s2_status = s2["status"] if s2 else "pending_approval"

    if option_type == "CALL":
        step1_details = f"Buy 1 CALL @ ${strike}"
        step2_details = "Sell 100 shares @ mkt"
        step3_details = "Buy 100 @ (S2 fill − S1 fill)"
    else:
        step1_details = f"Buy 1 PUT @ ${strike}"
        step2_details = "Buy 100 shares @ mkt"
        step3_details = "Sell 100 @ (S2 fill + S1 fill)"

    if s1_status == "pending_approval" and (running_cost + est_cost) > budget:
        st.warning(
            f"⚠️ **{ticker}** est. option cost ${est_cost:,.0f} — "
            f"running total ${running_cost + est_cost:,.0f} exceeds ${budget:,} budget."
        )

    badge_color = "#1a6e3c" if option_type == "CALL" else "#6e1a1a"
    type_badge  = (f'<span style="background:{badge_color};padding:2px 8px;'
                   f'border-radius:4px;font-size:12px;font-weight:bold;">{option_type}</span>')

    c_type, c_ticker, c_conf, c_strike, c_expiry, c_s1, c_s2, c_s3 = st.columns([1, 1, 1, 1, 1, 4, 2, 2])
    with c_type:   st.markdown(type_badge, unsafe_allow_html=True)
    with c_ticker: st.markdown(f"**{ticker}**")
    with c_conf:   st.markdown(f"{conf:.1f}%")
    with c_strike: st.markdown(f"${strike}")
    with c_expiry: st.markdown(str(expiry))
    with c_s1:     render_step_cell(client, trade, 1, step1_details, log_rows, unlocked=True)
    with c_s2:     render_step_cell(client, trade, 2, step2_details, log_rows, unlocked=(s1_status == "filled"))
    with c_s3:     render_step_cell(client, trade, 3, step3_details, log_rows, unlocked=(s2_status == "filled"))

    st.divider()
    return running_cost + est_cost


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="Order Desk", layout="wide")
    st.title("Order Desk")

    # ── Account switcher ────────────────────────────────────────────────────
    account_options = {v["label"]: k for k, v in ACCOUNTS.items()}
    prev_account    = st.session_state.get("selected_account", "paper95k")

    selected_label   = st.selectbox(
        "Account", options=list(account_options.keys()),
        index=list(account_options.values()).index(prev_account),
        label_visibility="collapsed",
    )
    selected_account = account_options[selected_label]

    if selected_account != prev_account:
        for key in [k for k in st.session_state if k.startswith(f"{prev_account}_")]:
            del st.session_state[key]
        st.session_state.selected_account = selected_account
        st.rerun()

    st.session_state.selected_account = selected_account

    # Liquidity legend
    st.markdown(
        '<div style="background:#1e1e1e;border:1px solid #333;border-radius:6px;'
        'padding:8px 16px;font-size:13px;margin-bottom:8px;">'
        '<b>Liquidity thresholds</b> &nbsp;|&nbsp;'
        '<b>Spread %</b>: 🟢 &lt;5% &nbsp; 🟡 5–15% &nbsp; 🔴 &gt;15%'
        '&nbsp;&nbsp;|&nbsp;&nbsp;'
        '<b>Open Interest</b>: 🟢 &gt;1000 &nbsp; 🟡 500–1000 &nbsp; 🔴 &lt;500'
        '&nbsp;&nbsp;|&nbsp;&nbsp;'
        '<b>Volume</b>: 🟢 &gt;100 &nbsp; 🟡 50–100 &nbsp; 🔴 &lt;50'
        '</div>',
        unsafe_allow_html=True,
    )

    if MISSING_CREDS:
        st.error(f"Missing required environment variables: {', '.join(MISSING_CREDS)}.")
        st.stop()

    cfg = ACCOUNTS[selected_account]
    if not os.getenv(cfg["api_key_env"]) or not os.getenv(cfg["secret_env"]):
        st.error(f"Missing credentials for **{cfg['label']}**: "
                 f"set `{cfg['api_key_env']}` and `{cfg['secret_env']}` in your .env file.")
        st.stop()

    render_account_status(selected_account)

    try:
        client = get_bq_client()
    except Exception as e:
        st.error(f"Failed to connect to BigQuery: {e}")
        st.stop()

    trades_key  = f"{selected_account}_trades"
    log_key     = f"{selected_account}_log_rows"
    confirm_key = f"{selected_account}_show_close_confirm"

    if trades_key not in st.session_state:
        st.session_state[trades_key]  = None
        st.session_state[log_key]     = None
        st.session_state[confirm_key] = False

    col_title, col_refresh, col_close = st.columns([6, 2, 2])
    with col_refresh:
        refresh_clicked = st.button("🔄 Refresh Status", use_container_width=True)
    with col_close:
        if st.button("🔴 Close All Positions", use_container_width=True):
            st.session_state[confirm_key] = True

    if st.session_state[trades_key] is None or refresh_clicked:
        with st.spinner("Loading trades from BigQuery…"):
            try:
                st.session_state[trades_key] = fetch_trades(client)
            except Exception as e:
                st.error(f"BigQuery read error: {e}")
                st.stop()

        with st.spinner("Loading order log…"):
            try:
                ensure_log_table(client)
                st.session_state[log_key] = fetch_log(client, selected_account)
            except Exception as e:
                st.error(f"Log table error: {e}")
                st.session_state[log_key] = []

        if refresh_clicked:
            updated = refresh_all_statuses(client, st.session_state[log_key])
            if updated:
                st.success(f"Updated {updated} order(s) from Alpaca.")
            else:
                st.info("No status changes from Alpaca.")

    trades   = st.session_state[trades_key]
    log_rows = st.session_state[log_key]

    if not trades:
        st.info("No trades with evaluation_status = 'PENDING_NEXT_DAY_DATA' found.")
        return

    st.caption(f"{len(trades)} trade(s) loaded for snapshot date: {trades[0].get('snapshot_date', 'N/A')}")
    render_investment_summary(log_rows, trades)
    st.markdown("---")

    if st.session_state.get(confirm_key):
        snapshot_dates  = {str(t.get("snapshot_date", "")) for t in trades}
        current_tickers = {(str(t.get("ticker", "")), str(t.get("option_type", ""))) for t in trades}
        positions       = get_active_positions(log_rows, trades, snapshot_dates, current_tickers)

        if not positions:
            st.warning("No filled positions found to close.")
            st.session_state[confirm_key] = False
        else:
            st.error("**Confirm: Close All Positions**")
            for p in positions:
                legs = "Option + Stock" if p["has_stock"] else "Option only"
                st.write(f"• **{p['ticker']}** {p['option_type'].upper()}  —  {legs}"
                         + ("  _(Step 3 limit will be cancelled first)_" if p["s3_order_id"] else ""))
            st.caption("Stock leg closes first (limit at bid/ask), then option (limit at midpoint).")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Confirm — Close All", type="primary", use_container_width=True):
                    with st.spinner("Closing positions…"):
                        results = do_close_all_positions(client, positions, log_rows)
                    for r in results:
                        if r["errors"]:
                            st.error(f"{r['ticker']} {r['option_type'].upper()}: " + " | ".join(r["errors"]))
                        else:
                            st.success(f"{r['ticker']} {r['option_type'].upper()}: " + " → ".join(r["steps"]))
                    st.session_state[confirm_key] = False
            with c2:
                if st.button("Cancel", use_container_width=True):
                    st.session_state[confirm_key] = False
                    st.rerun()

    render_table_header()
    budget       = ACCOUNTS[selected_account]["budget"]
    running_cost = 0.0
    for trade in trades:
        running_cost = render_trade_row(client, trade, log_rows, running_cost, budget)


if __name__ == "__main__":
    main()
