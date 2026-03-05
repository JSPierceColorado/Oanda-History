import os
import json
import time
import base64
import logging
from dataclasses import dataclass
from typing import List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


# ---------- Config ----------

@dataclass
class Config:
    sheet_id: str
    screener_tab: str = "Screener"
    history_tab: str = "history"
    poll_minutes: int = 15
    max_snapshots: int = 5000
    log_level: str = "INFO"
    dry_run: bool = False

    # FX market hours heuristic: open Sun 5pm NY -> Fri 5pm NY
    market_tz: str = "America/New_York"


def load_config() -> Config:
    return Config(
        sheet_id=os.environ["SHEET_ID"],
        screener_tab=os.getenv("SCREENER_TAB", "Screener"),
        history_tab=os.getenv("HISTORY_TAB", "history"),
        poll_minutes=int(os.getenv("POLL_MINUTES", "15")),
        max_snapshots=int(os.getenv("MAX_SNAPSHOTS", "5000")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        dry_run=os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes", "y"),
        market_tz=os.getenv("MARKET_TZ", "America/New_York"),
    )


# ---------- Logging ----------

def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


# ---------- Google Auth ----------

def load_service_account_info() -> dict:
    """
    Expects GOOGLE_SERVICE_ACCOUNT_JSON to be either:
      1) raw JSON string, or
      2) base64-encoded JSON string
    """
    raw = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        decoded = base64.b64decode(raw).decode("utf-8")
        return json.loads(decoded)


def make_gspread_client() -> gspread.Client:
    info = load_service_account_info()
    return gspread.service_account_from_dict(info)


# ---------- Market hours (FX) ----------

def fx_market_is_open(now_utc: datetime, market_tz: str) -> bool:
    """
    Simple/standard retail FX convention:
      - Opens Sunday 17:00 New York
      - Closes Friday 17:00 New York
    """
    ny = ZoneInfo(market_tz)
    now_ny = now_utc.astimezone(ny)

    wd = now_ny.weekday()  # Mon=0 ... Sun=6
    # Saturday closed
    if wd == 5:
        return False
    # Sunday: open at 17:00
    if wd == 6:
        return now_ny.hour >= 17
    # Friday: close at 17:00
    if wd == 4:
        return now_ny.hour < 17
    # Mon-Thu open
    return True


def seconds_until_fx_open(now_utc: datetime, market_tz: str) -> int:
    ny = ZoneInfo(market_tz)
    now_ny = now_utc.astimezone(ny)

    # If it's Saturday, next open is Sunday 17:00 NY
    if now_ny.weekday() == 5:
        target = (now_ny + timedelta(days=1)).replace(hour=17, minute=0, second=0, microsecond=0)
    # If it's Sunday before 17:00, open today at 17:00
    elif now_ny.weekday() == 6 and now_ny.hour < 17:
        target = now_ny.replace(hour=17, minute=0, second=0, microsecond=0)
    # If it's Friday after 17:00, open Sunday 17:00
    elif now_ny.weekday() == 4 and now_ny.hour >= 17:
        target = (now_ny + timedelta(days=2)).replace(hour=17, minute=0, second=0, microsecond=0)
    # Otherwise, already open (or edge-case); retry soon
    else:
        return 60

    delta = target - now_ny
    return max(60, int(delta.total_seconds()))


# ---------- Sheets ops ----------

def trim_empty_trailing_rows(rows: List[List[str]]) -> List[List[str]]:
    # remove fully-empty rows
    cleaned = []
    for r in rows:
        if any(str(cell).strip() != "" for cell in r):
            cleaned.append(r)
    return cleaned


def normalize_block(rows: List[List[str]]) -> List[List[str]]:
    """
    Pad each row to the same width so insert_rows is consistent.
    """
    if not rows:
        return rows
    width = max(len(r) for r in rows)
    return [r + [""] * (width - len(r)) for r in rows]


def block_has_any_data(rows: List[List[str]]) -> bool:
    return any(any(str(c).strip() for c in r) for r in rows)


@retry(
    retry=retry_if_exception_type(Exception),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
)
def snapshot_once(cfg: Config, gc: gspread.Client) -> None:
    sh = gc.open_by_key(cfg.sheet_id)
    ws_screener = sh.worksheet(cfg.screener_tab)
    ws_history = sh.worksheet(cfg.history_tab)

    screener_values = ws_screener.get_all_values()
    screener_values = trim_empty_trailing_rows(screener_values)

    if not screener_values:
        logging.warning("Screener tab returned no non-empty rows; skipping this cycle.")
        return

    screener_values = normalize_block(screener_values)
    width = len(screener_values[0])

    # --- CHANGE: drop header row from snapshots ---
    header = screener_values[0]  # kept for clarity (not written)
    data_rows = screener_values[1:]

    if not data_rows or not block_has_any_data(data_rows):
        logging.warning("Screener has only header/no data rows; skipping this cycle.")
        return
    # ---------------------------------------------

    blank_row = [""] * width
    block = data_rows + [blank_row]  # separated by one empty row

    snapshot_height = len(block)
    max_rows = cfg.max_snapshots * snapshot_height

    logging.info(
        "Snapshotting Screener (no header) -> history | rows=%d cols=%d | snapshot_height=%d | cap_rows=%d",
        len(data_rows), width, snapshot_height, max_rows
    )

    if cfg.dry_run:
        logging.info("DRY_RUN=true (no writes).")
        return

    # Insert newest snapshot at the top (row 1)
    ws_history.insert_rows(block, row=1, value_input_option="RAW")

    # Enforce cap by deleting excess rows from bottom
    current = ws_history.get_all_values()
    current_len = len(current)
    if current_len > max_rows:
        start_delete = max_rows + 1
        end_delete = current_len
        logging.info(
            "Trimming history rows %d..%d to maintain the last %d snapshots.",
            start_delete, end_delete, cfg.max_snapshots
        )
        ws_history.delete_rows(start_delete, end_delete)


# ---------- Main loop ----------

def sleep_to_next_interval(minutes: int) -> None:
    # aligns to real clock boundaries (e.g., :00, :15, :30, :45)
    now = datetime.utcnow()
    total = now.minute * 60 + now.second
    interval = minutes * 60
    wait = interval - (total % interval)
    if wait < 5:
        wait += interval
    time.sleep(wait)


def main():
    cfg = load_config()
    setup_logging(cfg.log_level)

    logging.info("Starting FX Screener History Bot")
    logging.info(
        "Sheet=%s | Screener=%s | History=%s | Every=%d min | MaxSnapshots=%d | DRY_RUN=%s",
        cfg.sheet_id, cfg.screener_tab, cfg.history_tab, cfg.poll_minutes, cfg.max_snapshots, cfg.dry_run
    )

    gc = make_gspread_client()

    # align to the next interval boundary on startup
    sleep_to_next_interval(cfg.poll_minutes)

    while True:
        now_utc = datetime.now(tz=ZoneInfo("UTC"))

        if not fx_market_is_open(now_utc, cfg.market_tz):
            secs = seconds_until_fx_open(now_utc, cfg.market_tz)
            logging.info("FX market closed (heuristic). Sleeping %d seconds...", secs)
            time.sleep(secs)
            continue

        try:
            snapshot_once(cfg, gc)
        except Exception as e:
            logging.exception("Snapshot cycle failed: %s", e)

        sleep_to_next_interval(cfg.poll_minutes)


if __name__ == "__main__":
    main()
