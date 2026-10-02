#!/usr/bin/env python3
"""
OTP Forwarder Bot (rich number files)
=====================================
Polls the CR API for new SMS/OTP records and forwards matching messages
to a Telegram group or channel.

DEFAULT TARGET: -1003878963392  (OTP channel)

NUMBER FILES — three formats supported, auto-detected:

1) PLAIN (backward compatible):
      44123456678
       # comment ignored
       +44 7700 900124

2) CSV with header (first line must contain "number"):
       number,label,target,enabled,keywords,notes
       44123456678,UK Vodafone,-1001234567890,yes,otp|verif,Primary line
       447700900123,Test SIM,,no,,Disabled

3) JSON array of objects (number, label, target, enabled, keywords, notes).

COMMANDS
--------
  /start /help              - welcome + usage
  /id                       - show current chat id
  /status                   - poller status (admin)
  /admins                   - list admin user ids (admin)
  /pause /resume            - pause/resume forwarding (admin)
  /target                   - show current target (admin)
  /settarget <id|@username> - change target (admin)
  /test                     - send test to target (admin)
  /last [n]                 - show last n forwarded messages (admin)
  /addnumber <n1,n2,...>    - add plain numbers (admin)
  /numbers [n]              - list stored numbers (admin)
  /show <number>            - show full details for one number (admin)
  /delnumber <number>       - delete one (admin)
  /clearnumbers             - wipe all (admin)
  /export                   - download current numbers store (admin)

Install:  pip install "python-telegram-bot[JobQueue]>=20"
"""

import asyncio
import csv
import hashlib
import html
import io
import json
import logging
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Optional

try:
    from telegram import Update
    from telegram.constants import ParseMode
    from telegram.ext import (
        ApplicationBuilder,
        CommandHandler,
        ContextTypes,
        MessageHandler,
        filters,
    )
except ImportError:
    sys.exit("Install dependency:  pip install 'python-telegram-bot[JobQueue]>=20'")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
API_URL   = os.environ.get("CR_API_URL", "http://147.135.212.197/crapi/had/viewstats")
API_TOKEN = os.environ.get(
    "CR_API_TOKEN",
    "QlFPQ0VBUzSDf5JUYHNWfIuEYEFpVZdcWWZwWFVog2VaUniDVVV1hA==",
)

BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN",
    "8924295551:AAGAu-CYP693Yyg1FOFwA3LpzMsNH2KBql4",
)

DEFAULT_ADMIN_IDS = "5659752731"
_raw_admins = os.environ.get("OTP_ADMIN_IDS", DEFAULT_ADMIN_IDS).strip()
ADMIN_IDS = {int(x) for x in _raw_admins.split(",") if x.strip().lstrip("-").isdigit()}

# === Default target: OTP channel ===
DEFAULT_TARGET_CHAT_ID = "-1003509638519"
TARGET_CHAT_ID = os.environ.get("OTP_TARGET_CHAT_ID", DEFAULT_TARGET_CHAT_ID).strip()

POLL_INTERVAL  = int(os.environ.get("OTP_POLL_INTERVAL", "10"))
FETCH_LIMIT    = int(os.environ.get("OTP_FETCH_LIMIT", "50"))
API_TIMEOUT    = int(os.environ.get("OTP_API_TIMEOUT", "30"))
STATE_FILE     = Path(os.environ.get("OTP_STATE_FILE", "otp_forwarder_state.json"))
NUMBERS_FILE   = Path(os.environ.get("OTP_NUMBERS_FILE", "otp_numbers.json"))
MAX_UPLOAD_MB  = int(os.environ.get("OTP_MAX_UPLOAD_MB", "5"))

_raw_kw = os.environ.get("OTP_KEYWORDS", "otp,code,pin,password,verif,验证码,رمز")
KEYWORDS = [k.strip().lower() for k in _raw_kw.split(",") if k.strip()]

OTP_CODE_RE = re.compile(r"\b\d{4,8}\b")
FORWARD_ALL = os.environ.get("OTP_FORWARD_ALL", "0") == "1"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("otp_forwarder")


# ---------------------------------------------------------------------------
# Number record model
# ---------------------------------------------------------------------------
@dataclass
class NumberEntry:
    number: str
    label: str = ""
    target: str = ""
    enabled: bool = True
    keywords: list = field(default_factory=list)
    notes: str = ""

    def to_dict(self) -> dict:
        return {
            "number": self.number,
            "label": self.label,
            "target": self.target,
            "enabled": self.enabled,
            "keywords": list(self.keywords),
            "notes": self.notes,
        }

    def summary_line(self) -> str:
        flag = "✅" if self.enabled else "⏸"
        label = f" — {html.escape(self.label)}" if self.label else ""
        tgt = f" → <code>{html.escape(self.target)}</code>" if self.target else ""
        return f"{flag} <code>{self.number}</code>{label}{tgt}"


def normalize_number(token: str) -> str:
    return re.sub(r"\D", "", token or "")


def _truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    return s in ("1", "yes", "y", "true", "on", "enabled", "enable", "✅")


def _split_keywords(raw) -> list:
    if not raw:
        return []
    items = raw if isinstance(raw, list) else re.split(r"[|;]", str(raw))
    return [str(k).strip().lower() for k in items if str(k).strip()]


# ---------------------------------------------------------------------------
# Number store
# ---------------------------------------------------------------------------
class NumbersStore:
    def __init__(self, path: Path):
        self.path = path
        self.entries: dict[str, NumberEntry] = {}
        self.load()

    def load(self):
        if not self.path.exists():
            legacy = self.path.with_suffix(".txt")
            if legacy.exists():
                try:
                    self._load_plain(legacy.read_text())
                    self.save()
                    log.info("Migrated legacy %s -> %s", legacy, self.path)
                except Exception as e:
                    log.warning("Legacy migration failed: %s", e)
            return
        try:
            raw = self.path.read_text()
            if not raw.strip():
                return
            data = json.loads(raw)
            if isinstance(data, list):
                for item in data:
                    self._ingest_dict(item)
        except Exception as e:
            log.warning("Numbers load failed: %s", e)

    def save(self):
        try:
            payload = [e.to_dict() for e in sorted(
                self.entries.values(), key=lambda x: x.number
            )]
            self.path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
        except Exception as e:
            log.warning("Numbers save failed: %s", e)

    def _ingest_dict(self, d: dict) -> Optional[NumberEntry]:
        if not isinstance(d, dict):
            return None
        num = normalize_number(d.get("number") or d.get("num") or d.get("phone") or "")
        if not (5 <= len(num) <= 15):
            return None
        entry = NumberEntry(
            number=num,
            label=str(d.get("label") or d.get("name") or "").strip(),
            target=str(d.get("target") or d.get("chat") or "").strip(),
            enabled=_truthy(d.get("enabled", True)),
            keywords=_split_keywords(d.get("keywords") or d.get("kw") or ""),
            notes=str(d.get("notes") or d.get("note") or "").strip(),
        )
        self.entries[num] = entry
        return entry

    def _load_plain(self, text: str):
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            for token in re.split(r"[\s,;|\t]+", line):
                digits = normalize_number(token)
                if 5 <= len(digits) <= 15:
                    self.entries[digits] = NumberEntry(number=digits)

    def add_simple(self, numbers) -> int:
        before = len(self.entries)
        for n in numbers:
            digits = normalize_number(n)
            if 5 <= len(digits) <= 15 and digits not in self.entries:
                self.entries[digits] = NumberEntry(number=digits)
        self.save()
        return len(self.entries) - before

    def upsert(self, entry: NumberEntry) -> str:
        result = "updated" if entry.number in self.entries else "added"
        self.entries[entry.number] = entry
        self.save()
        return result

    def remove(self, number: str) -> bool:
        digits = normalize_number(number)
        if digits in self.entries:
            del self.entries[digits]
            self.save()
            return True
        return False

    def clear(self) -> int:
        count = len(self.entries)
        self.entries.clear()
        self.save()
        return count

    def get(self, number: str) -> Optional[NumberEntry]:
        return self.entries.get(normalize_number(number))

    def match(self, record) -> Optional[NumberEntry]:
        if not self.entries:
            return None
        for field in ("number", "num", "to", "msisdn", "phone",
                      "cli", "sender", "senderid", "from", "shortcode"):
            val = rec_get(record, field)
            if not val:
                continue
            digits = normalize_number(str(val))
            if not digits:
                continue
            if digits in self.entries and self.entries[digits].enabled:
                return self.entries[digits]
            for key, entry in self.entries.items():
                if not entry.enabled:
                    continue
                if digits.endswith(key) or key.endswith(digits):
                    return entry
        return None


# ---------------------------------------------------------------------------
# File parsers
# ---------------------------------------------------------------------------
def parse_upload(text: str) -> tuple[list[NumberEntry], list[str]]:
    entries: list[NumberEntry] = []
    errors: list[str] = []
    stripped = text.lstrip()

    if stripped.startswith("[") or stripped.startswith("{"):
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                data = [data]
            for item in data:
                entry = _dict_to_entry(item)
                if entry:
                    entries.append(entry)
                else:
                    errors.append(f"Invalid JSON entry: {item!r}")
            return entries, errors
        except Exception as e:
            errors.append(f"JSON parse failed: {e}")
            return entries, errors

    lines = [l for l in text.splitlines() if l.strip() and not l.lstrip().startswith("#")]
    if lines:
        first = lines[0].lower()
        if "," in first and ("number" in first or "num" in first or "phone" in first):
            reader = csv.DictReader(io.StringIO("\n".join(lines)))
            for row_num, row in enumerate(reader, start=2):
                entry = _dict_to_entry(row)
                if entry:
                    entries.append(entry)
                else:
                    errors.append(f"Row {row_num}: no valid number ({row})")
            return entries, errors

    for line_no, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "," in line:
            parts = [p.strip() for p in line.split(",", 1)]
            digits = normalize_number(parts[0])
            if 5 <= len(digits) <= 15:
                entries.append(NumberEntry(number=digits, label=parts[1]))
                continue
        found = False
        for token in re.split(r"[\s;|\t]+", line):
            digits = normalize_number(token)
            if 5 <= len(digits) <= 15:
                entries.append(NumberEntry(number=digits))
                found = True
        if not found:
            errors.append(f"Line {line_no}: no number found ({line!r})")

    return entries, errors


def _dict_to_entry(d: dict) -> Optional[NumberEntry]:
    if not isinstance(d, dict):
        return None
    num = normalize_number(
        d.get("number") or d.get("num") or d.get("phone") or d.get("msisdn") or ""
    )
    if not (5 <= len(num) <= 15):
        return None
    return NumberEntry(
        number=num,
        label=str(d.get("label") or d.get("name") or "").strip(),
        target=str(d.get("target") or d.get("chat") or d.get("to_chat") or "").strip(),
        enabled=_truthy(d.get("enabled", True)),
        keywords=_split_keywords(d.get("keywords") or d.get("kw") or ""),
        notes=str(d.get("notes") or d.get("note") or "").strip(),
    )


# ---------------------------------------------------------------------------
# Poller state
# ---------------------------------------------------------------------------
class State:
    def __init__(self, path: Path):
        self.path = path
        self.seen = set()
        self.last_dt2 = None
        self.paused = False
        self.target = TARGET_CHAT_ID
        self.recent = deque(maxlen=20)
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text())
            self.seen = set(data.get("seen", []))[-5000:]
            self.last_dt2 = data.get("last_dt2")
            self.paused = bool(data.get("paused", False))
            saved = data.get("target")
            # Only use saved target if it matches the current default,
            # so changing DEFAULT_TARGET_CHAT_ID in code takes effect.
            if saved and saved == TARGET_CHAT_ID:
                self.target = saved
            elif saved and saved != TARGET_CHAT_ID:
                log.info("Ignoring stale saved target %s (default now %s)",
                         saved, TARGET_CHAT_ID)
                self.target = TARGET_CHAT_ID
        except Exception as e:
            log.warning("State load failed: %s", e)

    def save(self):
        try:
            self.path.write_text(json.dumps({
                "seen": list(self.seen)[-5000:],
                "last_dt2": self.last_dt2,
                "paused": self.paused,
                "target": self.target,
            }))
        except Exception as e:
            log.warning("State save failed: %s", e)


STATE   = State(STATE_FILE)
NUMBERS = NumbersStore(NUMBERS_FILE)


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------
def _api_fetch(dt1=None, dt2=None, records=FETCH_LIMIT):
    params = {"token": API_TOKEN, "records": records}
    if dt1:
        params["dt1"] = dt1
    if dt2:
        params["dt2"] = dt2
    url = API_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=API_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


async def api_fetch(**kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(_api_fetch, **kwargs))


def extract_records(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("data", "records", "result", "results", "rows", "items"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []


def rec_get(record, *names, default=""):
    if not isinstance(record, dict):
        return default
    lower = {k.lower(): v for k, v in record.items() if isinstance(k, str)}
    for n in names:
        v = lower.get(n.lower())
        if v not in (None, ""):
            return v
    return default


def build_text(record, entry: Optional[NumberEntry] = None) -> str:
    sender  = rec_get(record, "cli", "sender", "senderid", "from", "shortcode")
    number  = rec_get(record, "number", "num", "to", "msisdn", "phone")
    message = rec_get(record, "message", "msg", "text", "body", "sms", "content")
    when    = rec_get(record, "dt", "date", "time", "datetime", "received_at")

    lines = []
    if entry and entry.label:
        lines.append(f"🏷 <b>{html.escape(entry.label)}</b>")
    if sender:  lines.append(f"📨 <b>Sender:</b> {html.escape(str(sender))}")
    if number:  lines.append(f"📞 <b>Number:</b> {html.escape(str(number))}")
    if when:    lines.append(f"🕒 <b>Time:</b> {html.escape(str(when))}")
    if message:
        lines.append("")
        lines.append(f"<code>{html.escape(str(message))}</code>")
    if not lines:
        lines.append(f"<code>{html.escape(json.dumps(record, default=str))}</code>")
    return "\n".join(lines)


def looks_like_otp(record, entry: Optional[NumberEntry] = None) -> bool:
    if FORWARD_ALL:
        return True
    blob = " ".join([
        str(rec_get(record, "message", "msg", "text", "body", "sms", "content")),
        str(rec_get(record, "cli", "sender", "senderid", "from", "shortcode")),
    ]).lower()
    keywords = list(KEYWORDS)
    if entry and entry.keywords:
        keywords += entry.keywords
    if any(kw in blob for kw in keywords):
        return True
    if OTP_CODE_RE.search(blob):
        return True
    return False


def record_key(record) -> str:
    rid = rec_get(record, "id", "msgid", "message_id", "_id")
    if rid not in ("", None):
        return f"id:{rid}"
    parts = [
        str(rec_get(record, "number", "num", "to", "msisdn", "phone")),
        str(rec_get(record, "message", "msg", "text", "body", "sms", "content")),
        str(rec_get(record, "dt", "date", "time", "datetime")),
        str(rec_get(record, "cli", "sender", "senderid", "from")),
    ]
    return "h:" + hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Poller
# ---------------------------------------------------------------------------
def _time_window():
    now = datetime.now()
    dt2 = now.strftime("%Y-%m-%d %H:%M:%S")
    dt1 = STATE.last_dt2 or (now - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
    return dt1, dt2


async def poll_once(context: ContextTypes.DEFAULT_TYPE):
    if STATE.paused:
        return
    if not STATE.target and not any(e.target for e in NUMBERS.entries.values()):
        return

    dt1, dt2 = _time_window()
    try:
        payload = await api_fetch(dt1=dt1, dt2=dt2, records=FETCH_LIMIT)
    except Exception as e:
        log.warning("API fetch failed: %s", e)
        return

    records = extract_records(payload)
    if not records:
        STATE.last_dt2 = dt2
        STATE.save()
        return

    forwarded = 0
    for rec in records:
        key = record_key(rec)
        if key in STATE.seen:
            continue
        STATE.seen.add(key)

        entry = NUMBERS.match(rec) if NUMBERS.entries else None
        if NUMBERS.entries and entry is None:
            continue

        if not looks_like_otp(rec, entry):
            continue

        dest = (entry.target if entry and entry.target else STATE.target)
        if not dest:
            continue

        text = build_text(rec, entry)
        try:
            await context.bot.send_message(
                chat_id=dest,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            forwarded += 1
            STATE.recent.append((datetime.now().isoformat(timespec="seconds"), text))
        except Exception as e:
            log.exception("Forward failed: %s", e)
            msg = str(e).lower()
            if "chat not found" in msg or "kicked" in msg or "not enough rights" in msg:
                for admin in ADMIN_IDS:
                    try:
                        await context.bot.send_message(
                            admin,
                            f"⚠️ Cannot forward to <code>{html.escape(str(dest))}</code>:\n"
                            f"<code>{html.escape(str(e))}</code>",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass

    STATE.last_dt2 = dt2
    STATE.save()
    if forwarded:
        log.info("Forwarded %d new OTP(s)", forwarded)


# ---------------------------------------------------------------------------
# Admin helpers
# ---------------------------------------------------------------------------
def is_admin(update: Update) -> bool:
    if not ADMIN_IDS:
        return True
    return update.effective_user and update.effective_user.id in ADMIN_IDS


async def require_admin(update: Update) -> bool:
    if is_admin(update):
        return True
    uid = update.effective_user.id if update.effective_user else "?"
    await update.message.reply_text(
        f"⛔ Admin only.\nYour user id: <code>{uid}</code>",
        parse_mode=ParseMode.HTML,
    )
    return False


# ---------------------------------------------------------------------------
# General commands
# ---------------------------------------------------------------------------
async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Chat id: <code>{update.effective_chat.id}</code>\n"
        f"Your user id: <code>{update.effective_user.id}</code>\n"
        f"Chat type: {update.effective_chat.type}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    state = "⏸ paused" if STATE.paused else "▶️ running"
    target = STATE.target or "(not set)"
    admins = ", ".join(str(a) for a in sorted(ADMIN_IDS)) or "(none!)"
    total = len(NUMBERS.entries)
    enabled = sum(1 for e in NUMBERS.entries.values() if e.enabled)
    custom_targets = sum(1 for e in NUMBERS.entries.values() if e.target)
    number_mode = (
        f"filtering ({enabled} enabled / {total} stored, {custom_targets} custom targets)"
        if total else "off (all numbers forwarded)"
    )
    text = (
        f"<b>OTP Forwarder</b>\n"
        f"Status: {state}\n"
        f"Target: <code>{html.escape(str(target))}</code>\n"
        f"Admins: <code>{html.escape(admins)}</code>\n"
        f"Number filter: {number_mode}\n"
        f"Poll interval: {POLL_INTERVAL}s\n"
        f"Seen records: {len(STATE.seen)}\n"
        f"Last window end: <code>{STATE.last_dt2 or '-'}</code>\n"
        f"Keywords: {', '.join(KEYWORDS) or '-'}\n"
        f"Forward all: {FORWARD_ALL}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    lines = "\n".join(f"• <code>{a}</code>" for a in sorted(ADMIN_IDS)) or "(none)"
    await update.message.reply_text(f"<b>Admins</b>\n{lines}", parse_mode=ParseMode.HTML)


async def cmd_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    STATE.paused = True
    STATE.save()
    await update.message.reply_text("⏸ Paused.")


async def cmd_resume(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    STATE.paused = False
    STATE.save()
    await update.message.reply_text("▶️ Resumed.")


async def cmd_target(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    await update.message.reply_text(
        f"Current target: <code>{html.escape(str(STATE.target or '(not set)'))}</code>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_settarget(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /settarget <chat_id|@username>")
        return
    STATE.target = context.args[0].strip()
    STATE.save()
    await update.message.reply_text(
        f"✅ Target set to <code>{html.escape(STATE.target)}</code>",
        parse_mode=ParseMode.HTML,
    )


async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not STATE.target:
        await update.message.reply_text("No target configured. Use /settarget.")
        return
    try:
        await context.bot.send_message(
            chat_id=STATE.target,
            text="✅ <b>OTP Forwarder</b> test message — wiring works.",
            parse_mode=ParseMode.HTML,
        )
        await update.message.reply_text("Test sent.")
    except Exception as e:
        await update.message.reply_text(f"❌ Failed: {e}")


async def cmd_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    n = 5
    if context.args and context.args[0].isdigit():
        n = max(1, min(20, int(context.args[0])))
    items = list(STATE.recent)[-n:]
    if not items:
        await update.message.reply_text("Nothing forwarded yet.")
        return
    out = [f"<i>{html.escape(ts)}</i>\n{txt}" for ts, txt in items]
    await update.message.reply_text("\n\n———\n\n".join(out), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------------
# Number file handling
# ---------------------------------------------------------------------------
async def _ingest_document(update: Update, doc) -> None:
    size = doc.file_size or 0
    if size > MAX_UPLOAD_MB * 1024 * 1024:
        await update.message.reply_text(
            f"❌ File too large ({size / 1024 / 1024:.1f} MB). "
            f"Max is {MAX_UPLOAD_MB} MB."
        )
        return

    try:
        tg_file = await doc.get_file()
        raw = await tg_file.download_as_bytearray()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            text = raw.decode("latin-1", errors="replace")
    except Exception as e:
        await update.message.reply_text(f"❌ Could not read file: {e}")
        return

    entries, errors = parse_upload(text)
    if not entries:
        msg = "No valid entries found in the file."
        if errors:
            msg += "\n\nFirst errors:\n" + "\n".join(
                f"• {html.escape(e[:120])}" for e in errors[:5]
            )
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        return

    added = updated = 0
    for entry in entries:
        if NUMBERS.upsert(entry) == "added":
            added += 1
        else:
            updated += 1

    summary = (
        f"✅ File <b>{html.escape(doc.file_name or 'upload')}</b> processed.\n"
        f"• Parsed: {len(entries)}\n"
        f"• New: {added}\n"
        f"• Updated: {updated}\n"
        f"• Total stored: {len(NUMBERS.entries)}\n"
        f"• Enabled: {sum(1 for e in NUMBERS.entries.values() if e.enabled)}"
    )
    if errors:
        summary += f"\n\n⚠️ {len(errors)} issue(s):\n"
        summary += "\n".join(f"• {html.escape(e[:120])}" for e in errors[:5])
        if len(errors) > 5:
            summary += f"\n… and {len(errors) - 5} more"
    await update.message.reply_text(summary, parse_mode=ParseMode.HTML)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if update.message and update.message.document:
        await _ingest_document(update, update.message.document)


# ---------------------------------------------------------------------------
# Number commands
# ---------------------------------------------------------------------------
async def cmd_addnumber(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return

    reply = update.message.reply_to_message
    if reply and reply.document:
        await _ingest_document(update, reply.document)
        return

    if reply and reply.text and not context.args:
        entries, _ = parse_upload(reply.text)
        if not entries:
            await update.message.reply_text("No valid numbers found in replied message.")
            return
        added = sum(1 for e in entries if NUMBERS.upsert(e) == "added")
        await update.message.reply_text(
            f"✅ Added {added} new number(s). Total: {len(NUMBERS.entries)}"
        )
        return

    if not context.args:
        await update.message.reply_text(
            "Usage:\n"
            "• <code>/addnumber 44123456678,44123456679</code>\n"
            "• Send a .txt/.csv/.json file\n"
            "• Reply to a message/document with /addnumber",
            parse_mode=ParseMode.HTML,
        )
        return

    entries, _ = parse_upload(" ".join(context.args))
    if not entries:
        await update.message.reply_text("No valid numbers found.")
        return
    added = sum(1 for e in entries if NUMBERS.upsert(e) == "added")
    await update.message.reply_text(
        f"✅ Added {added} new number(s). Total: {len(NUMBERS.entries)}"
    )


async def cmd_numbers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not NUMBERS.entries:
        await update.message.reply_text(
            "No numbers stored. All OTPs are forwarded to the target channel."
        )
        return
    limit = 100
    if context.args and context.args[0].isdigit():
        limit = max(1, min(500, int(context.args[0])))
    sorted_entries = sorted(NUMBERS.entries.values(), key=lambda e: e.number)
    shown = sorted_entries[:limit]
    body = "\n".join(e.summary_line() for e in shown)
    more = ""
    if len(sorted_entries) > limit:
        more = f"\n\n<i>… {len(sorted_entries) - limit} more not shown</i>"
    await update.message.reply_text(
        f"<b>{len(sorted_entries)} number(s) stored</b>\n{body}{more}",
        parse_mode=ParseMode.HTML,
    )


async def cmd_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /show <number>")
        return
    entry = NUMBERS.get(context.args[0])
    if not entry:
        await update.message.reply_text("Not found.")
        return
    kws = ", ".join(entry.keywords) or "(none)"
    notes = html.escape(entry.notes) if entry.notes else "(none)"
    text = (
        f"<b>Number details</b>\n"
        f"• number: <code>{entry.number}</code>\n"
        f"• label: {html.escape(entry.label) or '(none)'}\n"
        f"• target: {html.escape(entry.target) or '(global)'}\n"
        f"• enabled: {entry.enabled}\n"
        f"• keywords: {html.escape(kws)}\n"
        f"• notes: {notes}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_delnumber(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /delnumber <number>")
        return
    if NUMBERS.remove(context.args[0]):
        await update.message.reply_text(f"✅ Removed. Total: {len(NUMBERS.entries)}")
    else:
        await update.message.reply_text("❌ Not found.")


async def cmd_clearnumbers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    count = NUMBERS.clear()
    await update.message.reply_text(
        f"🗑 Cleared {count} number(s). All OTPs will be forwarded again."
    )


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update):
        return
    if not NUMBERS.entries:
        await update.message.reply_text("Nothing to export.")
        return
    payload = [e.to_dict() for e in sorted(
        NUMBERS.entries.values(), key=lambda x: x.number
    )]
    data = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
    await update.message.reply_document(
        document=data,
        filename="otp_numbers_export.json",
        caption=f"📤 {len(payload)} number(s) exported.",
    )



async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "<b>OTP Forwarder</b>\n\n"
        f"Default target: <code>{html.escape(TARGET_CHAT_ID)}</code> (channel)\n\n"
        "<b>Commands</b>\n"
        "/id · /status · /admins · /pause · /resume\n"
        "/target · /settarget · /test · /last [n]\n\n"
        "<b>Numbers</b>\n"
        "/addnumber &lt;n1,n2,...&gt;\n"
        "/numbers [n] · /show &lt;n&gt; · /delnumber &lt;n&gt;\n"
        "/clearnumbers · /export\n\n"
        "<b>File format (auto-detected)</b>\n"
        "• Plain: one number per line\n"
        "• CSV with header:\n"
        "<code>number,label,target,enabled,keywords,notes\n"
        "44123456678,UK Vodafone,-1001234567890,yes,otp|verif,Primary</code>\n"
        "• JSON array of objects with keys: number, label, target, enabled, "
        "keywords, notes",
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not BOT_TOKEN or BOT_TOKEN == "PUT_YOUR_TELEGRAM_BOT_TOKEN_HERE":
        sys.exit("Set TELEGRAM_BOT_TOKEN.")

    log.info("Admins: %s", sorted(ADMIN_IDS) or "(open!)")
    log.info("Default target (channel): %s", TARGET_CHAT_ID)
    log.info("Stored numbers: %d (enabled: %d)",
             len(NUMBERS.entries),
             sum(1 for e in NUMBERS.entries.values() if e.enabled))

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_help))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("id", cmd_id))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("admins", cmd_admins))
    app.add_handler(CommandHandler("pause", cmd_pause))
    app.add_handler(CommandHandler("resume", cmd_resume))
    app.add_handler(CommandHandler("target", cmd_target))
    app.add_handler(CommandHandler("settarget", cmd_settarget))
    app.add_handler(CommandHandler("test", cmd_test))
    app.add_handler(CommandHandler("last", cmd_last))

    app.add_handler(CommandHandler("addnumber", cmd_addnumber))
    app.add_handler(CommandHandler("numbers", cmd_numbers))
    app.add_handler(CommandHandler("show", cmd_show))
    app.add_handler(CommandHandler("delnumber", cmd_delnumber))
    app.add_handler(CommandHandler("clearnumbers", cmd_clearnumbers))
    app.add_handler(CommandHandler("export", cmd_export))

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    if app.job_queue is None:
        sys.exit("JobQueue unavailable. Install: "
                 "pip install 'python-telegram-bot[JobQueue]>=20'")
    app.job_queue.run_repeating(poll_once, interval=POLL_INTERVAL, first=3,
                                name="otp-poller")

    log.info("Bot starting. Poll interval = %ss", POLL_INTERVAL)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()