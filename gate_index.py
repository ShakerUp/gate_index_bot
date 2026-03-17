import os
import time
import html
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

# =========================
# НАСТРОЙКИ
# =========================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не найден в .env")


TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
GATE_BASE = "https://www.gate.com/apiw/v2/futures"
SETTLE = "usdt"

KYIV_TZ = ZoneInfo("Europe/Kyiv")
UTC_TZ = ZoneInfo("UTC")

MAX_PERIOD_HOURS = 8
MAX_PERIOD_SECONDS = MAX_PERIOD_HOURS * 60 * 60
POINT_LIMIT = 600

HELP_TEXT = """
Доступные команды:

/start
/help

/avg <CONTRACT> <DD.MM.YYYY> <HH:MM> <DD.MM.YYYY> <HH:MM>
Считает среднее по 1m

/avg5 <CONTRACT> <DD.MM.YYYY> <HH:MM> <DD.MM.YYYY> <HH:MM>
Считает среднее по 5m

Примеры:
/avg POLYX_USDT 17.03.2026 21:20 17.03.2026 21:41
/avg5 POLYX_USDT 17.03.2026 21:20 17.03.2026 21:41

Время вводится по Украине (Europe/Kyiv).
Максимальный период: 8 часов.
""".strip()


# =========================
# TELEGRAM
# =========================
def tg_request(method: str, data: dict | None = None, timeout: int = 60):
    url = f"{TELEGRAM_API}/{method}"
    response = requests.post(url, data=data or {}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def send_message(chat_id: int, text: str):
    tg_request(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        },
    )


def get_updates(offset: int | None = None, timeout: int = 50):
    params = {
        "timeout": timeout,
        "allowed_updates": '["message"]',
    }
    if offset is not None:
        params["offset"] = offset

    response = requests.get(
        f"{TELEGRAM_API}/getUpdates",
        params=params,
        timeout=timeout + 10,
    )
    response.raise_for_status()
    return response.json()


# =========================
# ВРЕМЯ
# =========================
def parse_kyiv_datetime(date_str: str, time_str: str) -> int:
    dt_local = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
    dt_local = dt_local.replace(tzinfo=KYIV_TZ)
    dt_utc = dt_local.astimezone(UTC_TZ)
    return int(dt_utc.timestamp())


def format_ts_kyiv(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=UTC_TZ).astimezone(KYIV_TZ)
    return dt.strftime("%d.%m.%Y %H:%M")


def align_down(ts: int, step: int) -> int:
    return ts - (ts % step)


# =========================
# GATE API
# =========================
def gate_get_premium_index(contract: str, from_ts: int, to_ts: int, interval: str):
    url = f"{GATE_BASE}/{SETTLE}/premium_index"
    params = {
        "contract": contract.upper(),
        "from": from_ts,
        "to": to_ts,
        "interval": interval,
        "limit": POINT_LIMIT,
    }

    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, dict) and "data" in payload:
        items = payload["data"]
    elif isinstance(payload, list):
        items = payload
    else:
        raise ValueError(f"Неожиданный ответ Gate: {payload}")

    unique = {}
    for item in items:
        if "t" in item:
            unique[int(item["t"])] = item

    return [unique[t] for t in sorted(unique.keys())]


# =========================
# РАСЧЁТ
# =========================
def get_c_value(item: dict) -> float:
    return float(item["c"])


def build_series(items: list[dict], from_ts: int, to_ts: int, step_seconds: int):
    if not items:
        raise ValueError("Gate не вернул данных за этот период")

    by_ts = {int(item["t"]): item for item in items}
    expected_timestamps = list(range(from_ts, to_ts, step_seconds))

    if not expected_timestamps:
        raise ValueError("Пустой диапазон времени")

    values = []
    missing_points = 0
    zero_points = 0

    for ts in expected_timestamps:
        item = by_ts.get(ts)
        if item is None:
            missing_points += 1
            continue

        value = get_c_value(item)
        if value == 0:
            zero_points += 1

        values.append(value)

    if not values:
        raise ValueError("Нет ни одной точки в выбранном диапазоне")

    stats = {
        "expected_points": len(expected_timestamps),
        "used_points": len(values),
        "missing_points": missing_points,
        "zero_points": zero_points,
    }
    return values, stats


def calculate_average_deviation_percent(values: list[float], absolute: bool = False) -> float:
    if absolute:
        avg = sum(abs(v) for v in values) / len(values)
    else:
        avg = sum(values) / len(values)
    return avg * 100


def run_avg(chat_id: int, text: str, interval: str, step_seconds: int):
    parts = text.strip().split()

    if len(parts) != 6:
        send_message(
            chat_id,
            "Неверный формат.\n\n"
            "<b>Пример:</b>\n"
            "<code>/avg POLYX_USDT 17.03.2026 21:20 17.03.2026 21:41</code>\n"
            "<code>/avg5 POLYX_USDT 17.03.2026 21:20 17.03.2026 21:41</code>"
        )
        return

    _, contract, start_date, start_time, end_date, end_time = parts

    try:
        from_ts_raw = parse_kyiv_datetime(start_date, start_time)
        to_ts_raw = parse_kyiv_datetime(end_date, end_time)
    except ValueError:
        send_message(
            chat_id,
            "Ошибка в дате/времени.\n\n"
            "Формат:\n"
            "<code>DD.MM.YYYY HH:MM</code>"
        )
        return

    if to_ts_raw <= from_ts_raw:
        send_message(chat_id, "Конец периода должен быть позже начала.")
        return

    if to_ts_raw - from_ts_raw > MAX_PERIOD_SECONDS:
        send_message(chat_id, f"Максимальный период — {MAX_PERIOD_HOURS} часов.")
        return

    if to_ts_raw - from_ts_raw < step_seconds:
        send_message(chat_id, f"Минимальный период для {interval} — {step_seconds // 60} мин.")
        return

    from_ts = align_down(from_ts_raw, step_seconds)
    to_ts = align_down(to_ts_raw, step_seconds)

    if to_ts <= from_ts:
        send_message(chat_id, "После округления диапазон стал пустым.")
        return

    try:
        items = gate_get_premium_index(contract, from_ts, to_ts, interval=interval)
        values, stats = build_series(items, from_ts, to_ts, step_seconds)

        avg_signed = calculate_average_deviation_percent(values, absolute=False)
        avg_abs = calculate_average_deviation_percent(values, absolute=True)

        safe_contract = html.escape(contract.upper())

        msg = (
            f"<b>Premium index average</b>\n\n"
            f"<b>Контракт:</b> <code>{safe_contract}</code>\n"
            f"<b>Период:</b> {html.escape(format_ts_kyiv(from_ts))} → {html.escape(format_ts_kyiv(to_ts))} (Киев)\n"
            f"<b>Интервал:</b> {interval}\n\n"
            f"<b>Средневзвешенное отклонение:</b> <code>{avg_signed:.6f}%</code>\n"
            f"<b>Статистика:</b>\n"
            f"Ожидаемых точек: <code>{stats['expected_points']}</code>\n"
            f"Использовано точек: <code>{stats['used_points']}</code>\n"
            f"Нулевых точек: <code>{stats['zero_points']}</code>\n"
            f"Пропущенных точек: <code>{stats['missing_points']}</code>"
        )
        send_message(chat_id, msg)

    except requests.HTTPError as e:
        send_message(chat_id, f"Ошибка HTTP:\n<code>{html.escape(str(e))}</code>")
    except Exception as e:
        send_message(chat_id, f"Ошибка:\n<code>{html.escape(str(e))}</code>")


def handle_message(message: dict):
    chat_id = message["chat"]["id"]
    text = message.get("text", "").strip()

    if not text:
        return

    if text.startswith("/start") or text.startswith("/help"):
        send_message(chat_id, HELP_TEXT)
        return

    if text.startswith("/avg5"):
        run_avg(chat_id, text, interval="5m", step_seconds=300)
        return

    if text.startswith("/avg"):
        run_avg(chat_id, text, interval="1m", step_seconds=60)
        return

    send_message(chat_id, HELP_TEXT)


# =========================
# MAIN LOOP
# =========================
def main():
    if not BOT_TOKEN or BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":
        raise RuntimeError("Укажи BOT_TOKEN в переменной окружения или прямо в коде.")

    print("Bot started...")
    offset = None

    while True:
        try:
            data = get_updates(offset=offset, timeout=50)

            if not data.get("ok"):
                time.sleep(3)
                continue

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                message = update.get("message")
                if message:
                    handle_message(message)

        except requests.RequestException as e:
            print(f"Network error: {e}")
            time.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()