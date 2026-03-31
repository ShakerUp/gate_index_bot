import os
import time
import html
import requests
import pytz
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# =========================
# НАСТРОЙКИ
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не найден в .env")

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
GATE_BASE = "https://www.gate.com/apiw/v2/futures"
SETTLE = "usdt"

KYIV_TZ = pytz.timezone("Europe/Kyiv")
UTC_TZ = pytz.utc

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

/average <CONTRACT>
Считает среднее отклонение от начала текущего цикла фандинга до сейчас

/average <CONTRACT> <LIMIT>
Считает среднее отклонение от начала текущего цикла фандинга до сейчас
и показывает, какое минимальное среднее отклонение нужно на остаток цикла,
чтобы итоговое среднее по циклу было выше лимита

Примеры:
<code>/avg POLYX_USDT 17.03.2026 21:20 17.03.2026 21:41</code>
<code>/avg5 POLYX_USDT 17.03.2026 21:20 17.03.2026 21:41</code>
<code>/average POLYX_USDT</code>
<code>/average POLYX_USDT -2%</code>

Время вводится по Украине.
Максимальный период для ручных команд: 8 часов.
""".strip()


# =========================
# TELEGRAM
# =========================
def tg_request(method, data=None, timeout=60):
    url = f"{TELEGRAM_API}/{method}"
    response = requests.post(url, data=data or {}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def send_message(chat_id, text):
    tg_request(
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        },
    )


def get_updates(offset=None, timeout=50):
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
def parse_kyiv_datetime(date_str, time_str):
    naive_dt = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
    local_dt = KYIV_TZ.localize(naive_dt)
    utc_dt = local_dt.astimezone(UTC_TZ)
    return int(utc_dt.timestamp())


def format_ts_kyiv(ts):
    utc_dt = datetime.fromtimestamp(ts, tz=UTC_TZ)
    kyiv_dt = utc_dt.astimezone(KYIV_TZ)
    return kyiv_dt.strftime("%d.%m.%Y %H:%M")


def align_down(ts, step):
    return ts - (ts % step)


# =========================
# GATE API
# =========================
def gate_get_premium_index(contract, from_ts, to_ts, interval):
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


def gate_get_contract_info(contract):
    url = f"https://api.gateio.ws/api/v4/futures/{SETTLE}/contracts/{contract.upper()}"
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Неожиданный ответ Gate contract info: {payload}")

    return payload


# =========================
# РАСЧЁТ
# =========================
def get_c_value(item):
    if "c" not in item:
        raise ValueError(f"В записи нет поля 'c': {item}")
    return float(item["c"])


def build_series(items, from_ts, to_ts, step_seconds):
    """
    Строит ряд по точкам внутри диапазона [from_ts, to_ts).

    Логика:
    - если API вернул c=0, это учитывается как реальный 0
    - если точки вообще нет, это пропуск
    """
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


def calculate_average_deviation_percent(values, absolute=False):
    if not values:
        raise ValueError("Нет значений для расчёта")

    if absolute:
        avg = sum(abs(v) for v in values) / len(values)
    else:
        avg = sum(values) / len(values)

    return avg * 100


def choose_interval_for_period(period_seconds):
    """
    Чтобы не упереться в limit=600:
    - если период <= 600 минут -> 1m
    - иначе -> 5m
    """
    if period_seconds <= 600 * 60:
        return "1m", 60
    return "5m", 300


def parse_percent_input(value_str):
    s = value_str.strip().replace(",", ".")
    if s.endswith("%"):
        s = s[:-1].strip()

    if not s:
        raise ValueError("Пустой лимит")

    return float(s)


def calculate_required_rest_avg_percent(current_avg_percent, done_points, future_points, limit_percent):
    """
    Считает, какое минимальное среднее отклонение (%) нужно на оставшихся точках,
    чтобы итоговое среднее по всему циклу было > limit_percent.
    """
    if future_points <= 0:
        return None

    total_points = done_points + future_points
    required = (limit_percent * total_points - current_avg_percent * done_points) / future_points
    return required


# =========================
# ОСНОВНАЯ ЛОГИКА КОМАНД
# =========================
def run_avg(chat_id, text, interval, step_seconds):
    parts = text.strip().split()

    if len(parts) != 6:
        send_message(
            chat_id,
            "Неверный формат.\n\n"
            "<b>Примеры:</b>\n"
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
            "Используй формат:\n"
            "<code>DD.MM.YYYY HH:MM</code>"
        )
        return

    if to_ts_raw <= from_ts_raw:
        send_message(chat_id, "Конец периода должен быть позже начала.")
        return

    if (to_ts_raw - from_ts_raw) > MAX_PERIOD_SECONDS:
        send_message(chat_id, f"Максимальный период — {MAX_PERIOD_HOURS} часов.")
        return

    if (to_ts_raw - from_ts_raw) < step_seconds:
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


def run_average(chat_id, text):
    parts = text.strip().split()

    if len(parts) not in (2, 3):
        send_message(
            chat_id,
            "Неверный формат.\n\n"
            "<b>Примеры:</b>\n"
            "<code>/average POLYX_USDT</code>\n"
            "<code>/average POLYX_USDT -2%</code>"
        )
        return

    _, contract = parts[:2]
    limit_percent = None

    if len(parts) == 3:
        try:
            limit_percent = parse_percent_input(parts[2])
        except ValueError:
            send_message(
                chat_id,
                "Ошибка в лимите.\n\n"
                "Примеры:\n"
                "<code>/average POLYX_USDT -2%</code>\n"
                "<code>/average POLYX_USDT 1.5%</code>"
            )
            return

    safe_contract = html.escape(contract.upper())

    try:
        contract_info = gate_get_contract_info(contract)

        funding_interval = int(contract_info.get("funding_interval", 0))
        funding_next_apply = int(float(contract_info.get("funding_next_apply", 0)))

        if funding_interval <= 0 or funding_next_apply <= 0:
            raise ValueError("У контракта нет данных по funding cycle")

        now_ts_raw = int(time.time())

        cycle_start_raw = funding_next_apply - funding_interval
        from_ts_raw = cycle_start_raw
        to_ts_raw = now_ts_raw

        if to_ts_raw <= from_ts_raw:
            raise ValueError("Текущий цикл фандинга ещё не начался")

        interval, step_seconds = choose_interval_for_period(to_ts_raw - from_ts_raw)

        from_ts = align_down(from_ts_raw, step_seconds)
        to_ts = align_down(to_ts_raw, step_seconds)
        cycle_end_ts = align_down(funding_next_apply, step_seconds)

        if to_ts <= from_ts:
            raise ValueError("После округления диапазон стал пустым")

        items = gate_get_premium_index(contract, from_ts, to_ts, interval=interval)
        values, stats = build_series(items, from_ts, to_ts, step_seconds)

        avg_signed = calculate_average_deviation_percent(values, absolute=False)

        msg = (
            f"<b>Premium index average (funding cycle)</b>\n\n"
            f"<b>Контракт:</b> <code>{safe_contract}</code>\n"
            f"<b>Начало текущего цикла:</b> {html.escape(format_ts_kyiv(cycle_start_raw))} (Киев)\n"
            f"<b>Следующий funding:</b> {html.escape(format_ts_kyiv(funding_next_apply))} (Киев)\n"
            f"<b>Период расчёта:</b> {html.escape(format_ts_kyiv(from_ts))} → {html.escape(format_ts_kyiv(to_ts))} (Киев)\n"
            f"<b>Интервал:</b> {interval}\n"
            f"<b>Длина funding cycle:</b> <code>{funding_interval // 3600} ч</code>\n\n"
            f"<b>Средневзвешенное отклонение сейчас:</b> <code>{avg_signed:.6f}%</code>\n"
            f"<b>Статистика:</b>\n"
            f"Ожидаемых точек: <code>{stats['expected_points']}</code>\n"
            f"Использовано точек: <code>{stats['used_points']}</code>\n"
            f"Нулевых точек: <code>{stats['zero_points']}</code>\n"
            f"Пропущенных точек: <code>{stats['missing_points']}</code>"
        )

        if limit_percent is not None:
            done_points = stats["used_points"]

            future_expected_points = 0
            if cycle_end_ts > to_ts:
                future_expected_points = len(range(to_ts, cycle_end_ts, step_seconds))

            required_rest_avg = calculate_required_rest_avg_percent(
                current_avg_percent=avg_signed,
                done_points=done_points,
                future_points=future_expected_points,
                limit_percent=limit_percent,
            )

            msg += (
                f"\n\n<b>Лимит:</b> <code>{limit_percent:.6f}%</code>\n"
                f"<b>Оставшихся точек до funding:</b> <code>{future_expected_points}</code>"
            )

            if future_expected_points <= 0:
                if avg_signed > limit_percent:
                    msg += (
                        f"\n<b>Итог:</b> цикл уже практически завершён, "
                        f"текущее среднее <code>{avg_signed:.6f}%</code> уже <b>выше</b> лимита."
                    )
                else:
                    msg += (
                        f"\n<b>Итог:</b> цикл уже практически завершён, "
                        f"текущее среднее <code>{avg_signed:.6f}%</code> уже <b>не выше</b> лимита."
                    )
            else:
                msg += (
                    f"\n<b>Минимально нужное среднее отклонение на остаток цикла:</b> "
                    f"<code>{required_rest_avg:.6f}%</code>"
                )
        send_message(chat_id, msg)

    except requests.HTTPError as e:
        send_message(chat_id, f"Ошибка HTTP:\n<code>{html.escape(str(e))}</code>")
    except Exception as e:
        send_message(chat_id, f"Ошибка:\n<code>{html.escape(str(e))}</code>")


def handle_message(message):
    chat_id = message["chat"]["id"]
    text = message.get("text", "").strip()

    if not text:
        return

    if text.startswith("/start") or text.startswith("/help"):
        send_message(chat_id, HELP_TEXT)
        return

    if text.startswith("/average"):
        run_average(chat_id, text)
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