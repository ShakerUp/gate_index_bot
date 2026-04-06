import time
import html
import requests
import pytz
from datetime import datetime

# =========================
# НАСТРОЙКИ
# =========================
BINANCE_FAPI_BASE = "https://fapi.binance.com"

KYIV_TZ = pytz.timezone("Europe/Kyiv")
UTC_TZ = pytz.utc

DEFAULT_FUNDING_INTERVAL_HOURS = 8
DEFAULT_INTEREST_RATE_PER_8H = 0.0001
DEFAULT_FUNDING_CAP = 0.02
DEFAULT_FUNDING_FLOOR = -0.02

STEP_SECONDS = 60
KLINE_INTERVAL = "1m"
DEFAULT_PRICE_MODE = "close"   # close по умолчанию


# =========================
# ВРЕМЯ
# =========================
def format_ts_kyiv(ts):
    utc_dt = datetime.fromtimestamp(ts, tz=UTC_TZ)
    kyiv_dt = utc_dt.astimezone(KYIV_TZ)
    return kyiv_dt.strftime("%d.%m.%Y %H:%M")


def align_down(ts, step):
    return ts - (ts % step)


def format_remaining_seconds(seconds_left):
    if seconds_left < 0:
        seconds_left = 0

    hours = seconds_left // 3600
    minutes = (seconds_left % 3600) // 60

    if hours > 0:
        return f"{hours}ч {minutes}м"
    return f"{minutes}м"


# =========================
# BINANCE API
# =========================
def binance_get(path, params=None, timeout=20):
    url = f"{BINANCE_FAPI_BASE}{path}"
    response = requests.get(url, params=params or {}, timeout=timeout)
    response.raise_for_status()
    return response.json()


def binance_get_premium_index_info(symbol):
    payload = binance_get("/fapi/v1/premiumIndex", {"symbol": symbol.upper()})
    if not isinstance(payload, dict):
        raise ValueError(f"Неожиданный ответ premiumIndex: {payload}")
    return payload


def binance_get_funding_info_map():
    payload = binance_get("/fapi/v1/fundingInfo")
    if not isinstance(payload, list):
        raise ValueError(f"Неожиданный ответ fundingInfo: {payload}")

    result = {}
    for item in payload:
        sym = str(item.get("symbol", "")).upper()
        if sym:
            result[sym] = item
    return result


def binance_get_symbol_funding_config(symbol):
    symbol = symbol.upper()
    info_map = binance_get_funding_info_map()
    adjusted = info_map.get(symbol)

    if adjusted:
        funding_interval_hours = int(
            adjusted.get("fundingIntervalHours", DEFAULT_FUNDING_INTERVAL_HOURS)
        )
        cap = float(adjusted.get("adjustedFundingRateCap", DEFAULT_FUNDING_CAP))
        floor = float(adjusted.get("adjustedFundingRateFloor", DEFAULT_FUNDING_FLOOR))
    else:
        funding_interval_hours = DEFAULT_FUNDING_INTERVAL_HOURS
        cap = DEFAULT_FUNDING_CAP
        floor = DEFAULT_FUNDING_FLOOR

    return {
        "funding_interval_hours": funding_interval_hours,
        "funding_interval_seconds": funding_interval_hours * 3600,
        "cap": cap,
        "floor": floor,
    }


def binance_get_premium_index_klines(symbol, start_ms, end_ms, interval="1m", limit=1500):
    payload = binance_get(
        "/fapi/v1/premiumIndexKlines",
        {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        },
    )

    if not isinstance(payload, list):
        raise ValueError(f"Неожиданный ответ premiumIndexKlines: {payload}")

    return payload


# =========================
# РАСЧЁТ
# =========================
def parse_kline_open_time_sec(kline):
    return int(kline[0] // 1000)


def parse_kline_open(kline):
    return float(kline[1])


def parse_kline_close(kline):
    return float(kline[4])


def normalize_price_mode(mode_str):
    mode = (mode_str or DEFAULT_PRICE_MODE).strip().lower()
    if mode not in ("open", "close", "mid"):
        raise ValueError("Режим должен быть open или close")
    return mode


def build_premium_series_from_klines(klines, from_ts, to_ts, step_seconds=60, price_mode="close"):
    """
    Собирает минутный ряд premium index по OPEN или CLOSE.
    """
    if not klines:
        raise ValueError("Binance не вернул premiumIndexKlines за этот период")

    by_ts = {parse_kline_open_time_sec(k): k for k in klines}
    expected_timestamps = list(range(from_ts, to_ts, step_seconds))

    if not expected_timestamps:
        raise ValueError("Пустой диапазон времени")

    values_percent = []
    missing_points = 0

    for ts in expected_timestamps:
        k = by_ts.get(ts)
        if k is None:
            missing_points += 1
            continue

        if price_mode == "open":
            value = parse_kline_open(k)
        elif price_mode == "close":
            value = parse_kline_close(k)
        elif price_mode == "mid":
            value = ( parse_kline_open(k) + parse_kline_close(k) ) / 2
        else:
            raise ValueError(f"Неизвестный price_mode: {price_mode}")

        values_percent.append(value * 100)

    if not values_percent:
        raise ValueError("Нет ни одной точки premium index в выбранном диапазоне")

    stats = {
        "expected_points": len(expected_timestamps),
        "used_points": len(values_percent),
        "missing_points": missing_points,
    }
    return values_percent, stats


def calculate_binance_weighted_average_premium_percent(values_percent, funding_interval_hours):
    if not values_percent:
        raise ValueError("Нет значений для расчёта")

    if funding_interval_hours <= 1:
        return sum(values_percent) / len(values_percent)

    weighted_sum = 0.0
    weights_sum = 0.0

    for i, value in enumerate(values_percent, start=1):
        weighted_sum += i * value
        weights_sum += i

    return weighted_sum / weights_sum


def calculate_projected_weighted_average_percent(values_percent, funding_interval_hours, expected_total_points):
    """
    Прогноз:
    все оставшиеся точки до конца цикла = последнему текущему значению.
    """
    if not values_percent:
        raise ValueError("Нет значений для расчёта")

    used_points = len(values_percent)
    last_value = values_percent[-1]

    if used_points >= expected_total_points:
        return calculate_binance_weighted_average_premium_percent(
            values_percent,
            funding_interval_hours,
        )

    if funding_interval_hours <= 1:
        total_sum = sum(values_percent) + (expected_total_points - used_points) * last_value
        return total_sum / expected_total_points

    weighted_sum = 0.0
    weights_sum = 0.0

    for i, value in enumerate(values_percent, start=1):
        weighted_sum += i * value
        weights_sum += i

    for i in range(used_points + 1, expected_total_points + 1):
        weighted_sum += i * last_value
        weights_sum += i

    return weighted_sum / weights_sum


def clamp(value, min_value, max_value):
    return max(min_value, min(max_value, value))


def calculate_binance_estimated_funding_rate_percent(
    avg_premium_percent,
    interest_rate_8h_percent,
    funding_interval_hours,
    cap_percent,
    floor_percent,
):
    damped = clamp(interest_rate_8h_percent - avg_premium_percent, -0.05, 0.05)
    raw_rate = (avg_premium_percent + damped) / (8 / funding_interval_hours)
    capped_rate = clamp(raw_rate, floor_percent, cap_percent)
    return raw_rate, capped_rate


def choose_status_emoji(value_percent):
    if value_percent > 0:
        return "🟢"
    if value_percent < 0:
        return "🔴"
    return "⚪"


def format_signed_percent_4(value):
    return f"{value:.4f}%"


def format_cap_floor(floor_percent, cap_percent):
    cap_sign = "+" if cap_percent >= 0 else ""
    return f"{floor_percent:.4f}% / {cap_sign}{cap_percent:.4f}%"


# =========================
# КОМАНДА ДЛЯ ОСНОВНОГО БОТА
# =========================
def run_baverage(chat_id, text, send_message):
    parts = text.strip().split()

    if len(parts) not in (2, 3):
        send_message(
            chat_id,
            "Неверный формат.\n\n"
            "<b>Примеры:</b>\n"
            "<code>/baverage BTCUSDT</code>\n"
            "<code>/baverage BTCUSDT close</code>\n"
            "<code>/baverage BTCUSDT open</code>"
        )
        return

    symbol = parts[1].upper()
    price_mode = DEFAULT_PRICE_MODE if len(parts) == 2 else parts[2].lower()

    try:
        price_mode = normalize_price_mode(price_mode)

        premium_info = binance_get_premium_index_info(symbol)
        funding_cfg = binance_get_symbol_funding_config(symbol)

        next_funding_ms = int(premium_info["nextFundingTime"])
        next_funding_ts = next_funding_ms // 1000

        interest_rate_8h = float(
            premium_info.get("interestRate", DEFAULT_INTEREST_RATE_PER_8H)
        )
        interest_rate_8h_percent = interest_rate_8h * 100

        current_funding_percent = float(
            premium_info.get("lastFundingRate", "0")
        ) * 100

        funding_interval_hours = funding_cfg["funding_interval_hours"]
        funding_interval_seconds = funding_cfg["funding_interval_seconds"]
        cap_percent = funding_cfg["cap"] * 100
        floor_percent = funding_cfg["floor"] * 100

        now_ts_raw = int(time.time())
        cycle_start_raw = next_funding_ts - funding_interval_seconds

        from_ts = align_down(cycle_start_raw, STEP_SECONDS)
        to_ts = align_down(now_ts_raw, STEP_SECONDS)

        if to_ts <= from_ts:
            raise ValueError("Текущий funding cycle ещё не начался")

        expected_total_points = funding_interval_hours * 60

        klines = binance_get_premium_index_klines(
            symbol=symbol,
            start_ms=from_ts * 1000,
            end_ms=to_ts * 1000,
            interval=KLINE_INTERVAL,
        )

        values_percent, stats = build_premium_series_from_klines(
            klines=klines,
            from_ts=from_ts,
            to_ts=to_ts,
            step_seconds=STEP_SECONDS,
            price_mode=price_mode,
        )

        current_avg_percent = calculate_binance_weighted_average_premium_percent(
            values_percent=values_percent,
            funding_interval_hours=funding_interval_hours,
        )

        projected_avg_percent = calculate_projected_weighted_average_percent(
            values_percent=values_percent,
            funding_interval_hours=funding_interval_hours,
            expected_total_points=expected_total_points,
        )

        _, projected_funding_percent = calculate_binance_estimated_funding_rate_percent(
            avg_premium_percent=projected_avg_percent,
            interest_rate_8h_percent=interest_rate_8h_percent,
            funding_interval_hours=funding_interval_hours,
            cap_percent=cap_percent,
            floor_percent=floor_percent,
        )

        seconds_left = next_funding_ts - now_ts_raw
        remaining_text = format_remaining_seconds(seconds_left)

        emoji = choose_status_emoji(projected_avg_percent)
        mode_label = price_mode.upper()

        msg = (
            f"{emoji} <b>Binance ({funding_interval_hours}ч)</b>\n"
            f"Символ: <code>{html.escape(symbol)}</code>\n"
            f"Режим: <code>{mode_label}</code>\n"
            f"Ср.откл. текущее: <code>{format_signed_percent_4(current_avg_percent)}</code>\n"
            f"Ср.откл. ожид.: <code>{format_signed_percent_4(projected_avg_percent)}</code>\n"
            f"Фандинг сейчас: <code>{format_signed_percent_4(current_funding_percent)}</code>\n"
            f"Фанд.ожид.: <code>{format_signed_percent_4(projected_funding_percent)}</code>\n"
            f"Лимиты: <code>{format_cap_floor(floor_percent, cap_percent)}</code>\n"
            f"Цикл: <code>{html.escape(format_ts_kyiv(cycle_start_raw))}</code>\n"
            f"Выплата: <code>{html.escape(format_ts_kyiv(next_funding_ts))}</code>\n"
            f"Осталось: <code>{remaining_text}</code>\n"
            f"Точек: <code>{stats['used_points']}/{expected_total_points}</code>\n"
            f"Пропущено: <code>{stats['missing_points']}</code>"
        )

        send_message(chat_id, msg)

    except requests.HTTPError as e:
        send_message(chat_id, f"Ошибка HTTP:\n<code>{html.escape(str(e))}</code>")
    except Exception as e:
        send_message(chat_id, f"Ошибка:\n<code>{html.escape(str(e))}</code>")