import os
import time
import html
import math
import requests
import pytz
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import matplotlib.dates as mdates
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

CHART_HOURS_DEFAULT = 4
CHART_MAX_HOURS = 24
CHART_INTERVAL = "1m"
CHART_STEP_SECONDS = 60

# защита от повторной обработки одного и того же update_id
PROCESSED_UPDATE_IDS = set()
PROCESSED_UPDATE_IDS_MAX = 2000

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

/chart <CONTRACT>
Строит графики за последние 4 часа

/chart <CONTRACT> <HOURS>
Строит графики за последние HOURS часов

/chartdate <CONTRACT> <DD.MM.YYYY> <HH:MM> <HOURS>
Строит графики от указанной даты и времени на HOURS часов вперёд

Примеры:
<code>/avg POLYX_USDT 17.03.2026 21:20 17.03.2026 21:41</code>
<code>/avg5 POLYX_USDT 17.03.2026 21:20 17.03.2026 21:41</code>
<code>/average POLYX_USDT</code>
<code>/average POLYX_USDT -2%</code>
<code>/chart POLYX_USDT</code>
<code>/chart POLYX_USDT 6</code>
<code>/chartdate POLYX_USDT 21.03.2026 14:00 4</code>

Время вводится по Украине.
Максимальный период для ручных average-команд: 8 часов.
Максимальный период для графиков: 24 часа.
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


def send_photo(chat_id, photo_path, caption=None):
    url = f"{TELEGRAM_API}/sendPhoto"
    with open(photo_path, "rb") as f:
        files = {"photo": f}
        data = {"chat_id": chat_id}
        if caption:
            data["caption"] = caption
            data["parse_mode"] = "HTML"
        response = requests.post(url, data=data, files=files, timeout=120)
        response.raise_for_status()
        return response.json()


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
        raise ValueError(f"Неожиданный ответ Gate premium_index: {payload}")

    unique = {}
    for item in items:
        if "t" in item:
            unique[int(item["t"])] = item

    return [unique[t] for t in sorted(unique.keys())]
  
  
def choose_y_grid_step(values_a, values_b):
    all_values = list(values_a) + list(values_b)
    if not all_values:
        return 0.5, 0.25

    vmin = min(all_values)
    vmax = max(all_values)
    yrange = abs(vmax - vmin)

    if yrange <= 0.8:
        return 0.1, 0.05
    elif yrange <= 1.5:
        return 0.2, 0.1
    elif yrange <= 3:
        return 0.25, 0.125
    elif yrange <= 6:
        return 0.5, 0.25
    elif yrange <= 12:
        return 0.75, 0.25
    else:
        return 1.0, 0.5


def choose_x_grid_locator(times):
    if not times or len(times) < 2:
        return (
            mdates.MinuteLocator(interval=5),
            mdates.MinuteLocator(interval=1),
            "%H:%M",
        )

    total_minutes = (times[-1] - times[0]).total_seconds() / 60

    if total_minutes <= 60:
        return (
            mdates.MinuteLocator(interval=5),
            mdates.MinuteLocator(interval=1),
            "%H:%M",
        )
    elif total_minutes <= 180:
        return (
            mdates.MinuteLocator(interval=10),
            mdates.MinuteLocator(interval=5),
            "%H:%M",
        )
    elif total_minutes <= 360:
        return (
            mdates.MinuteLocator(interval=15),
            mdates.MinuteLocator(interval=5),
            "%H:%M",
        )
    elif total_minutes <= 720:
        return (
            mdates.MinuteLocator(interval=30),
            mdates.MinuteLocator(interval=10),
            "%H:%M",
        )
    elif total_minutes <= 1440:
        return (
            mdates.HourLocator(interval=1),
            mdates.MinuteLocator(interval=15),
            "%d.%m %H:%M",
        )
    else:
        return (
            mdates.HourLocator(interval=2),
            mdates.HourLocator(interval=1),
            "%d.%m %H:%M",
        )


def gate_get_contract_info(contract):
    url = f"https://api.gateio.ws/api/v4/futures/{SETTLE}/contracts/{contract.upper()}"
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Неожиданный ответ Gate contract info: {payload}")

    return payload


def gate_get_candlesticks(contract, from_ts, to_ts, interval="1m", price_type="last"):
    """
    История свечей Gate.

    price_type:
    - "last"  -> обычные свечи контракта
    - "mark"  -> mark price свечи
    - "index" -> index price свечи
    """
    contract_name = contract.upper()

    if price_type == "mark":
        contract_name = f"mark_{contract_name}"
    elif price_type == "index":
        contract_name = f"index_{contract_name}"
    elif price_type != "last":
        raise ValueError(f"Неизвестный price_type: {price_type}")

    url = f"https://api.gateio.ws/api/v4/futures/{SETTLE}/candlesticks"
    params = {
        "contract": contract_name,
        "from": from_ts,
        "to": to_ts,
        "interval": interval,
    }

    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list):
        raise ValueError(f"Неожиданный ответ Gate candlesticks: {payload}")

    unique = {}
    for item in payload:
        if "t" in item:
            unique[int(item["t"])] = item

    return [unique[t] for t in sorted(unique.keys())]


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


def parse_positive_hours(hours_str):
    value = float(hours_str.replace(",", "."))
    if value <= 0:
        raise ValueError("Часы должны быть больше 0")
    if value > CHART_MAX_HOURS:
        raise ValueError(f"Максимум для графика — {CHART_MAX_HOURS} часов")
    return value


def calculate_required_rest_avg_percent(current_avg_percent, done_points, future_points, limit_percent):
    if future_points <= 0:
        return None

    total_points = done_points + future_points
    required = (limit_percent * total_points - current_avg_percent * done_points) / future_points
    return required


def extract_float_field(item, field_names):
    for name in field_names:
        if name in item and item[name] not in (None, ""):
            return float(item[name])
    raise ValueError(f"Не удалось найти поле {field_names} в {item}")


def build_comparison_series(contract, from_ts, to_ts, step_seconds, interval="1m"):
    """
    Строит:
    - premium index (%)
    - deviation = (mark - index) / index (%)
    """
    premium_items = gate_get_premium_index(contract, from_ts, to_ts, interval)
    mark_items = gate_get_candlesticks(contract, from_ts, to_ts, interval, price_type="mark")
    index_items = gate_get_candlesticks(contract, from_ts, to_ts, interval, price_type="index")

    premium_by_ts = {int(item["t"]): item for item in premium_items}
    mark_by_ts = {int(item["t"]): item for item in mark_items}
    index_by_ts = {int(item["t"]): item for item in index_items}

    expected_timestamps = list(range(from_ts, to_ts, step_seconds))
    if not expected_timestamps:
        raise ValueError("Пустой диапазон времени")

    times = []
    premium_values = []
    deviation_values = []

    missing_premium = 0
    missing_index = 0
    missing_mark = 0

    for ts in expected_timestamps:
        premium_item = premium_by_ts.get(ts)
        mark_item = mark_by_ts.get(ts)
        index_item = index_by_ts.get(ts)

        if premium_item is None:
            missing_premium += 1
            continue
        if mark_item is None:
            missing_mark += 1
            continue
        if index_item is None:
            missing_index += 1
            continue

        premium = get_c_value(premium_item) * 100
        mark_price = extract_float_field(mark_item, ["c", "close"])
        index_price = extract_float_field(index_item, ["c", "close"])

        if index_price == 0:
            continue

        deviation = ((mark_price - index_price) / index_price) * 100

        print(
            f"[GATE CHART] {format_ts_kyiv(ts)} | "
            f"premium={premium:.6f}% | "
            f"mark={mark_price:.8f} | "
            f"index={index_price:.8f} | "
            f"deviation={deviation:.6f}%"
        )

        times.append(datetime.fromtimestamp(ts, tz=UTC_TZ).astimezone(KYIV_TZ))
        premium_values.append(premium)
        deviation_values.append(deviation)

    if not times:
        raise ValueError("Нет точек для построения графика")

    stats = {
        "points": len(times),
        "missing_premium": missing_premium,
        "missing_index": missing_index,
        "missing_mark": missing_mark,
    }

    return times, premium_values, deviation_values, stats


def calc_correlation(x, y):
    n = len(x)
    if n < 2:
        return None

    mean_x = sum(x) / n
    mean_y = sum(y) / n

    num = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
    den_x = math.sqrt(sum((a - mean_x) ** 2 for a in x))
    den_y = math.sqrt(sum((b - mean_y) ** 2 for b in y))

    if den_x == 0 or den_y == 0:
        return None

    return num / (den_x * den_y)


def style_axis_percent_grid(ax):
    ax.yaxis.set_major_locator(MultipleLocator(0.5))
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.grid(True, which="major", axis="both", alpha=0.4, linewidth=0.8)
    ax.grid(True, which="minor", axis="y", alpha=0.22, linewidth=0.5)
    
    
def choose_single_y_grid_step(values):
    if not values:
        return 0.1, 0.05

    vmin = min(values)
    vmax = max(values)
    yrange = abs(vmax - vmin)

    if yrange <= 0.4:
        return 0.05, 0.025
    elif yrange <= 0.8:
        return 0.1, 0.05
    elif yrange <= 1.5:
        return 0.2, 0.1
    elif yrange <= 3:
        return 0.25, 0.125
    elif yrange <= 6:
        return 0.5, 0.25
    else:
        return 1.0, 0.5


def plot_comparison_chart(contract, times, premium_values, deviation_values, output_path):
    corr = calc_correlation(premium_values, deviation_values)

    abs_diff_values = [
        abs(p - d) for p, d in zip(premium_values, deviation_values)
    ]

    fig = plt.figure(figsize=(16, 10))

    # =========================
    # Верхний график: overlay
    # =========================
    ax1 = fig.add_subplot(2, 1, 1)

    ax1.plot(
        times,
        premium_values,
        label="Premium Index %",
        linewidth=1.6,
        color="tab:blue",
    )

    ax1.plot(
        times,
        deviation_values,
        label="Отклонение от Mark Price %",
        linewidth=1.6,
        color="tab:red",
    )

    ax1.set_title(f"{contract} — Overlay", fontsize=14)
    ax1.set_ylabel("%")
    ax1.set_xlabel("Time (Kyiv)")
    ax1.set_facecolor("#fcfcfc")

    for spine in ax1.spines.values():
        spine.set_alpha(0.35)

    major_y_1, minor_y_1 = choose_y_grid_step(premium_values, deviation_values)
    ax1.yaxis.set_major_locator(MultipleLocator(major_y_1))
    ax1.yaxis.set_minor_locator(MultipleLocator(minor_y_1))

    major_x, minor_x, xfmt = choose_x_grid_locator(times)
    ax1.xaxis.set_major_locator(major_x)
    ax1.xaxis.set_minor_locator(minor_x)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter(xfmt))

    ax1.grid(True, which="major", axis="both", alpha=0.34, linewidth=0.9)
    ax1.grid(True, which="minor", axis="both", alpha=0.14, linewidth=0.5)

    ax1.legend()

    # =========================
    # Нижний график: abs diff
    # =========================
    ax2 = fig.add_subplot(2, 1, 2)

    ax2.plot(
        times,
        abs_diff_values,
        label="|Premium Index % - Отклонение от Mark Price %|",
        linewidth=1.6,
        color="tab:green",
    )

    ax2.set_title(f"{contract} — Абсолютная разница между линиями", fontsize=13)
    ax2.set_ylabel("%")
    ax2.set_xlabel("Time (Kyiv)")
    ax2.set_facecolor("#fcfcfc")

    for spine in ax2.spines.values():
        spine.set_alpha(0.35)

    major_y_2, minor_y_2 = choose_single_y_grid_step(abs_diff_values)
    ax2.yaxis.set_major_locator(MultipleLocator(major_y_2))
    ax2.yaxis.set_minor_locator(MultipleLocator(minor_y_2))

    ax2.xaxis.set_major_locator(major_x)
    ax2.xaxis.set_minor_locator(minor_x)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter(xfmt))

    ax2.grid(True, which="major", axis="both", alpha=0.34, linewidth=0.9)
    ax2.grid(True, which="minor", axis="both", alpha=0.14, linewidth=0.5)

    ax2.legend()

    fig.autofmt_xdate()

    if corr is not None:
        fig.suptitle(f"{contract} | Correlation: {corr:.4f}", fontsize=14)
    else:
        fig.suptitle(f"{contract} | Correlation: n/a", fontsize=14)

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(output_path, dpi=170)
    plt.close(fig)


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


def calculate_diff_stats(premium_values, deviation_values):
    if not premium_values or not deviation_values:
        return None

    diffs = [p - d for p, d in zip(premium_values, deviation_values)]
    abs_diffs = [abs(x) for x in diffs]

    avg_signed_diff = sum(diffs) / len(diffs)
    avg_abs_diff = sum(abs_diffs) / len(abs_diffs)

    max_abs_diff = max(abs_diffs)
    max_idx = abs_diffs.index(max_abs_diff)

    return {
        "avg_signed_diff": avg_signed_diff,
        "avg_abs_diff": avg_abs_diff,
        "max_abs_diff": max_abs_diff,
        "max_idx": max_idx,
    }

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


def run_chart_period(chat_id, contract, from_ts, to_ts, interval="1m", step_seconds=60):
    contract = contract.upper()

    if to_ts <= from_ts:
        send_message(chat_id, "Конец периода должен быть позже начала.")
        return

    if (to_ts - from_ts) > CHART_MAX_HOURS * 3600:
        send_message(chat_id, f"Максимальный период для графика — {CHART_MAX_HOURS} часов.")
        return

    try:
        times, premium_values, deviation_values, stats = build_comparison_series(
            contract=contract,
            from_ts=from_ts,
            to_ts=to_ts,
            step_seconds=step_seconds,
            interval=interval,
        )

        file_name = f"chart_{contract}_{int(time.time())}.png"
        plot_comparison_chart(
            contract=contract,
            times=times,
            premium_values=premium_values,
            deviation_values=deviation_values,
            output_path=file_name,
        )

        corr = calc_correlation(premium_values, deviation_values)
        corr_text = "n/a" if corr is None else f"{corr:.4f}"
        
        diff_stats = calculate_diff_stats(premium_values, deviation_values)

        avg_signed_diff = diff_stats["avg_signed_diff"]
        avg_abs_diff = diff_stats["avg_abs_diff"]
        max_abs_diff = diff_stats["max_abs_diff"]
        max_idx = diff_stats["max_idx"]

        max_diff_time = times[max_idx].strftime("%d.%m.%Y %H:%M")
        max_diff_premium = premium_values[max_idx]
        max_diff_deviation = deviation_values[max_idx]

        caption = (
              f"<b>{html.escape(contract)}</b>\n"
              f"Период: {html.escape(format_ts_kyiv(from_ts))} → {html.escape(format_ts_kyiv(to_ts))} (Киев)\n"
              f"Точек: <code>{stats['points']}</code>\n"
              f"Correlation: <code>{corr_text}</code>\n"
              f"Avg abs diff: <code>{avg_abs_diff:.4f}%</code>\n"
              f"Avg signed diff: <code>{avg_signed_diff:.4f}%</code>\n"
              f"Max abs diff: <code>{max_abs_diff:.4f}%</code>\n"
              f"At: <code>{html.escape(max_diff_time)}</code>\n"
              f"Premium at max diff: <code>{max_diff_premium:.4f}%</code>\n"
              f"Deviation at max diff: <code>{max_diff_deviation:.4f}%</code>\n"
              f"Missing premium: <code>{stats['missing_premium']}</code>\n"
              f"Missing index: <code>{stats['missing_index']}</code>\n"
              f"Missing mark: <code>{stats['missing_mark']}</code>"
          )

        send_photo(chat_id, file_name, caption=caption)

        try:
            os.remove(file_name)
        except OSError:
            pass

    except requests.HTTPError as e:
        send_message(chat_id, f"Ошибка HTTP:\n<code>{html.escape(str(e))}</code>")
    except Exception as e:
        send_message(chat_id, f"Ошибка:\n<code>{html.escape(str(e))}</code>")


def run_chart(chat_id, text):
    parts = text.strip().split()

    if len(parts) not in (2, 3):
        send_message(
            chat_id,
            "Неверный формат.\n\n"
            "<b>Примеры:</b>\n"
            "<code>/chart POLYX_USDT</code>\n"
            "<code>/chart POLYX_USDT 6</code>"
        )
        return

    _, contract = parts[:2]

    try:
        hours = CHART_HOURS_DEFAULT if len(parts) == 2 else parse_positive_hours(parts[2])

        now_ts_raw = int(time.time())
        from_ts_raw = now_ts_raw - int(hours * 3600)

        from_ts = align_down(from_ts_raw, CHART_STEP_SECONDS)
        to_ts = align_down(now_ts_raw, CHART_STEP_SECONDS)

        run_chart_period(
            chat_id=chat_id,
            contract=contract,
            from_ts=from_ts,
            to_ts=to_ts,
            interval=CHART_INTERVAL,
            step_seconds=CHART_STEP_SECONDS,
        )

    except ValueError as e:
        send_message(chat_id, f"Ошибка:\n<code>{html.escape(str(e))}</code>")


def run_chartdate(chat_id, text):
    parts = text.strip().split()

    if len(parts) != 5:
        send_message(
            chat_id,
            "Неверный формат.\n\n"
            "<b>Пример:</b>\n"
            "<code>/chartdate POLYX_USDT 21.03.2026 14:00 4</code>"
        )
        return

    _, contract, start_date, start_time, hours_str = parts

    try:
        from_ts_raw = parse_kyiv_datetime(start_date, start_time)
        hours = parse_positive_hours(hours_str)
        to_ts_raw = from_ts_raw + int(hours * 3600)

        from_ts = align_down(from_ts_raw, CHART_STEP_SECONDS)
        to_ts = align_down(to_ts_raw, CHART_STEP_SECONDS)

        run_chart_period(
            chat_id=chat_id,
            contract=contract,
            from_ts=from_ts,
            to_ts=to_ts,
            interval=CHART_INTERVAL,
            step_seconds=CHART_STEP_SECONDS,
        )

    except ValueError as e:
        send_message(chat_id, f"Ошибка:\n<code>{html.escape(str(e))}</code>")


def get_command_name(text):
    if not text:
        return ""
    return text.split()[0].split("@")[0].lower()


def cleanup_processed_update_ids():
    global PROCESSED_UPDATE_IDS
    if len(PROCESSED_UPDATE_IDS) > PROCESSED_UPDATE_IDS_MAX:
        PROCESSED_UPDATE_IDS = set(list(PROCESSED_UPDATE_IDS)[-1000:])


def handle_message(message):
    chat_id = message["chat"]["id"]
    text = message.get("text", "").strip()

    if not text:
        return

    command = get_command_name(text)

    if command in ("/start", "/help"):
        send_message(chat_id, HELP_TEXT)
        return

    if command == "/chartdate":
        run_chartdate(chat_id, text)
        return

    # chartlink пока отключён
    # if command == "/chartlink":
    #     run_chartlink(chat_id, text)
    #     return

    if command == "/chart":
        run_chart(chat_id, text)
        return

    if command == "/average":
        run_average(chat_id, text)
        return

    if command == "/avg5":
        run_avg(chat_id, text, interval="5m", step_seconds=300)
        return

    if command == "/avg":
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
                update_id = update["update_id"]

                if update_id in PROCESSED_UPDATE_IDS:
                    offset = max(offset or 0, update_id + 1)
                    continue

                PROCESSED_UPDATE_IDS.add(update_id)
                cleanup_processed_update_ids()

                offset = update_id + 1

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