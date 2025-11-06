"""
utils/date_utils.py
All date and time-related helper functions.
"""

import pytz
from datetime import datetime, timedelta
from collections import defaultdict
import constants


def get_fantasy_week(date_str: str) -> tuple[int, int]:
    """
    Get fantasy week number (year, week_num) from a UTC date string.
    Fantasy weeks run Monday-Sunday (ISO week standard).
    """
    utc_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    fantasy_tz = pytz.timezone(constants.FANTASY_TIMEZONE)
    local_date = utc_date.astimezone(fantasy_tz)

    iso_calendar = local_date.isocalendar()
    return (iso_calendar.year, iso_calendar.week)


def get_week_dates(year: int, week: int) -> tuple[str, str]:
    """
    Get the Monday (start) and Sunday (end) dates for a given ISO week.
    Returns tuple of (monday_date, sunday_date) as strings.
    """
    jan_4 = datetime(year, 1, 4)
    week_1_monday = jan_4 - timedelta(days=jan_4.weekday())
    target_monday = week_1_monday + timedelta(weeks=week - 1)
    target_sunday = target_monday + timedelta(days=6)
    return (target_monday.strftime("%Y-%m-%d"), target_sunday.strftime("%Y-%m-%d"))


def get_schedule_by_date(schedule_by_id: dict) -> dict:
    """
    Re-index the master schedule by fantasy date string.
    """
    schedule_by_date = defaultdict(list)
    fantasy_tz = pytz.timezone(constants.FANTASY_TIMEZONE)

    for game_id, game_data in schedule_by_id.items():
        utc_date = datetime.fromisoformat(game_data["date"].replace("Z", "+00:00"))
        local_date = utc_date.astimezone(fantasy_tz)
        date_key = local_date.strftime("%Y-%m-%d")

        schedule_by_date[date_key].append(
            {"game_id": game_id, "game_date_str": date_key, **game_data}
        )
    return dict(schedule_by_date)
