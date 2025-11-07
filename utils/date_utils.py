"""
utils/date_utils.py
All date and time-related helper functions.
"""

import pytz
from datetime import datetime, timedelta
from collections import defaultdict
import constants
from utils.nhl_api_utils import get_schedule


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


def calculate_remaining_week_matchups() -> dict:
    """
    Calculates remaining games for all teams from today
    through the end of the current fantasy week (Mon-Sun).

    Returns:
        dict: { "TEAM_ABBREV": ["vs OPP (YYYY-MM-DD)", ...], ... }
    """

    # 1. Get today's date in the correct timezone
    tz = pytz.timezone(constants.FANTASY_TIMEZONE)
    today = datetime.now(tz)

    # 2. Find the end of the current week
    today_weekday_iso = today.isoweekday()  # 1=Mon, 7=Sun
    end_of_week = today + timedelta(days=7 - today_weekday_iso)

    # 3. Load the full schedule, indexed by date
    schedule_by_id = get_schedule()
    schedule_by_date = get_schedule_by_date(schedule_by_id)

    # 4. Find all games from *today* until the end of the week
    all_remaining_games = []
    current_day = today

    while current_day.date() <= end_of_week.date():
        current_day_str = current_day.strftime("%Y-%m-%d")
        games_on_day = schedule_by_date.get(current_day_str, [])
        all_remaining_games.extend(games_on_day)
        current_day += timedelta(days=1)

    # 5. Group the remaining games by team
    matchups = {}
    for game in all_remaining_games:
        home_team = game["home_abbrev"]
        away_team = game["away_abbrev"]
        game_date = game["game_date_str"]

        if home_team not in matchups:
            matchups[home_team] = []
        if away_team not in matchups:
            matchups[away_team] = []

        matchups[home_team].append(f"vs {away_team} ({game_date})")
        matchups[away_team].append(f"@ {home_team} ({game_date})")

    # 6. Sort games by date for clean output
    for team, games in matchups.items():
        sorted_games = sorted(games, key=lambda x: x.split(" ")[-1])
        matchups[team] = sorted_games

    return matchups
