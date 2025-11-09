"""
utils/transforms.py
Functions for transforming, merging, and calculating data.
"""

from collections import defaultdict
import constants


# --- Schedule Transforms ---


def count_games_per_team_per_week(schedule: dict) -> dict:
    """
    Count games per team per fantasy week and track opponents
    """
    # Note: This function requires get_fantasy_week,
    # so the file that calls this must also import date_utils
    from utils.date_utils import get_fantasy_week

    team_week_data = defaultdict(
        lambda: defaultdict(lambda: {"count": 0, "opponents": [], "games": []})
    )

    for game_id, game_data in schedule.items():
        year, week = get_fantasy_week(game_data["date"])
        week_key = f"{year}-W{week:02d}"

        home_team = game_data["home_abbrev"]
        away_team = game_data["away_abbrev"]

        # Track for home team
        team_week_data[home_team][week_key]["count"] += 1
        team_week_data[home_team][week_key]["opponents"].append(f"vs {away_team}")

        # Track for away team
        team_week_data[away_team][week_key]["count"] += 1
        team_week_data[away_team][week_key]["opponents"].append(f"@ {home_team}")

    return {team: dict(weeks) for team, weeks in team_week_data.items()}
