"""
seed/seed_weekly_schedule.py
Controller script to analyze the full schedule and create the
'team_weekly_schedule.csv' file.
"""

import sys
import csv
from pathlib import Path
from src.core.constants import WEEKLY_SCHEDULE_CSV, FANTASY_TIMEZONE

# Import from the new utility files
from src.utils.date_utils import get_week_dates, get_fantasy_week
from src.api.nhl_api_utils import get_schedule

from collections import defaultdict


def count_games_per_team_per_week(schedule: dict) -> dict:
    """
    Count games per team per fantasy week and track opponents
    """
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


def export_to_csv(team_week_data: dict):
    """
    Export to CSV for easy viewing.
    """
    filepath = WEEKLY_SCHEDULE_CSV
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["Team", "Week", "Monday_Date", "Sunday_Date", "Game_Count", "Opponents"]
        )

        for team in sorted(team_week_data.keys()):
            weeks = sorted(team_week_data[team].items())
            for week_key, week_data in weeks:
                year, week_num = week_key.split("-W")
                monday, sunday = get_week_dates(int(year), int(week_num))
                opponents = ", ".join(week_data["opponents"])

                writer.writerow(
                    [team, week_key, monday, sunday, week_data["count"], opponents]
                )
    print(f"\n✅Successfully Exported weekly schedule to {filepath}")


if __name__ == "__main__":
    force_refresh = "--force" in sys.argv

    # 1. Get schedule
    schedule = get_schedule(force_refresh=force_refresh)
    if not schedule:
        print("Error: Could not get schedule. Exiting.")
        sys.exit(1)

    # 2. Analyze the data
    print(
        f"\nCalculating games per team per fantasy week (Mon-Sun in {FANTASY_TIMEZONE})..."
    )
    team_week_data = count_games_per_team_per_week(schedule)

    # 3. Export the result
    export_to_csv(team_week_data)
