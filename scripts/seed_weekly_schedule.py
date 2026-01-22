"""
seed_weekly_schedule.py

Controller script to analyze the full schedule and save the
weekly breakdown to the `team_schedule` table in the database.
"""

import sys
import asyncio
from collections import defaultdict
from sqlmodel import Session
from src.database.database import engine
from src.database.models import TeamSchedule
from src.database.utils import clear_table, bulk_insert_data
from src.core.constants import FANTASY_TIMEZONE

# Import from the new utility files
from src.utils.date_utils import get_week_dates, get_fantasy_week
from src.api.nhl_api_utils import get_schedule


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


def save_to_database(team_week_data: dict):
    """
    Saves the analyzed weekly schedule data to the database.
    This function will clear the table and do a fresh insert.
    """
    print("\nSaving weekly schedule to database...")
    schedule_objects = []

    for team in sorted(team_week_data.keys()):
        weeks = sorted(team_week_data[team].items())
        for week_key, week_data in weeks:
            year, week_num = week_key.split("-W")
            monday, sunday = get_week_dates(int(year), int(week_num))
            opponents = ", ".join(week_data["opponents"])

            # Create the SQLModel object
            team_schedule_entry = TeamSchedule(
                team=team,
                week=week_key,
                monday_date=monday,
                sunday_date=sunday,
                game_count=week_data["count"],
                opponents=opponents,
            )
            schedule_objects.append(team_schedule_entry)

    with Session(engine) as session:
        # Clear the table for a fresh seed
        clear_table(session, TeamSchedule)

        # Bulk add all new objects
        # bulk_insert_data handles the session.add_all() and session.commit()
        bulk_insert_data(session, schedule_objects)

    print(
        f"✅Successfully saved {len(schedule_objects)} weekly schedule entries to the database."
    )


async def main():
    """
    Async main function to orchestrate fetching, analyzing,
    and saving the weekly schedule.
    """
    force_refresh = "--force" in sys.argv

    # 1. Get schedule (async)
    print("Fetching NHL schedule...")
    schedule = await get_schedule(force_refresh=force_refresh)
    if not schedule:
        print("Error: Could not get schedule. Exiting.")
        sys.exit(1)

    # 2. Analyze the data
    print(
        f"\nCalculating games per team per fantasy week (Mon-Sun in {FANTASY_TIMEZONE})..."
    )
    team_week_data = count_games_per_team_per_week(schedule)

    # 3. Save the result to the DB (replaces export_to_csv)
    save_to_database(team_week_data)


if __name__ == "__main__":
    asyncio.run(main())
