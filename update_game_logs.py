"""
update_game_logs.py - The "Daily" Script

This script should be run once per day (e.g., every morning).
It fetches all games from YESTERDAY, processes them, and adds
them to the player_game_stats and goalie_game_stats tables
using the modular utility functions.
"""

import pytz
from datetime import datetime, timedelta

# SQLModel imports for modern querying
from sqlmodel import Session, select
from sqlalchemy import func

from database import engine
from models.database import PlayerGameStats
import constants

# Import all the necessary helpers from your new utils/ files
from utils.date_utils import get_schedule_by_date
from utils.nhl_api_utils import get_schedule, process_game, save_player_log_cache
import time


def update_yesterdays_games():
    """
    Main function. Fetches only games from yesterday.
    """
    print("=" * 60)
    print("NHL DAILY GAME LOG UPDATER")
    print("=" * 60)

    # 1. Get "yesterday" in the fantasy timezone
    tz = pytz.timezone(constants.FANTASY_TIMEZONE)
    yesterday = datetime.now(tz) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    print(f"\n1. Target Date: {yesterday_str}")

    # 2. Load schedule (from utils)
    print("\n2. Loading season schedule...")
    schedule_by_id = get_schedule()  # From nhl_api_utils
    if not schedule_by_id:
        print("Error: Could not load schedule.")
        return
    print(f"   ✓ Loaded {len(schedule_by_id)} games from schedule")

    # 3. Re-index by date (from utils)
    schedule_by_date = get_schedule_by_date(schedule_by_id)  # From date_utils

    # 4. Find games from yesterday
    print(f"\n3. Finding games for {yesterday_str}...")
    games_to_fetch = schedule_by_date.get(yesterday_str)

    if not games_to_fetch:
        print("   ✓ No games were played yesterday. All done!")
        return

    print(f"   ✓ Found {len(games_to_fetch)} games to process")

    # 5. Process games
    print("\n4. Fetching and processing games...")
    print("-" * 60)

    total_skaters = 0
    total_goalies = 0

    with Session(engine) as session:
        for i, game in enumerate(games_to_fetch, 1):
            game_id = int(game["game_id"])
            game_date = game["game_date_str"]

            print(f"\n[{i}/{len(games_to_fetch)}] Game {game_id} on {game_date}")

            # Simple check if game is already in DB
            statement = select(func.count(PlayerGameStats.game_id)).where(
                PlayerGameStats.game_id == game_id
            )
            existing_count = session.exec(statement).one()
            if existing_count > 0:
                print("   ✓ Game already in database. Skipping.")
                continue

            # === CALL THE REUSABLE FUNCTION ===
            skaters, goalies = process_game(game, session)  # From nhl_api_utils

            total_skaters += skaters
            total_goalies += goalies

    print("\n" + "=" * 60)
    print("SUMMARY")
    print(f"Games processed: {len(games_to_fetch)}")
    print(f"Total skaters saved: {total_skaters}")
    print(f"Total goalies saved: {total_goalies}")
    save_player_log_cache()
    print(f"\n✓ Daily update complete!")


if __name__ == "__main__":
    # NOTE: We DO NOT run init_db() here.
    # This script assumes the database has already been created
    # by seed/seed_past_game_data.py
    start_time = time.time()
    update_yesterdays_games()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
