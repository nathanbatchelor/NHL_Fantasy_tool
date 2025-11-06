"""
seed/seed_past_game_data.py
Controller script to seed the database with all past games.
"""

import pytz
from datetime import datetime
from sqlmodel import Session, select

from database import engine, init_db

from models.database import PlayerGameStats
import constants

# Import from the new utility files
from utils.date_utils import get_schedule_by_date
from utils.nhl_api_utils import get_schedule, process_game, save_player_log_cache

import time


def fetch_and_save_stats():
    """Main function"""
    print("=" * 60)
    print("NHL PAST GAME DATA SEEDER")
    print("=" * 60)

    # 1. Load schedule
    print("\n1. Loading season schedule...")
    schedule_by_id = get_schedule()  # From utils
    if not schedule_by_id:
        print("Error: Could not load schedule.")
        return
    print(f"   ✓ Loaded {len(schedule_by_id)} games from schedule")

    # 2. Re-index by date
    print("\n2. Re-indexing schedule by date...")
    schedule_by_date = get_schedule_by_date(schedule_by_id)  # From utils

    # 3. Finding past games...
    tz = pytz.timezone(constants.FANTASY_TIMEZONE)
    today_str = datetime.now(tz).strftime("%Y-%m-%d")
    all_past_games = []
    for game_date_str, games_on_day in sorted(schedule_by_date.items()):
        if game_date_str < today_str:
            all_past_games.extend(games_on_day)
    print(f"   ✓ Found {len(all_past_games)} past games")
    if not all_past_games:
        print("   No past games found.")
        return

    # 4. Checking database...
    print("\n4. Checking database for existing data...")
    with Session(engine) as session:
        existing_game_ids = set(
            session.exec(select(PlayerGameStats.game_id).distinct()).all()
        )
        print(f"   ✓ Found {len(existing_game_ids)} games already in DB")

        games_to_fetch = [
            game
            for game in all_past_games
            if int(game["game_id"]) not in existing_game_ids
        ]

        if not games_to_fetch:
            print("\n✓ Database is already up-to-date!")
            return
        print(f"   ✓ Need to fetch {len(games_to_fetch)} new games")

        # 5. Fetching and processing games...
        print("\n5. Fetching and processing games...")
        total_skaters, total_goalies = 0, 0

        COMMIT_BATCH_SIZE = 50

        for i, game in enumerate(games_to_fetch, 1):
            print(
                f"\n[{i}/{len(games_to_fetch)}] Game {int(game['game_id'])} on {game['game_date_str']}"
            )

            # === CALL THE REUSABLE FUNCTION ===
            start_time = time.time()
            skaters, goalies = process_game(game, session)  # From utils
            end_time = time.time()
            print(f"Total execution time: {end_time - start_time:.2f} seconds")

            total_skaters += skaters
            total_goalies += goalies

            if i % COMMIT_BATCH_SIZE == 0:
                print(f"   ... Committing batch of {COMMIT_BATCH_SIZE} games ...")
                session.commit()

        # --- Commit any remaining games ---
        print("   ... Committing final batch ...")
        session.commit()

        print("\n" + "=" * 60)
        print("SUMMARY")
        print(f"Games processed: {len(games_to_fetch)}")
        print(f"Total skaters saved: {total_skaters}")
        print(f"Total goalies saved: {total_goalies}")

        save_player_log_cache()

        print(f"\n✓ Database backfill complete!")


if __name__ == "__main__":
    init_db()  # Create DB and tables if they don't exist
    fetch_and_save_stats()
