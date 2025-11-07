"""
seed/seed_past_game_data.py
Controller script to seed the database with all past games.

Uses a ThreadPoolExecutor to fetch and parse all games concurrently,
then adds them to the database in a single bulk transaction.

Usage:
  python seed/seed_past_game_data.py
  (To force a full re-run, ignoring cache and existing DB entries):
  python seed/seed_past_game_data.py --force
"""

import sys  # <-- IMPORT SYS
import pytz
from datetime import datetime
from sqlmodel import Session, select
from concurrent.futures import ThreadPoolExecutor, as_completed

from database import engine, init_db

from models.database import PlayerGameStats, GoalieGameStats
import constants

# Import from the new utility files
from utils.date_utils import get_schedule_by_date

# IMPORTANT: Import our new function, not the old one
from utils.nhl_api_utils import (
    get_schedule,
    fetch_and_parse_game_data,
    save_player_log_cache,
)

import time


# <-- MODIFIED FUNCTION DEFINITION
def fetch_and_save_stats(force: bool = False):
    """Main function"""
    print("=" * 60)
    print("NHL PAST GAME DATA SEEDER")
    print("=" * 60)

    # <-- ADDED FORCE CHECK
    if force:
        print("   *** FORCE RUN ENABLED ***")
        print("   Will re-fetch all games, ignoring cache and existing DB data.")

    # 1. Load schedule
    print("\n1. Loading season schedule...")
    # <-- MODIFIED CALL TO USE force
    schedule_by_id = get_schedule(force_refresh=force)
    if not schedule_by_id:
        print("Error: Could not load schedule.")
        return
    print(f"   ✓ Loaded {len(schedule_by_id)} games from schedule")

    # 2. Re-index by date
    print("\n2. Re-indexing schedule by date...")
    schedule_by_date = get_schedule_by_date(schedule_by_id)

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
    # We create the session here, to get existing games
    # We will also use THIS session at the end to commit
    with Session(engine) as session:

        games_to_fetch = []  # Define scope

        # <-- ADDED if/else BLOCK FOR FORCE CHECK
        if force:
            print("   ! FORCE RUN: Re-fetching all past games.")
            games_to_fetch = all_past_games
        else:
            existing_game_ids = set(
                session.exec(select(PlayerGameStats.game_id).distinct()).all()
            )
            print(f"   ✓ Found {len(existing_game_ids)} games already in DB")

            games_to_fetch = [
                game
                for game in all_past_games
                if int(game["game_id"]) not in existing_game_ids
            ]
        # <-- END if/else BLOCK

        if not games_to_fetch:
            print("\n✓ Database is already up-to-date!")
            return

        num_games = len(games_to_fetch)
        print(f"   ✓ Need to fetch {num_games} new games")

        # 5. Fetching and processing games... (CONCURRENTLY)
        print("\n5. Submitting all games to thread pool...")

        # Master lists to hold all returned objects
        all_skaters_to_add: list[PlayerGameStats] = []
        all_goalies_to_add: list[GoalieGameStats] = []

        futures_to_game = {}
        games_processed = 0

        # Use a ThreadPoolExecutor to fetch in parallel
        # We set max_workers to 4 to be polite to the API
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all jobs to the pool
            for game in games_to_fetch:
                future = executor.submit(fetch_and_parse_game_data, game)
                futures_to_game[future] = game

            print(f"   ✓ All {num_games} games submitted. Waiting for results...")

            # Process results as they complete
            for i, future in enumerate(as_completed(futures_to_game), 1):
                game = futures_to_game[future]
                game_id = game["game_id"]

                try:
                    # Get the result from the thread
                    skater_list, goalie_list = future.result()

                    if skater_list or goalie_list:
                        all_skaters_to_add.extend(skater_list)
                        all_goalies_to_add.extend(goalie_list)
                        games_processed += 1

                    if i % 20 == 0 or i == num_games:
                        print(f"   ... Progress: {i}/{num_games} jobs complete")

                except Exception as e:
                    print(f"   ✗ CRITICAL ERROR processing game {game_id}: {e}")

        print("\n6. All API requests complete.")
        print("   Adding all data to session...")

        # --- BULK ADD AND COMMIT ---
        # This is the single database transaction
        # NOTE: Using merge() is safer than add_all() if --force
        # is run, as it will update existing entries instead of
        # creating duplicates or erroring.
        for skater in all_skaters_to_add:
            session.merge(skater)
        for goalie in all_goalies_to_add:
            session.merge(goalie)

        print("   Committing transaction...")
        start_commit = time.time()
        session.commit()
        end_commit = time.time()
        print(f"   ✓ Commit complete in {end_commit - start_commit:.2f}s")

        print("\n" + "=" * 60)
        print("SUMMARY")
        print(f"Games processed: {games_processed} / {num_games}")
        print(f"Total skaters saved: {len(all_skaters_to_add)}")
        print(f"Total goalies saved: {len(all_goalies_to_add)}")

        save_player_log_cache()

        print(f"\n✓ Database backfill complete!")


if __name__ == "__main__":
    init_db()  # Create DB and tables if they don't exist

    # <-- MODIFIED MAIN EXECUTION BLOCK
    # Check for --force argument
    force_run = "--force" in sys.argv

    start_time = time.time()
    fetch_and_save_stats(force=force_run)  # <-- PASS THE FLAG
    end_time = time.time()
    print(f"\nTotal execution time: {end_time - start_time:.2f} seconds")
