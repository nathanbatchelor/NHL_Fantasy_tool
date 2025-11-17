# In: NHL_Fantasy_tool/get_remaining_week_matchups.py

import pytz
from datetime import datetime, timedelta
from src.core.constants import FANTASY_TIMEZONE, NHL_TEAMS

# --- Import the new logic function ---
from src.utils.date_utils import calculate_remaining_week_matchups
import asyncio


async def print_remaining_matchups():
    """
    Fetches and prints remaining games for the current fantasy week.
    """
    print("=" * 60)
    print("REMAINING GAMES FOR CURRENT FANTASY WEEK")
    print("=" * 60)

    # 1. Get today's date info for the printout
    tz = pytz.timezone(FANTASY_TIMEZONE)
    today = datetime.now(tz)
    today_weekday_iso = today.isoweekday()
    start_of_week = today - timedelta(days=today_weekday_iso - 1)
    end_of_week = today + timedelta(days=7 - today_weekday_iso)

    print(
        f"Current Fantasy Week: {start_of_week.strftime('%Y-%m-%d')} to {end_of_week.strftime('%Y-%m-%d')}"
    )
    print(f"Today's Date: {today.strftime('%Y-%m-%d')}\n")

    # 2. Call the reusable logic function
    matchups = await calculate_remaining_week_matchups()

    # 3. Print the results
    print("--- Remaining Matchups ---")
    for team in NHL_TEAMS:
        if team in matchups:
            game_count = len(matchups[team])
            opponents_str = ", ".join(matchups[team])
            print(f"  {team} ({game_count} games): {opponents_str}")
        else:
            print(f"  {team} (0 games): None")


if __name__ == "__main__":
    asyncio.run(print_remaining_matchups())
