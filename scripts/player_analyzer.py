"""
player_analyzer.py

A script to run a full fantasy analysis on a single player for the
current fantasy week.

Usage:
  python player_analyzer.py "Player Name Pattern"

Example:
  python player_analyzer.py "S% Walker"
  python player_analyzer.py "M% Ekholm"
  python player_analyzer.py "C% McDavid"
"""

import sys
from typing import Optional
import pandas as pd
from src.database.database import engine  # Imports the engine from your database.py
from src.utils.date_utils import (
    calculate_remaining_week_matchups,
)  # Imports your schedule function
import re

# --- Hardcoded Current Season ---
# This ensures all analysis is for the *current* fantasy season
CURRENT_SEASON = "20252026"  # This is your main fantasy season

# --- SQL Query Templates ---

# Finds the player's most recent team
QUERY_GET_TEAM = """
SELECT 
    player_name, 
    team_abbrev
FROM player_game_stats
WHERE 
    player_name LIKE :player_name
    AND season = :current_season  -- Filter for current season
ORDER BY 
    game_date DESC
LIMIT 1;
"""

# Query 14: Opponent Difficulty
QUERY_OPPONENT_DIFFICULTY = """
SELECT
  opponent_abbrev,
  AVG(total_fpts) AS avg_fpts_allowed
FROM
  player_game_stats
WHERE
  season = :current_season  -- Filter for current season
GROUP BY
  opponent_abbrev
ORDER BY
  avg_fpts_allowed DESC;
"""

# Query 1 (Modified): Player Season Baseline
QUERY_BASELINE = """
SELECT
  player_name,
  COUNT(game_id) AS games_played,
  AVG(total_fpts) AS avg_fpts,
  AVG(goals) AS avg_g,
  AVG(assists) AS avg_a,
  AVG(shots) AS avg_sog
FROM
  player_game_stats
WHERE
  player_name LIKE :player_name
  AND season = :current_season  -- Filter for current season
GROUP BY
    player_name;
"""

# Query 4 (Modified): Player Peripheral "Floor"
QUERY_FLOOR = """
SELECT
  player_name,
  AVG(hits) AS avg_hits,
  AVG(blocked_shots) AS avg_blocks,
  (AVG(hits) * 0.1) + (AVG(blocked_shots) * 0.5) AS avg_peripheral_fpts
FROM
  player_game_stats
WHERE
  player_name LIKE :player_name
  AND season = :current_season  -- Filter for current season
GROUP BY
  player_name;
"""

# Query 6 (Modified): Player Recent Form
QUERY_RECENT_FORM = """
SELECT
  player_name,
  COUNT(game_id) AS games_played_last_14_days,
  AVG(total_fpts) AS avg_fpts_last_14_days
FROM
  player_game_stats
WHERE
  game_date >= date('now', '-14 days')
  AND player_name LIKE :player_name
  AND season = :current_season  -- Filter for current season
GROUP BY
  player_name;
"""

# Query 5 (Modified): Player History vs. All Teams
# NOTE: This query intentionally searches ALL seasons to get a larger sample size.
QUERY_HISTORY = """
SELECT
  opponent_abbrev,
  COUNT(game_id) AS games_against,
  AVG(total_fpts) AS avg_fpts_against
FROM
  player_game_stats
WHERE
  player_name LIKE :player_name
GROUP BY
  opponent_abbrev
ORDER BY
  avg_fpts_against DESC;
"""


def print_header(title):
    """Helper function to print clean section headers."""
    print("\n" + "=" * 60)
    print(f" {title.upper()}")
    print("=" * 60)


def get_opponent_from_string(game_str: str) -> Optional[str]:
    """
    Extracts opponent abbreviation from game string.

    Args:
        game_str: Game string like 'vs WSH (2025-11-17)' or '@ BOS (2025-11-18)'

    Returns:
        Team abbreviation or None if parsing fails

    Raises:
        ValueError: If game_str is empty
    """
    if not game_str or not game_str.strip():
        raise ValueError("game_str cannot be empty")

    match = re.search(r"(@|vs) ([A-Z]{2,3})", game_str)
    if not match:
        print(f"Warning: Could not parse opponent from '{game_str}'")
        return None

    return match.group(2)


def main():
    if len(sys.argv) != 2:
        print('Usage: python player_analyzer.py "Player Name Pattern"')
        print('Example: python player_analyzer.py "S% Walker"')
        sys.exit(1)

    player_like_pattern = sys.argv[1]

    # Use one database connection for all queries
    with engine.connect() as connection:

        print_header(f"ANALYSIS FOR: {player_like_pattern} ({CURRENT_SEASON})")

        # --- 1. Get Player's Team ---
        params = {"player_name": player_like_pattern, "current_season": CURRENT_SEASON}
        df_team = pd.read_sql(QUERY_GET_TEAM, connection, params=params)

        if df_team.empty:
            print(f"Error: Player not found with pattern '{player_like_pattern}'")
            return

        player_name = df_team.iloc[0]["player_name"]
        team_abbrev = df_team.iloc[0]["team_abbrev"]
        print(f"Found Player: {player_name} (Team: {team_abbrev})")

        # --- 2. Get Schedule & Opponent Difficulty ---
        all_matchups = calculate_remaining_week_matchups()
        player_matchups = all_matchups.get(team_abbrev, [])

        # Pass params to filter difficulty for the current season
        df_difficulty = pd.read_sql(
            QUERY_OPPONENT_DIFFICULTY, connection, params=params
        )
        # Add a rank column for easy reference
        df_difficulty["rank"] = (
            df_difficulty["avg_fpts_allowed"].rank(ascending=False).astype(int)
        )
        # Create a dict for easy lookup
        difficulty_dict = df_difficulty.set_index("opponent_abbrev").to_dict("index")

        print_header("THIS WEEK'S SCHEDULE")
        if not player_matchups:
            print("  No games found for the rest of this week.")
        else:
            print(f"  {team_abbrev} ({len(player_matchups)} games remaining)")
            for game_str in player_matchups:
                opponent = get_opponent_from_string(game_str)
                if opponent and opponent in difficulty_dict:
                    rank = difficulty_dict[opponent]["rank"]
                    fpts = difficulty_dict[opponent]["avg_fpts_allowed"]

                    if rank <= 10:
                        difficulty = "Easy"
                    elif rank >= 23:
                        difficulty = "Hard"
                    else:
                        difficulty = "Average"

                    print(
                        f"    - {game_str:<25} (Matchup: {difficulty} - {rank}th, allows {fpts:.2f} Fpts)"
                    )
                else:
                    print(f"    - {game_str}")

        # --- 3. Run Player-Specific Queries ---

        print_header(f"SEASON BASELINE (QUERY 1) - {CURRENT_SEASON}")
        df_baseline = pd.read_sql(QUERY_BASELINE, connection, params=params)
        print(df_baseline.to_string(index=False))

        print_header(f"PERIPHERAL 'FLOOR' (QUERY 4) - {CURRENT_SEASON}")
        df_floor = pd.read_sql(QUERY_FLOOR, connection, params=params)
        print(df_floor.to_string(index=False))

        print_header(f"RECENT FORM (LAST 14 DAYS) (QUERY 6) - {CURRENT_SEASON}")
        df_recent = pd.read_sql(QUERY_RECENT_FORM, connection, params=params)
        if df_recent.empty:
            print("  No games played in the last 14 days.")
        else:
            print(df_recent.to_string(index=False))

        print_header("PERFORMANCE HISTORY VS. ALL TEAMS (QUERY 5) - ALL SEASONS")
        # NOTE: We intentionally *do not* pass the current_season param here
        # We want all history for this query.
        history_params = {"player_name": player_like_pattern}
        df_history = pd.read_sql(QUERY_HISTORY, connection, params=history_params)
        if df_history.empty:
            print("  No game history found for this player.")
        else:
            print(df_history.to_string(index=False))

    print("\nAnalysis complete.\n")


if __name__ == "__main__":
    main()
