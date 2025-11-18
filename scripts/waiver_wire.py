"""
waiver_wire.py

Finds all available free agents from the local database
and identifies the "hottest" players over the last 14 days.

This script runs entirely off your local DB and assumes
the `pro_players` table is kept up-to-date by other scripts.
"""

import pandas as pd
from sqlmodel import Session, text
from src.database.database import engine
from src.core.constants import SEASON_ID

# We can combine finding free agents and hot players into one SQL query
QUERY_HOT_FREE_AGENTS = text(
    """
    SELECT
      pp.player_name,
      pp.team_abbrev,
      pp.position,
      -- ADDED: Join to fantasy_team table to get the team name
      COALESCE(ft.team_name, '*** Free Agent ***') AS fantasy_team,
      COUNT(ps.game_id) AS games_last_14,
      AVG(ps.total_fpts) AS avg_fpts_last_14
    FROM
      player_game_stats ps
    JOIN
      pro_players pp ON ps.player_id = pp.player_id
    -- ADDED: LEFT JOIN to get the fantasy team name if it exists
    LEFT JOIN
      fantasy_team ft ON pp.fantasy_team_id = ft.team_id
    WHERE
      ps.game_date >= date('now', '-14 days')
      AND ps.season = :current_season
      AND (pp.fantasy_team_id IS NULL or pp.fantasy_team_id IS 1) -- Free Agents or Team 1
    GROUP BY
      pp.player_id, pp.player_name, pp.team_abbrev, pp.position, ft.team_name
    HAVING
      games_last_14 > 2  -- Only show players with a decent number of games
    ORDER BY
      avg_fpts_last_14 DESC
    LIMIT 50;
    """
)


def get_hot_players() -> pd.DataFrame:
    """
    Queries the database for the hottest players who are
    marked as free agents (fantasy_team_id IS NULL).
    """
    params = {"current_season": SEASON_ID}

    print("Querying database for top 25 hottest free agents...")
    with engine.connect() as conn:
        df = pd.read_sql(QUERY_HOT_FREE_AGENTS, conn, params=params)

    return df


def main():
    # 1. Query our database for the hottest players
    hot_players_df = get_hot_players()

    # 2. Print the final report
    print("\n" + "=" * 60)
    print(" TOP 25 HOTTEST AVAILABLE PLAYERS (LAST 14 DAYS)")
    print("=" * 60)

    if hot_players_df.empty:
        print("No hot players found on the waiver wire.")
    else:
        hot_players_df["avg_fpts_last_14"] = hot_players_df["avg_fpts_last_14"].round(2)
        print(hot_players_df.to_string(index=False))

    print("\n")


if __name__ == "__main__":
    main()
