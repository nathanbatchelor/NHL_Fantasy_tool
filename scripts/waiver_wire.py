"""
waiver_wire.py

This tool connects to your ESPN league, finds all available free agents,
and then queries your local database to find which of them are
the "hottest" players over the last 14 days.
"""

from connect_espn import connect
from database import engine
import constants
import pandas as pd
from sqlmodel import text


def get_free_agent_nhl_ids(league) -> list[int]:
    """
    Connects to ESPN and returns a list of all NHL IDs
    for players who are currently free agents.
    """
    print("Fetching all available free agents from ESPN...")
    free_agents = league.free_agents(size=2000)
    espn_id_list = [player.playerId for player in free_agents]
    print(f"Found {len(espn_id_list)} free agents available.")

    if not espn_id_list:
        return []

    placeholders = ",".join([f":id{i}" for i in range(len(espn_id_list))])
    query = text(
        f"""
        SELECT nhl_id 
        FROM player_map
        WHERE espn_id IN ({placeholders})
        """
    )

    params = {f"id{i}": espn_id_list[i] for i in range(len(espn_id_list))}

    nhl_id_list = []
    print("Cross-referencing ESPN IDs with player_map table...")
    with engine.connect() as conn:
        result = conn.execute(query, params)
        nhl_id_list = [row[0] for row in result]

    print(f"Found {len(nhl_id_list)} mapped NHL IDs for free agents.")
    return nhl_id_list


def get_hot_players(nhl_id_list: list[int]) -> pd.DataFrame:
    """
    Queries the player_game_stats table to find the hottest
    players, but ONLY for the IDs in the provided list.
    """
    if not nhl_id_list:
        return pd.DataFrame()

    placeholders = ",".join([f":id{i}" for i in range(len(nhl_id_list))])
    query = text(
        f"""
        SELECT
          ps.player_name,
          ps.team_abbrev,
          COUNT(ps.game_id) AS games_last_14_days,
          AVG(ps.total_fpts) AS avg_fpts_last_14_days
        FROM
          player_game_stats ps
        WHERE
          ps.game_date >= date('now', '-14 days')
          AND ps.season = :current_season
          AND ps.player_id IN ({placeholders})
        GROUP BY
          ps.player_id, ps.player_name, ps.team_abbrev
        HAVING
          games_last_14_days > 2
        ORDER BY
          avg_fpts_last_14_days DESC
        LIMIT 25;
        """
    )

    params = {"current_season": constants.SEASON_ID}
    params.update({f"id{i}": nhl_id_list[i] for i in range(len(nhl_id_list))})

    print("Querying database for top 25 hottest free agents...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    return df


def main():
    # 1. Connect to ESPN
    league = connect()
    if not league:
        print("Failed to connect to ESPN league.")
        return

    # 2. Get the list of NHL IDs for all available free agents
    free_agent_nhl_ids = get_free_agent_nhl_ids(league)
    if not free_agent_nhl_ids:
        print("No mapped free agents found. Exiting.")
        return

    # 3. Query our database for the hottest players in that list
    hot_players_df = get_hot_players(free_agent_nhl_ids)

    # 4. Print the final report
    print("\n" + "=" * 60)
    print(" TOP 25 HOTTEST AVAILABLE PLAYERS (LAST 14 DAYS)")
    print("=" * 60)

    if hot_players_df.empty:
        print("No hot players found on the waiver wire.")
    else:
        hot_players_df["avg_fpts_last_14_days"] = hot_players_df[
            "avg_fpts_last_14_days"
        ].round(2)
        print(hot_players_df.to_string(index=False))

    print("\n")


if __name__ == "__main__":
    main()
