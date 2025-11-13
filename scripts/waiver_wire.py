"""
waiver_wire.py

This tool connects to your ESPN league, finds all available free agents,
including those on IR, waivers, or marked unavailable, and then queries
your local database to find which of them are the "hottest" players
over the last 14 days.
"""

from connect_espn import connect
from database import engine
import constants
import pandas as pd
from sqlmodel import text


def get_free_agent_nhl_ids(league) -> list[int]:
    """
    Connects to ESPN and returns a list of all NHL IDs
    for players who are currently free agents, on waivers,
    or marked as unavailable (includes IR/IR+ players).
    """
    print(
        "Fetching all available players from ESPN (FA + WAIVERS + UNAVAILABLE + IR)..."
    )
    all_espn_players = []
    seen_ids = set()

    # Try to grab all possible free agent pools
    for status in ["FREEAGENT", "WAIVERS", "UNAVAILABLE"]:
        try:
            pool = league.free_agents(size=2000, status=status)
        except TypeError:
            # fallback for versions that don't support the 'status' param
            try:
                pool = league.free_agents(size=2000)
            except Exception:
                pool = []
        except Exception:
            pool = []

        if not pool:
            continue

        for player in pool:
            if player.playerId not in seen_ids:
                seen_ids.add(player.playerId)
                all_espn_players.append(player)

    print(f"Found {len(all_espn_players)} total available players across all pools.")

    if not all_espn_players:
        return []

    # Separate IR players for visibility
    ir_players = [
        p for p in all_espn_players if getattr(p, "injuryStatus", "") in ("IR", "IR+")
    ]
    if ir_players:
        print("\nPlayers currently on IR/IR+ but available:")
        for p in ir_players:
            print(f"  (IR) {p.name} [{p.proTeam}]")

    # Get all ESPN IDs
    espn_id_list = [p.playerId for p in all_espn_players]

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
    print("\nCross-referencing ESPN IDs with player_map table...")
    with engine.connect() as conn:
        result = conn.execute(query, params)
        nhl_id_list = [row[0] for row in result]

    print(f"Found {len(nhl_id_list)} mapped NHL IDs for available players.")
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

    print("Querying database for top 25 hottest available players (last 14 days)...")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    return df


def main():
    # 1. Connect to ESPN
    league = connect()
    if not league:
        print("Failed to connect to ESPN league.")
        return

    # 2. Get the list of NHL IDs for all available free agents (includes IRs)
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
