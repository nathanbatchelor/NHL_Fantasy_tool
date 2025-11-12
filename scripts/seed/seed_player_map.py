"""
seed_player_map.py

This is a one-time script to populate the 'player_map' table.
It connects to both your local DB and the ESPN API, matches
players by their full name, and saves the (nhl_id, espn_id) pair.

This enables much faster and more reliable queries, as we will
no longer need to match on strings.
"""

import pandas as pd
from espn_api.hockey import League
from sqlmodel import Session
from database import engine
from models.database import PlayerMap
import connect_espn

# --- Configuration ---

# Get all unique players from our NHL database
QUERY_DB_PLAYERS = """
    SELECT player_id, player_name
    FROM player_game_stats
    WHERE player_name IS NOT NULL
    
    UNION
    
    SELECT player_id, player_name
    FROM goalie_game_stats
    WHERE player_name IS NOT NULL
"""


def get_db_players():
    """Returns a dict of {player_name: nhl_id} from your database."""
    print("Fetching all unique players from nhl_stats.db...")
    with engine.connect() as connection:
        df_players = pd.read_sql(QUERY_DB_PLAYERS, connection)

    # Convert to a dictionary for fast O(1) lookups
    player_map = df_players.set_index("player_name")["player_id"].to_dict()
    print(f"Found {len(player_map)} unique players in local DB.")
    return player_map


def format_player_name(name):
    assert isinstance(name, str)
    split_name = name.split(" ")

    first_name_initial = split_name[0][0]
    last_name = split_name[1]
    res = f"{first_name_initial}. {last_name}"

    return res


def main():
    # 1. Get player list from our database
    db_player_map = get_db_players()  # { "Connor McDavid": 8478402 }

    league = connect_espn.connect()

    # 2. Get player list from ESPN
    espn_players = [
        (espn_id, name)
        for name, espn_id in league.player_map.items()
        if isinstance(espn_id, int)
    ]

    if not espn_players:
        print("Could not fetch ESPN players. Exiting.")
        return

    print("\nMatching players and populating 'player_map' table...")

    matched_count = 0
    unmatched_count = 0

    with Session(engine) as session:
        for espn_id, espn_name in espn_players:
            formatted_name = format_player_name(espn_name)

            nhl_id = db_player_map.get(formatted_name)
            if nhl_id:
                # 4. If matched, create the map entry
                player_map_entry = PlayerMap(
                    nhl_id=int(nhl_id),
                    espn_id=espn_id,
                    player_name=formatted_name,
                )

                # Use session.merge() to insert or update
                session.merge(player_map_entry)
                matched_count += 1
            else:
                # Player exists in ESPN but not our DB (e.g., old/retired/minor leaguer)
                unmatched_count += 1

        # 5. Commit all changes to the DB
        session.commit()

    print("\n" + "=" * 50)
    print(" PLAYER MAP SEEDING COMPLETE")
    print("=" * 50)
    print(f"  ✓ Matched:   {matched_count} players")
    print(f"  ! Unmatched: {unmatched_count} players")
    print("\nYour 'player_map' table is now populated.")


if __name__ == "__main__":
    main()
