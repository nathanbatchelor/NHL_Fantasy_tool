"""
seed_player_map.py

One-time script to populate the 'player_map' table.
It connects to your local NHL stats DB and the ESPN API,
matches players by normalized full name, and saves (nhl_id, espn_id).

This allows much faster and more reliable lookups later.
"""

import pandas as pd
from espn_api.hockey import League
from sqlmodel import Session
from database import engine
from models.database import PlayerMap
import connect_espn
import constants
import unicodedata
import re

# --- Configuration ---

QUERY_DB_PLAYERS = """
    SELECT player_id, player_name
    FROM player_game_stats
    WHERE player_name IS NOT NULL
    
    UNION
    
    SELECT player_id, player_name
    FROM goalie_game_stats
    WHERE player_name IS NOT NULL
"""


def normalize_name(name: str) -> str:
    """Convert a player's full name to a normalized key like 'tstutzle' or 'jvanriemsdyk'."""
    if not isinstance(name, str) or not name.strip():
        return ""

    # Normalize whitespace and lowercase
    name = re.sub(r"\s+", " ", name.strip().lower())

    # Split words and keep all after the first as the 'last name'
    parts = name.split()
    if len(parts) == 1:
        return strip_accents(parts[0])  # single name fallback

    first_name = parts[0]
    last_name = " ".join(parts[1:])  # handle 'van', 'de', etc.

    # Strip accents
    first_name = strip_accents(first_name)
    last_name = strip_accents(last_name)

    # Remove spaces and hyphens from last name to keep it consistent
    last_name = re.sub(r"[\s\-]", "", last_name)

    return f"{first_name[0]}{last_name}"


def strip_accents(s: str) -> str:
    """Remove accent marks from a string."""
    nfkd_form = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd_form if not unicodedata.combining(c))


def get_db_players() -> dict:
    """Returns {normalized_name: nhl_id} for all players in the local DB."""
    print("Fetching all unique players from nhl_stats.db...")

    with engine.connect() as connection:
        df_players = pd.read_sql(QUERY_DB_PLAYERS, connection)

    df_players["normalized_name"] = df_players["player_name"].apply(normalize_name)

    # Keep the first occurrence if duplicates exist
    player_map = (
        df_players.drop_duplicates(subset="normalized_name")
        .set_index("normalized_name")["player_id"]
        .to_dict()
    )

    print(f"Found {len(player_map)} unique players in local DB.")
    return player_map


# --- Main ---


def main():
    # 1. Get all DB players (normalized name → nhl_id)
    db_player_map = get_db_players()

    # 2. Connect to ESPN league
    league = connect_espn.connect()

    # 3. Get all ESPN players (espn_id, normalized_name)
    espn_players = [
        (espn_id, normalize_name(name))
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
        for espn_id, normalized_espn_name in espn_players:
            if espn_id in getattr(constants, "AVOID_PLAYER_ESPN_IDS", []):
                continue

            print(normalized_espn_name)

            nhl_id = db_player_map.get(normalized_espn_name)
            if nhl_id:
                # Create or update entry
                player_map_entry = PlayerMap(
                    nhl_id=int(nhl_id),
                    espn_id=espn_id,
                    player_name=normalized_espn_name,
                )
                session.merge(player_map_entry)
                matched_count += 1
            else:
                unmatched_count += 1

        session.commit()

    print("\n" + "=" * 50)
    print(" PLAYER MAP SEEDING COMPLETE")
    print("=" * 50)
    print(f"  ✓ Matched:   {matched_count} players")
    print(f"  ! Unmatched: {unmatched_count} players")
    print("\nYour 'player_map' table is now populated.")


if __name__ == "__main__":
    main()
