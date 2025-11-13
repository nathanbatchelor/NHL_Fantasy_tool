"""
seed_player_map.py

One-time script to populate the 'player_map' table.
It connects to your local NHL stats DB and the ESPN API,
matches players by normalized full name, and saves (nhl_id, espn_id, ACTUAL_player_name).

This allows much faster and more reliable lookups later.

IMPORTANT: Loops through DATABASE players first to ensure players who actually
played games take priority over free agents with similar names.
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
    """
    Returns {normalized_name: (nhl_id, actual_name)} for all players in the local DB.
    """
    print("Fetching all unique players from nhl_stats.db...")

    with engine.connect() as connection:
        df_players = pd.read_sql(QUERY_DB_PLAYERS, connection)

    df_players["normalized_name"] = df_players["player_name"].apply(normalize_name)

    # Keep the first occurrence if duplicates exist
    # Return both the nhl_id AND the actual player_name
    player_map = {}
    for _, row in df_players.drop_duplicates(subset="normalized_name").iterrows():
        player_map[row["normalized_name"]] = (row["player_id"], row["player_name"])

    print(f"Found {len(player_map)} unique players in local DB.")
    return player_map


def get_espn_players(league) -> dict:
    """
    Returns {normalized_name: espn_id} for all ESPN players.
    """
    print("Fetching all ESPN players...")

    espn_player_map = {}
    for name, espn_id in league.player_map.items():
        if isinstance(espn_id, int):
            normalized = normalize_name(name)
            if normalized:
                espn_player_map[normalized] = espn_id

    print(f"Found {len(espn_player_map)} ESPN players.")
    return espn_player_map


# --- Main ---


def main():
    # 1. Get all DB players (normalized name → (nhl_id, actual_name))
    db_player_map = get_db_players()

    # 2. Connect to ESPN league
    league = connect_espn.connect()

    # 3. Get all ESPN players (normalized_name → espn_id)
    espn_player_map = get_espn_players(league)

    if not espn_player_map:
        print("Could not fetch ESPN players. Exiting.")
        return

    print("\nMatching players and populating 'player_map' table...")

    matched_count = 0
    unmatched_count = 0

    with Session(engine) as session:
        # CRITICAL: Loop through DATABASE players first!
        # This ensures players who actually played games get priority
        for normalized_name, (nhl_id, actual_player_name) in db_player_map.items():

            # Skip players in the avoid list
            espn_id = espn_player_map.get(normalized_name)
            if espn_id and espn_id in getattr(constants, "AVOID_PLAYER_ESPN_IDS", []):
                continue

            if espn_id:
                print(f"✓ {actual_player_name} (NHL:{nhl_id} → ESPN:{espn_id})")

                # Create or update entry using the ACTUAL name from the database
                player_map_entry = PlayerMap(
                    nhl_id=int(nhl_id),
                    espn_id=espn_id,
                    player_name=actual_player_name,
                )
                session.merge(player_map_entry)
                matched_count += 1
            else:
                print(f"✗ {actual_player_name} (no ESPN match)")
                unmatched_count += 1

        session.commit()

    print("\n" + "=" * 50)
    print(" PLAYER MAP SEEDING COMPLETE")
    print("=" * 50)
    print(f"  ✓ Matched:   {matched_count} players")
    print(f"  ✗ Unmatched: {unmatched_count} players")
    print("\nYour 'player_map' table is now populated.")
    print("Players who actually played games were prioritized over free agents.")


if __name__ == "__main__":
    main()
