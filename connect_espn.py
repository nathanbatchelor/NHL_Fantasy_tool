"""
connect_to_espn.py

This script connects to a private ESPN fantasy hockey league
using the espn-api library.
"""

import os
from dotenv import load_dotenv
from espn_api.hockey import League


def connect():

    load_dotenv()

    LEAGUE_ID = os.environ.get("LEAGUE_ID")
    YEAR = os.environ.get("YEAR")

    ESPN_S2 = os.environ.get("ESPN_S2")
    SWID = os.environ.get("SWID")

    if not all([LEAGUE_ID, ESPN_S2, SWID]):
        print("!" * 60)
        print("ERROR: Missing required environment variables.")
        print("Please make sure you have a .env file in the root directory")
        print("with LEAGUE_ID, ESPN_S2, and SWID.")
        print("!" * 60)
        exit()

    # --- 2. CONNECT TO THE LEAGUE ---
    print(f"Connecting to ESPN league {LEAGUE_ID} for {YEAR}...")

    try:
        league = League(
            league_id=int(LEAGUE_ID),
            year=int(YEAR),
            espn_s2=ESPN_S2,
            swid=SWID,
        )
        print("✅ Connection successful!")
        return league

    except Exception as e:
        print("\n" + "!" * 60)
        print(f"ERROR: Could not connect to the league.")
        print("Please check that your LEAGUE_ID, ESPN_S2, and SWID are correct.")
        print(f"Details: {e}")
        print("!" * 60)
        exit()


if __name__ == "__main__":
    connect()
