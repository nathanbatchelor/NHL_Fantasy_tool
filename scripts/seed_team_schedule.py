"""
seed_team_schedule.py

One-time script to load team_schedule.csv into the database.
"""

import pandas as pd
from sqlmodel import Session
from src.database.database import engine
from src.database.models import TeamSchedule

# --- NEW IMPORT ---
from src.database.utils import bulk_insert_data, clear_table

CSV_PATH = "data/team_weekly_schedule.csv"


def seed_team_schedule():
    print("Loading team schedule data...")
    df = pd.read_csv(CSV_PATH)

    # Convert dataframe rows to a list of TeamSchedule objects
    entries = []
    for _, row in df.iterrows():
        entries.append(
            TeamSchedule(
                team=row["Team"],
                week=row["Week"],
                monday_date=row["Monday_Date"],
                sunday_date=row["Sunday_Date"],
                game_count=int(row["Game_Count"]),
                opponents=row["Opponents"],
            )
        )

    # Use the database utils to perform the import
    with Session(engine) as session:
        # Clear the table first to avoid duplicates on re-run
        clear_table(session, TeamSchedule)
        # Bulk insert all entries in one transaction
        bulk_insert_data(session, entries)

    print(f"Loaded {len(df)} rows into team_schedule table.")


if __name__ == "__main__":
    seed_team_schedule()
