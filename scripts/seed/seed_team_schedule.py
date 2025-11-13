"""
seed_team_schedule.py

One-time script to load team_schedule.csv into the database.
"""

import pandas as pd
from sqlmodel import Session
from database import engine
from models.database import TeamSchedule

CSV_PATH = "data/team_weekly_schedule.csv"


def seed_team_schedule():
    print("Loading team schedule data...")
    df = pd.read_csv(CSV_PATH)

    with Session(engine) as session:
        for _, row in df.iterrows():
            entry = TeamSchedule(
                team=row["Team"],
                week=row["Week"],
                monday_date=row["Monday_Date"],
                sunday_date=row["Sunday_Date"],
                game_count=int(row["Game_Count"]),
                opponents=row["Opponents"],
            )
            session.add(entry)
        session.commit()

    print(f"Loaded {len(df)} rows into team_schedule table.")


if __name__ == "__main__":
    seed_team_schedule()
