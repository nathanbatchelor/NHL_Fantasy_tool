"""
This is the central file for all database models and connection logic.
"""

import src.core.constants as constants  # Assuming you store your DB path in constants
from pathlib import Path

from sqlmodel import SQLModel, create_engine
from src.database.models import PlayerGameStats, GoalieGameStats, PlayerMap, TeamSchedule


# --- Database Setup ---

DATABASE_FILE = "data/nhl_stats.db"
sqlite_url = f"sqlite:///{DATABASE_FILE}"

engine = create_engine(sqlite_url)


# --- Initialization Function ---


def init_db():
    """
    This is the "one-time" initialization function.
    It creates the database file and all tables if they don't exist.
    """
    print("Initializing database...")
    # Ensure the 'data' directory exists
    Path(DATABASE_FILE).parent.mkdir(parents=True, exist_ok=True)

    # Now this knows about PlayerGameStats and GoalieGameStats
    SQLModel.metadata.create_all(engine)
    print(f"Database {DATABASE_FILE} and tables created successfully.")


if __name__ == "__main__":
    init_db()
