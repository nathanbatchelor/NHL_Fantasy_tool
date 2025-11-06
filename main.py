import sqlite3
from sqlmodel import create_engine, Session, select, SQLModel
from models.database import PlayerGameStats
from pathlib import Path




def init_db(db_path: str = "data/nhl_stats.db"):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(engine)
    print(f"Database initialized at {db_path}")
    return engine


if __name__ == "__main__":
    engine = init_db()
