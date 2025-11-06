from sqlmodel import Session
from models.database import PlayerGameStats, GoalieGameStats


def save_skater_log(session: Session, log: PlayerGameStats):
    """Insert or update a skater's game log."""
    session.merge(log)


def save_goalie_log(session: Session, log: GoalieGameStats):
    """Insert or update a goalie's game log."""
    session.merge(log)
