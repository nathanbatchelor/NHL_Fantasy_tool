from sqlmodel import Session, select
from typing import List, Dict, Any

# Import your models
from .models import (
    ProPlayers,
    PlayerGameStats,
    GoalieGameStats,
    FantasyTeam,
    TeamSchedule,
)


# --- Generic Utilities ---


def clear_table(session: Session, model_class: Any) -> int:
    """
    Deletes all rows from a given table. Returns the number of rows deleted.
    WARNING: This is a destructive operation.
    """
    print(f"Clearing all data from {model_class.__tablename__}...")
    statement = select(model_class)
    results = session.exec(statement).all()

    count = len(results)
    for row in results:
        session.delete(row)

    session.commit()
    print(f"Deleted {count} rows.")
    return count


def bulk_insert_data(session: Session, data: List[Any]):
    """
    Inserts a list of SQLModel objects into the database in a single session.
    """
    print(f"Bulk inserting {len(data)} objects...")
    session.add_all(data)
    session.commit()
    print("Bulk insert complete.")


def bulk_merge_data(session: Session, data: List[Any]):
    """
    Merges a list of SQLModel objects into the database session.
    This performs an "upsert" (insert or update) for each item.
    Does NOT commit the session.
    """
    print(f"Merging {len(data)} objects into session...")
    for item in data:
        session.merge(item)
    print("Bulk merge complete. (Remember to commit the session)")


# --- ProPlayers Utilities ---


def get_player_by_nhl_id(session: Session, player_id: int) -> ProPlayers | None:
    """
    Fetches a single player by their NHL ID (player_id).
    """
    statement = select(ProPlayers).where(ProPlayers.player_id == player_id)
    player = session.exec(statement).first()
    return player


def create_or_update_player(session: Session, player_id: int, **kwargs) -> ProPlayers:
    """
    Fetches a player by their NHL ID. If they exist, updates them with
    the provided data. If they don't exist, creates them.

    Usage:
        create_or_update_player(
            session,
            player_id=8478402,
            player_name="Connor McDavid",
            team_abbrev="EDM"
        )
    """
    player = get_player_by_nhl_id(session, player_id)

    if player:
        # Update existing player
        for key, value in kwargs.items():
            if hasattr(player, key):
                setattr(player, key, value)
        print(f"Updated player: {player.player_name}")
    else:
        # Create new player
        player = ProPlayers(player_id=player_id, **kwargs)
        session.add(player)
        print(f"Created new player: {player.player_name}")

    session.commit()
    session.refresh(player)
    return player


def get_free_agents(session: Session) -> List[ProPlayers]:
    """
    Returns a list of all players who are not on a fantasy team.
    """
    statement = select(ProPlayers).where(ProPlayers.fantasy_team_id == None)
    return session.exec(statement).all()


# --- FantasyTeam Utilities ---


def get_fantasy_team_by_name(session: Session, team_name: str) -> FantasyTeam | None:
    """
    Fetches a fantasy team by its exact name.
    """
    statement = select(FantasyTeam).where(FantasyTeam.team_name == team_name)
    return session.exec(statement).first()


def get_fantasy_team_roster(session: Session, fantasy_team_id: int) -> List[ProPlayers]:
    """
    Returns a list of all ProPlayers on a given fantasy team.
    """
    statement = select(ProPlayers).where(ProPlayers.fantasy_team_id == fantasy_team_id)
    return session.exec(statement).all()


# --- GameStats Utilities ---


def get_player_game_log(session: Session, player_id: int) -> List[PlayerGameStats]:
    """
    Returns all game stats for a single skater, ordered by date.
    """
    statement = (
        select(PlayerGameStats)
        .where(PlayerGameStats.player_id == player_id)
        .order_by(PlayerGameStats.game_date.desc())
    )
    return session.exec(statement).all()


def get_goalie_game_log(session: Session, player_id: int) -> List[GoalieGameStats]:
    """
    Returns all game stats for a single goalie, ordered by date.
    """
    statement = (
        select(GoalieGameStats)
        .where(GoalieGameStats.player_id == player_id)
        .order_by(GoalieGameStats.game_date.desc())
    )
    return session.exec(statement).all()


def get_all_stats_for_date(session: Session, game_date: str) -> Dict[str, List[Any]]:
    """
    Returns all skater and goalie stats for a specific date.
    """
    skater_statement = select(PlayerGameStats).where(
        PlayerGameStats.game_date == game_date
    )
    goalie_statement = select(GoalieGameStats).where(
        GoalieGameStats.game_date == game_date
    )

    skater_stats = session.exec(skater_statement).all()
    goalie_stats = session.exec(goalie_statement).all()

    return {"skaters": skater_stats, "goalies": goalie_stats}
