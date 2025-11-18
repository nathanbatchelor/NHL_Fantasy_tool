from sqlmodel import Session, func, select, desc
from typing import List, Dict, Union, Any, cast

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


def bulk_merge_data(session: Session, data: List[Any]) -> int:
    """
    Merges a list of SQLModel objects into the database session.
    This performs an "upsert" (insert or update) for each item.
    Does NOT commit the session.

    Returns:
        Number of successfully merged items

    Raises:
        Exception: Re-raises the first critical error after logging all failures
    """
    if not data:
        print("No data to merge.")
        return 0

    print(f"Merging {len(data)} objects into session...")
    merged_count = 0
    failed_items: List[tuple[Any, Exception]] = []

    for item in data:
        try:
            session.merge(item)
            merged_count += 1
        except Exception as e:
            # Log the error but continue processing
            print(f"Warning: Failed to merge item {item}: {e}")
            failed_items.append((item, e))

    if failed_items:
        print(f"⚠️  {len(failed_items)} items failed to merge out of {len(data)}")
        # You could log failed items to a file here if needed

    print(f"Bulk merge complete. Successfully merged {merged_count}/{len(data)} items.")
    return merged_count


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
    return list(session.exec(statement).all())


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
    return list(session.exec(statement).all())


# --- GameStats Utilities ---


def get_player_game_log(session: Session, player_id: int) -> List[PlayerGameStats]:
    """
    Returns all game stats for a single skater, ordered by date.
    """
    statement = (
        select(PlayerGameStats)
        .where(PlayerGameStats.player_id == player_id)
        .order_by(desc(PlayerGameStats.game_date))
    )
    return list(session.exec(statement).all())


def get_goalie_game_log(session: Session, player_id: int) -> List[GoalieGameStats]:
    """
    Returns all game stats for a single goalie, ordered by date.
    """
    statement = (
        select(GoalieGameStats)
        .where(GoalieGameStats.player_id == player_id)
        .order_by(desc(GoalieGameStats.game_date))
    )
    return list(session.exec(statement).all())


def get_all_stats_for_date(
    session: Session, game_date: str
) -> Dict[str, List[Union[PlayerGameStats, GoalieGameStats]]]:
    """
    Returns all skater and goalie stats for a specific date.
    """
    skater_statement = select(PlayerGameStats).where(
        PlayerGameStats.game_date == game_date
    )
    goalie_statement = select(GoalieGameStats).where(
        GoalieGameStats.game_date == game_date
    )

    skater_stats: List[Union[PlayerGameStats, GoalieGameStats]] = cast(
        List[Union[PlayerGameStats, GoalieGameStats]],
        list(session.exec(skater_statement).all()),
    )
    goalie_stats: List[Union[PlayerGameStats, GoalieGameStats]] = cast(
        List[Union[PlayerGameStats, GoalieGameStats]],
        list(session.exec(goalie_statement).all()),
    )

    return {"skaters": skater_stats, "goalies": goalie_stats}


def find_player_interactive(
    session: Session, free_agents_only: bool = False
) -> ProPlayers | None:
    """
    Interactively prompts the user to find a player in the pro_players table.
    Handles 0, 1, and 2+ results cases.
    If free_agents_only is True, only searches for players with fantasy_team_id IS NULL.
    """
    while True:
        search_prompt = "  > Add player name (e.g., C. McDavid) or 'stop': "
        if free_agents_only:
            search_prompt = "  > Search Free Agent name (e.g., C. McDavid) or 'stop': "

        search_name = input(search_prompt).strip()
        if not search_name:
            continue
        if search_name.lower() == "stop":
            return None

        # Search the database
        search_pattern = f"%{search_name}%"
        statement = select(ProPlayers).where(
            func.lower(ProPlayers.player_name).like(search_pattern.lower())
        )

        # --- MODIFICATION ---
        # Add filter for free agents if requested
        if free_agents_only:
            statement = statement.where(ProPlayers.fantasy_team_id == None)

        results = session.exec(statement).all()

        if len(results) == 0:
            print(f"  No players found matching '{search_name}'. Please try again.")
            continue

        if len(results) == 1:
            player = results[0]
            confirm_res = input(
                f"  Found: {player.player_name} ({player.team_abbrev}). Select? (Y/n): "
            ).lower()
            if confirm_res in ["", "y", "yes"]:
                return player
            else:
                print("  Player not selected.")
                continue

        # More than 1 result, force user to pick
        print(
            f"\n  Found {len(results)} players matching '{search_name}'. Please pick one by ID:"
        )
        print("  " + "-" * 70)
        print(f"  {'ID':<12} | {'Name':<25} | {'Team':<5} | {'#':<3} | {'Pos':<5}")
        print("  " + "-" * 70)
        id_map = {}
        for player in results:
            id_map[str(player.player_id)] = player
            print(
                f"  {player.player_id:<12} | {player.player_name:<25} | {player.team_abbrev:<5} | {player.jersey_number:<3} | {player.position:<5}"
            )
        print("  " + "-" * 70)

        while True:
            choice_id = input("  > Enter Player ID to select (or 'cancel'): ").strip()
            if choice_id.lower() == "cancel":
                break

            chosen_player = id_map.get(choice_id)
            if chosen_player:
                print(f"  Selected: {chosen_player.player_name}")
                return chosen_player
            else:
                print(f"  Invalid ID '{choice_id}'. Please try again.")
