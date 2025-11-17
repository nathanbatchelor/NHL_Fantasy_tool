"""
scripts/populate_pro_players.py

Scans the player_game_stats and goalie_game_stats tables and creates/updates
entries in the pro_players table with the most recent information.

This creates a master list of all NHL players in your database.
"""

from sqlmodel import Session, select, func
from src.database.database import engine
from src.database.models import ProPlayers, PlayerGameStats, GoalieGameStats
from src.core.constants import SEASON_ID
from typing import Dict, Optional


def get_latest_skater_info(session: Session) -> Dict[int, dict]:
    """
    Gets the most recent game info for each skater.
    Returns dict: {player_id: {player_name, team_abbrev, position, jersey_number}}
    """
    print("Collecting skater information...")

    # Subquery to get the latest game date for each player
    latest_game_subquery = (
        select(
            PlayerGameStats.player_id,
            func.max(PlayerGameStats.game_date).label("latest_date"),
        )
        .where(PlayerGameStats.season == SEASON_ID)
        .group_by(PlayerGameStats.player_id)
        .subquery()
    )

    # Get the full record for each player's latest game
    statement = (
        select(PlayerGameStats)
        .join(
            latest_game_subquery,
            (PlayerGameStats.player_id == latest_game_subquery.c.player_id)
            & (PlayerGameStats.game_date == latest_game_subquery.c.latest_date),
        )
        .where(PlayerGameStats.season == SEASON_ID)
    )

    results = session.exec(statement).all()

    player_info = {}
    for stat in results:
        player_info[stat.player_id] = {
            "player_name": stat.player_name,
            "team_abbrev": stat.team_abbrev,
            "position": stat.position,
            "jersey_number": stat.jersey_number,
            "is_goalie": False,
        }

    print(f"  Found {len(player_info)} unique skaters")
    return player_info


def get_latest_goalie_info(session: Session) -> Dict[int, dict]:
    """
    Gets the most recent game info for each goalie.
    Returns dict: {player_id: {player_name, team_abbrev, position, jersey_number}}
    """
    print("Collecting goalie information...")

    # Subquery to get the latest game date for each goalie
    latest_game_subquery = (
        select(
            GoalieGameStats.player_id,
            func.max(GoalieGameStats.game_date).label("latest_date"),
        )
        .where(GoalieGameStats.season == SEASON_ID)
        .group_by(GoalieGameStats.player_id)
        .subquery()
    )

    # Get the full record for each goalie's latest game
    statement = (
        select(GoalieGameStats)
        .join(
            latest_game_subquery,
            (GoalieGameStats.player_id == latest_game_subquery.c.player_id)
            & (GoalieGameStats.game_date == latest_game_subquery.c.latest_date),
        )
        .where(GoalieGameStats.season == SEASON_ID)
    )

    results = session.exec(statement).all()

    goalie_info = {}
    for stat in results:
        goalie_info[stat.player_id] = {
            "player_name": stat.player_name,
            "team_abbrev": stat.team_abbrev,
            "position": "Goalie",
            "jersey_number": stat.jersey_number,
            "is_goalie": True,
        }

    print(f"  Found {len(goalie_info)} unique goalies")
    return goalie_info


def calculate_season_totals(session: Session, player_id: int, is_goalie: bool) -> dict:
    """
    Calculate accumulated season stats for a player.
    """
    if is_goalie:
        statement = select(GoalieGameStats).where(
            GoalieGameStats.player_id == player_id, GoalieGameStats.season == SEASON_ID
        )
        games = session.exec(statement).all()

        return {
            "season_games_played": len(games),
            "season_total_fpts": sum(g.total_fpts for g in games),
            "season_wins": sum(g.wins for g in games),
            "season_shutouts": sum(g.shutouts for g in games),
            "season_ot_losses": sum(g.ot_losses for g in games),
            "season_saves": sum(g.saves for g in games),
            "season_goals_against": sum(g.goals_against for g in games),
        }
    else:
        statement = select(PlayerGameStats).where(
            PlayerGameStats.player_id == player_id, PlayerGameStats.season == SEASON_ID
        )
        games = session.exec(statement).all()

        return {
            "season_games_played": len(games),
            "season_total_fpts": sum(g.total_fpts for g in games),
            "season_goals": sum(g.goals for g in games),
            "season_assists": sum(g.assists for g in games),
            "season_pp_points": sum(g.pp_points for g in games),
            "season_sh_points": sum(g.sh_points for g in games),
            "season_shots": sum(g.shots for g in games),
            "season_blocked_shots": sum(g.blocked_shots for g in games),
            "season_hits": sum(g.hits for g in games),
        }


def populate_pro_players():
    """
    Main function to populate the pro_players table.
    """
    print("=" * 60)
    print("POPULATING PRO_PLAYERS TABLE")
    print("=" * 60)

    with Session(engine) as session:
        # Step 1: Collect all unique players
        skater_info = get_latest_skater_info(session)
        goalie_info = get_latest_goalie_info(session)

        # Combine (goalies take precedence if a player_id appears in both)
        all_players = {**skater_info, **goalie_info}

        print(f"\nTotal unique players: {len(all_players)}")

        # Step 2: Create/Update ProPlayers records
        print("\nCreating/updating player records...")
        created_count = 0
        updated_count = 0

        for player_id, info in all_players.items():
            # Check if player already exists
            existing_player = session.exec(
                select(ProPlayers).where(ProPlayers.player_id == player_id)
            ).first()

            # Calculate season totals
            season_totals = calculate_season_totals(
                session, player_id, info["is_goalie"]
            )

            if existing_player:
                # Update existing player
                existing_player.player_name = info["player_name"]
                existing_player.team_abbrev = info["team_abbrev"]
                existing_player.position = info["position"]
                existing_player.jersey_number = info["jersey_number"]
                existing_player.is_active = True

                # Update season totals
                for key, value in season_totals.items():
                    setattr(existing_player, key, value)

                updated_count += 1
            else:
                # Create new player
                new_player = ProPlayers(
                    player_id=player_id,
                    player_name=info["player_name"],
                    team_abbrev=info["team_abbrev"],
                    position=info["position"],
                    jersey_number=info["jersey_number"],
                    is_active=True,
                    **season_totals,
                )
                session.add(new_player)
                created_count += 1

            # Commit in batches of 100 for better performance
            if (created_count + updated_count) % 100 == 0:
                session.commit()
                print(f"  Processed {created_count + updated_count} players...")

        # Final commit
        session.commit()

        print("\n" + "=" * 60)
        print("✅ POPULATION COMPLETE!")
        print("=" * 60)
        print(f"Players created: {created_count}")
        print(f"Players updated: {updated_count}")
        print(f"Total players in pro_players: {len(all_players)}")


if __name__ == "__main__":
    populate_pro_players()
