import argparse
import sys
from sqlmodel import Session, select, func
from src.core import constants
from src.database.utils import clear_table, find_player_interactive
from src.database.database import engine
from src.database.models import FantasyTeam, ProPlayers
from typing import List


def force_refresh(auto_confirm=False, session: Session = None):
    """
    Clears the FantasyTeam table after confirmation.
    """
    res = None
    if not session:
        print("ERROR: No session found")
        return

    if not auto_confirm:
        print("\nWARNING: This will delete all fantasy teams and their rosters.")
        res = input(
            "Are you sure you want to force refresh the fantasy team table? (y/n): "
        ).lower()

    if res == "y" or auto_confirm:
        clear_table(session, FantasyTeam)
        # Also need to clear the fantasy_team_id from all players
        statement = select(ProPlayers).where(ProPlayers.fantasy_team_id != None)
        players_to_clear = session.exec(statement).all()

        if players_to_clear:
            print(f"Clearing fantasy_team_id from {len(players_to_clear)} players...")
            for player in players_to_clear:
                player.fantasy_team_id = None
            session.add_all(players_to_clear)
            session.commit()

        print("Successfully refreshed FantasyTeams table.")
        return True

    print("Operation cancelled. Refresh was unsuccessful.")
    return False


def add_roster(session: Session, team: FantasyTeam):
    """
    Loops and interactively adds players to a given team's roster from free agency.
    """
    print("\n" + "=" * 50)
    print(f"Adding roster for: {team.team_name} ({team.owner_name})")
    print("Type player names or 'stop' when finished.")
    print("=" * 50)

    while True:
        # --- MODIFICATION ---
        # Call find_player_interactive, searching only free agents
        player_to_add = find_player_interactive(session, free_agents_only=True)

        if player_to_add is None:
            # User typed 'stop'
            print(f"Finished adding players to {team.team_name}.")
            break

        # Check if player is already on a team (should be redundant now, but safe)
        if player_to_add.fantasy_team_id is not None:
            if player_to_add.fantasy_team_id == team.team_id:
                print(f"  {player_to_add.player_name} is already on this team.")
            else:
                print(f"  {player_to_add.player_name} is already on another team!")
            continue

        # Add player to team
        player_to_add.fantasy_team_id = team.team_id
        session.add(player_to_add)
        session.commit()
        print(f"  Added {player_to_add.player_name} to {team.team_name}.")


def add_teams(session: Session):
    """
    Walks the user through adding all teams (user's first) and their rosters.
    """
    print("--- Initial Fantasy Team Setup ---")

    # 1. Add User's Team
    print("\nFirst, let's add your team.")
    user_team_name = ""
    while not user_team_name:
        user_team_name = input("Provide the name of your team: ").strip()

    user_team = FantasyTeam(team_name=user_team_name, owner_name=constants.YOUR_NAME)
    session.add(user_team)
    session.commit()
    session.refresh(user_team)

    print(f"Team '{user_team.team_name}' created for {user_team.owner_name}.")

    # 2. Add User's Roster
    add_roster(session, user_team)

    # 3. Add Opponent Teams
    print("\n--- Add Opponent Teams ---")
    print("Enter team and owner names. Type 'stop' as the team name when finished.")

    team_count = 1
    while True:
        print("\n" + "-" * 30)
        print(f"Opponent Team #{team_count}")

        team_name = input("  Team Name (or 'stop'): ").strip()
        if team_name.lower() == "stop":
            print("Finished adding all teams.")
            break
        if not team_name:
            continue

        owner_name = ""
        while not owner_name:
            owner_name = input(f"  Owner Name for {team_name}: ").strip()

        new_team = FantasyTeam(team_name=team_name, owner_name=owner_name)
        session.add(new_team)
        session.commit()
        session.refresh(new_team)

        print(f"Team '{new_team.team_name}' created for {new_team.owner_name}.")

        # 4. Add Roster for this Opponent
        add_roster(session, new_team)
        team_count += 1


# --- NEW FUNCTION ---
def select_team_interactive(session: Session, prompt: str) -> FantasyTeam | None:
    """
    Displays a list of all teams and prompts the user to select one.
    """
    print(f"\n{prompt}")
    teams = session.exec(select(FantasyTeam).order_by(FantasyTeam.team_name)).all()
    if not teams:
        print("No fantasy teams found in the database.")
        return None

    team_map = {}
    for i, team in enumerate(teams, 1):
        team_map[str(i)] = team
        print(f"  {i}. {team.team_name} ({team.owner_name})")

    print("  (C)ancel")

    while True:
        choice = input("\n> Select team #: ").strip()
        if choice.lower() == "c":
            return None

        chosen_team = team_map.get(choice)
        if chosen_team:
            return chosen_team
        else:
            print("Invalid selection.")


# --- NEW FUNCTION ---
def select_players_from_roster(session: Session, team: FantasyTeam) -> List[ProPlayers]:
    """
    Shows a team's roster and lets the user select one or more players.
    Returns a list of the selected ProPlayers objects.
    """
    print("\n" + "-" * 50)
    print(f"Select players from: {team.team_name} ({team.owner_name})")
    print("Type player IDs separated by commas (e.g., 8478402, 8480069) or 'done'.")
    print("-" * 50)

    roster = session.exec(
        select(ProPlayers)
        .where(ProPlayers.fantasy_team_id == team.team_id)
        .order_by(ProPlayers.player_name)
    ).all()

    if not roster:
        print("  This team has no players.")
        return []

    print(f"  {'ID':<12} | {'Name':<25} | {'Pos':<5}")
    print("  " + "-" * 50)
    id_map = {}
    for player in roster:
        id_map[str(player.player_id)] = player
        print(
            f"  {player.player_id:<12} | {player.player_name:<25} | {player.position:<5}"
        )
    print("  " + "-" * 50)

    selected_players = []
    while True:
        choice_str = input("  > Player IDs to trade (or 'done'): ").strip().lower()
        if choice_str == "done":
            return selected_players

        player_ids = [pid.strip() for pid in choice_str.split(",")]

        valid_players_in_choice = []
        invalid_id_found = False

        for pid in player_ids:
            player = id_map.get(pid)
            if not player:
                print(f"  Invalid ID or player not on this team: '{pid}'")
                invalid_id_found = True
                break
            if player in selected_players:
                print(f"  {player.player_name} is already in the trade list.")
            else:
                valid_players_in_choice.append(player)

        if not invalid_id_found:
            for player in valid_players_in_choice:
                selected_players.append(player)
                print(f"  Added {player.player_name} to trade.")

            # Show current trade list
            if selected_players:
                print("\n  Current players in trade:")
                for p in selected_players:
                    print(f"    - {p.player_name}")
            return selected_players  # Return after one successful entry for simplicity


# --- NEW FUNCTION ---
def execute_trade_interactive(session: Session):
    """
    Walks the user through selecting two teams and the players to trade.
    """
    print("\n" + "=" * 50)
    print("Execute Player Trade")
    print("=" * 50)

    # 1. Select Team A
    team_a = select_team_interactive(session, "Select Team A (Sending Players)")
    if not team_a:
        print("Trade cancelled.")
        return

    # 2. Select Team B
    team_b = select_team_interactive(session, "Select Team B (Receiving Players)")
    if not team_b:
        print("Trade cancelled.")
        return

    if team_a.team_id == team_b.team_id:
        print("Cannot trade with the same team. Trade cancelled.")
        return

    # 3. Select Players from Team A
    players_to_send = select_players_from_roster(session, team_a)
    if not players_to_send:
        print("No players selected from Team A. Trade cancelled.")
        return

    # 4. Select Players from Team B
    players_to_acquire = select_players_from_roster(session, team_b)
    if not players_to_acquire:
        print("No players selected from Team B. Trade cancelled.")
        return

    # 5. Show Confirmation
    print("\n" + "=" * 50)
    print("CONFIRM TRADE")
    print("=" * 50)

    print(f"{team_a.team_name} sends:")
    if not players_to_send:
        print("  - (Nothing)")
    for player in players_to_send:
        print(f"  - {player.player_name} ({player.team_abbrev})")

    print(f"\n{team_b.team_name} sends:")
    if not players_to_acquire:
        print("  - (Nothing)")
    for player in players_to_acquire:
        print(f"  - {player.player_name} ({player.team_abbrev})")

    print("\n" + "=" * 50)

    confirm = input("Execute this trade? (y/N): ").strip().lower()

    # 6. Execute Trade
    if confirm in ["y", "yes"]:
        players_to_commit = []
        # Assign players from A to B
        for player in players_to_send:
            player.fantasy_team_id = team_b.team_id
            players_to_commit.append(player)

        # Assign players from B to A
        for player in players_to_acquire:
            player.fantasy_team_id = team_a.team_id
            players_to_commit.append(player)

        session.add_all(players_to_commit)
        session.commit()
        print("\n✅ Trade Successful!")
    else:
        print("\nTrade Cancelled.")


def drop_player_interactive(session: Session, team: FantasyTeam):
    """
    Shows a team's roster and lets the user pick a player to drop.
    """
    print("\n" + "-" * 50)
    print(f"Dropping player from: {team.team_name}")
    print("-" * 50)

    # 1. Get and display roster
    roster = session.exec(
        select(ProPlayers)
        .where(ProPlayers.fantasy_team_id == team.team_id)
        .order_by(ProPlayers.player_name)
    ).all()

    if not roster:
        print("  This team has no players to drop.")
        return

    print(f"  {'ID':<12} | {'Name':<25} | {'Pos':<5}")
    print("  " + "-" * 50)
    id_map = {}
    for player in roster:
        id_map[str(player.player_id)] = player
        print(
            f"  {player.player_id:<12} | {player.player_name:<25} | {player.position:<5}"
        )
    print("  " + "-" * 50)

    # 2. Get user choice
    while True:
        choice_id = input("  > Enter Player ID to DROP (or 'cancel'): ").strip()
        if choice_id.lower() == "cancel":
            print("  Drop operation cancelled.")
            return

        player_to_drop = id_map.get(choice_id)
        if not player_to_drop:
            print(f"  Invalid ID '{choice_id}'. Please try again.")
            continue

        # 3. Confirm and execute drop
        confirm_res = input(
            f"  Really drop {player_to_drop.player_name}? (y/N): "
        ).lower()

        if confirm_res in ["y", "yes"]:
            player_to_drop.fantasy_team_id = None
            session.add(player_to_drop)
            session.commit()
            print(
                f"  {player_to_drop.player_name} has been dropped and is now a Free Agent."
            )
            return
        else:
            print("  Player not dropped.")
            return


def edit_team_menu(session: Session, team: FantasyTeam):
    """
    Shows the menu for editing a single team.
    """
    while True:
        print("\n" + "=" * 50)
        print(f"Editing: {team.team_name} ({team.owner_name})")
        print("=" * 50)

        # Show roster count
        roster_count = session.exec(
            select(func.count(ProPlayers.player_id)).where(
                ProPlayers.fantasy_team_id == team.team_id
            )
        ).one()
        print(f"Current Roster Size: {roster_count} players")

        print("\n  (A)dd Player (from Free Agents)")
        print("  (D)rop Player")
        print("  (V)iew Roster")
        print("  (B)ack to main menu")

        choice = input("\n> ").strip().lower()

        if choice == "a":
            print("Searching for Free Agents to add...")
            player_to_add = find_player_interactive(session, free_agents_only=True)
            if player_to_add:
                # This check is redundant if free_agents_only=True, but it's safe.
                if player_to_add.fantasy_team_id is not None:
                    print(f"ERROR: {player_to_add.player_name} is already on a team!")
                else:
                    player_to_add.fantasy_team_id = team.team_id
                    session.add(player_to_add)
                    session.commit()
                    print(f"  Added {player_to_add.player_name} to {team.team_name}.")

        elif choice == "d":
            drop_player_interactive(session, team)

        elif choice == "v":
            roster = session.exec(
                select(ProPlayers)
                .where(ProPlayers.fantasy_team_id == team.team_id)
                .order_by(ProPlayers.player_name)
            ).all()
            if not roster:
                print("  This team has no players.")
            else:
                print("\n" + "-" * 60)
                print(f"Current Roster for {team.team_name}:")
                print(f"  {'ID':<12} | {'Name':<25} | {'Pos':<5} | {'NHL Team':<5}")
                print("  " + "-" * 60)
                for player in roster:
                    print(
                        f"  {player.player_id:<12} | {player.player_name:<25} | {player.position:<5} | {player.team_abbrev:<5}"
                    )

        elif choice == "b":
            print("Returning to main menu...")
            return

        else:
            print("Invalid choice, please try again.")


# --- HEAVILY MODIFIED FUNCTION ---
def show_main_menu(session: Session):
    """
    Main menu for managing fantasy teams.
    """
    while True:
        print("\n--- Fantasy Team Manager ---")
        print("Teams are already configured.")
        print("\nWhat would you like to do?")
        print("  (E)dit a team's roster (Add/Drop)")
        print("  (T)rade Players")
        print("  (V)iew all teams and roster counts")
        print("  (Q)uit")

        choice = input("\n> ").strip().lower()

        if choice == "e":
            # 1. List teams and ask user to pick
            teams = session.exec(
                select(FantasyTeam).order_by(FantasyTeam.team_name)
            ).all()
            if not teams:
                print("No teams found. This shouldn't happen.")
                return

            print("\nWhich team do you want to edit?")
            team_map = {}
            for i, team in enumerate(teams, 1):
                team_map[str(i)] = team
                print(f"  {i}. {team.team_name} ({team.owner_name})")

            print("  (C)ancel")

            while True:
                team_choice = input("\n> Select team #: ").strip()
                if team_choice.lower() == "c":
                    break

                chosen_team = team_map.get(team_choice)
                if chosen_team:
                    edit_team_menu(session, chosen_team)
                    break  # Break back to main menu
                else:
                    print("Invalid selection.")

        # --- NEW OPTION ---
        elif choice == "t":
            execute_trade_interactive(session)

        elif choice == "v":
            # View all teams and roster counts
            print("\n" + "=" * 60)
            print("All Fantasy Teams & Roster Counts")
            print("=" * 60)

            statement = (
                select(
                    FantasyTeam.team_name,
                    FantasyTeam.owner_name,
                    func.count(ProPlayers.player_id).label("roster_count"),
                )
                .join(
                    ProPlayers,
                    FantasyTeam.team_id == ProPlayers.fantasy_team_id,
                    isouter=True,  # Use LEFT JOIN to include teams with 0 players
                )
                .group_by(FantasyTeam.team_id)
                .order_by(FantasyTeam.team_name)
            )

            teams_with_counts = session.exec(statement).all()

            if not teams_with_counts:
                print("No teams found in database.")
            else:
                print(f"  {'Team Name':<30} | {'Owner':<20} | {'Players':<5}")
                print("  " + "-" * 60)
                for team in teams_with_counts:
                    print(
                        f"  {team.team_name:<30} | {team.owner_name:<20} | {team.roster_count:<5}"
                    )

        elif choice == "q":
            print("Exiting.")
            sys.exit(0)

        else:
            print("Invalid choice, please try again.")


def main(should_force_refresh, auto_confirm):
    refreshed = False
    with Session(engine) as session:
        if should_force_refresh:
            refreshed = force_refresh(auto_confirm=auto_confirm, session=session)
        else:
            if auto_confirm:
                print("Note: -y/--yes flag has no effect without --force_refresh.")

        teams = session.exec(select(FantasyTeam)).all()

        if (refreshed and len(teams) == 0) or (len(teams) == 0):
            # Table is empty, start first-time setup
            add_teams(session)
        else:
            # Teams exist, show the main menu
            show_main_menu(session)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update fantasy teams.")

    parser.add_argument(
        "--force_refresh",
        action="store_true",
        help="Clear and re-build all fantasy teams.",
    )

    # --- FIX: Added the missing -y/--yes argument ---
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Auto confirms action (e.g., force refresh).",
    )

    args = parser.parse_args()
    main(should_force_refresh=args.force_refresh, auto_confirm=args.yes)
