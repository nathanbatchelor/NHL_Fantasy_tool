"""
run_predictions.py

PHASE 3: PREDICTION AND INTEGRATION

1. Loads the trained ML model (fpts_predictor.pkl).
2. Identifies all skaters (NO GOALIES) playing on the TARGET DATE (User Input or Tomorrow).
3. FILTERS: Only includes players who have actually played a game in the current season.
4. Calculates the required features for those players using up-to-the-minute DB data.
5. Uses the model to predict their fantasy score for that game.
6. Updates the 'predicted_fpts' column in the ProPlayers table.
7. Prints filtered results for Team 1 and Free Agents.
"""

import pandas as pd
import numpy as np
import joblib
import asyncio
import pytz
from datetime import datetime, timedelta
from sqlmodel import Session, select, text
from src.database.database import engine, init_db
from src.database.models import ProPlayers, PlayerGameStats
from src.core.constants import SEASON_ID, FANTASY_TIMEZONE
from src.api.nhl_api_utils import get_schedule
from src.utils.date_utils import get_schedule_by_date


def get_player_recent_games(
    session: Session, player_id: int, limit: int = 10
) -> pd.DataFrame:
    """
    Fetches the most recent games for a player (up to limit).
    Returns a DataFrame with game stats.
    """
    SQL_RECENT_GAMES = text(
        """
        SELECT 
            player_id, game_date, total_fpts, goals, assists, shots
        FROM player_game_stats
        WHERE player_id = :player_id
        AND season = :season
        ORDER BY game_date DESC
        LIMIT :limit
        """
    )

    df = pd.read_sql(
        SQL_RECENT_GAMES,
        session.connection(),
        params={"player_id": player_id, "season": SEASON_ID, "limit": limit},
    )
    return df


def calculate_opponent_strength(session: Session) -> pd.DataFrame:
    """
    Calculate opponent strength metrics based on current season data.
    Returns DataFrame with opponent_abbrev as key.
    """
    SQL_OPP_STATS = text(
        """
        SELECT 
            opponent_abbrev,
            COUNT(game_id) AS opp_games_played,
            AVG(total_fpts) AS opp_avg_fpts_allowed,
            AVG(goals) AS opp_avg_goals_allowed,
            AVG(shots) AS opp_avg_shots_allowed
        FROM player_game_stats
        WHERE season = :season
        GROUP BY opponent_abbrev
        """
    )

    df = pd.read_sql(SQL_OPP_STATS, session.connection(), params={"season": SEASON_ID})
    return df


def generate_prediction_features_for_player(
    player: ProPlayers,
    recent_games_df: pd.DataFrame,
    opponent_abbrev: str,
    is_home: bool,
    opp_stats_df: pd.DataFrame,
    days_since_last_game: int,
) -> pd.DataFrame:
    """
    Generates a single-row feature DataFrame for one player's upcoming game.
    """
    # Calculate rolling averages from recent games
    recent_games_sorted = recent_games_df.sort_values("game_date")

    avg_fpts_L3 = (
        recent_games_sorted["total_fpts"].tail(3).mean()
        if len(recent_games_sorted) >= 1
        else 0
    )
    avg_fpts_L5 = (
        recent_games_sorted["total_fpts"].tail(5).mean()
        if len(recent_games_sorted) >= 1
        else 0
    )

    # Days rest and back-to-back
    days_rest = days_since_last_game
    is_b2b = 1 if days_rest == 1 else 0

    # Opponent stats
    opp_row = opp_stats_df[opp_stats_df["opponent_abbrev"] == opponent_abbrev]
    if len(opp_row) > 0:
        opp_avg_fpts_allowed = opp_row["opp_avg_fpts_allowed"].iloc[0]
        opp_avg_goals_allowed = opp_row["opp_avg_goals_allowed"].iloc[0]
    else:
        opp_avg_fpts_allowed = 0
        opp_avg_goals_allowed = 0

    # Position encoding (one-hot)
    position = player.position if player.position else "C"
    pos_C = 1 if position == "C" else 0
    pos_D = 1 if position == "D" else 0
    pos_L = 1 if position == "L" else 0
    pos_R = 1 if position == "R" else 0
    pos_Goalie = 1 if position == "Goalie" else 0

    features = {
        "player_id": player.player_id,
        "prior_season_avg_fpts": player.prior_season_avg_fpts or 0,
        "prior_season_games_played": player.prior_season_games_played or 0,
        "avg_fpts_L3": avg_fpts_L3,
        "avg_fpts_L5": avg_fpts_L5,
        "is_b2b": is_b2b,
        "days_rest": days_rest,
        "is_home_game": 1 if is_home else 0,
        "opp_avg_fpts_allowed": opp_avg_fpts_allowed,
        "opp_avg_goals_allowed": opp_avg_goals_allowed,
        "pos_C": pos_C,
        "pos_D": pos_D,
        "pos_L": pos_L,
        "pos_R": pos_R,
        "pos_Goalie": pos_Goalie,
    }

    return pd.DataFrame([features])


async def main():
    init_db()

    print("=" * 70)
    print("PHASE 3: RUNNING ML PREDICTIONS (SKATERS ONLY)")
    print("=" * 70)

    try:
        model = joblib.load("models/fpts_predictor.pkl")
        print("1. ✅ Successfully loaded ML model.")
    except FileNotFoundError:
        print("ERROR: Model not found at 'models/fpts_predictor.pkl'.")
        print("Run train_model.py first.")
        return

    # --- NEW: Date Selection Logic ---
    tz = pytz.timezone(FANTASY_TIMEZONE)

    # Determine tomorrow's date as the default
    default_date_obj = datetime.now(tz) + timedelta(days=1)
    default_date_str = default_date_obj.strftime("%Y-%m-%d")

    # Prompt user for input
    print(f"\nDefault prediction date is TOMORROW ({default_date_str}).")
    user_date_input = input(
        "Enter target date (YYYY-MM-DD) or press Enter to use default: "
    ).strip()

    target_date_obj = None
    target_date_str = ""

    if not user_date_input:
        # User pressed Enter, use default
        target_date_obj = default_date_obj
        target_date_str = default_date_str
    else:
        # Try to parse user input
        try:
            # Parse naive date
            dt_naive = datetime.strptime(user_date_input, "%Y-%m-%d")
            # Localize it (set to noon to avoid boundary issues)
            target_date_obj = tz.localize(dt_naive.replace(hour=12))
            target_date_str = user_date_input
        except ValueError:
            print("❌ Invalid date format. Please use YYYY-MM-DD.")
            return

    print(f"2. Target Date for Prediction: {target_date_str}")

    # Get schedule
    schedule_by_id = await get_schedule()
    schedule_by_date = get_schedule_by_date(schedule_by_id)
    target_games = schedule_by_date.get(target_date_str, [])

    if not target_games:
        print(f"No games scheduled for {target_date_str}. Prediction halted.")
        return

    # Build a map of team -> opponent and home/away status
    game_info_map = {}  # {team_abbrev: (opponent_abbrev, is_home)}

    for game in target_games:
        home_team = game["home_abbrev"]
        away_team = game["away_abbrev"]
        game_info_map[home_team] = (away_team, True)
        game_info_map[away_team] = (home_team, False)

    teams_playing = set(game_info_map.keys())

    print(
        f"3. Found {len(target_games)} games with {len(teams_playing)} teams playing."
    )

    with Session(engine) as session:
        # -----------------------------------------------------------------
        # SUBQUERY: Identify players who have played at least 1 game in 20252026
        # -----------------------------------------------------------------
        players_with_games_subquery = select(PlayerGameStats.player_id).where(
            PlayerGameStats.season == SEASON_ID
        )

        # Get all active players on teams playing on target date
        # FILTERS:
        # 1. Team is playing
        # 2. Player is active
        # 3. NOT a Goalie
        # 4. Has played at least one game this season (in subquery)
        players_playing = session.exec(
            select(ProPlayers).where(
                ProPlayers.team_abbrev.in_(teams_playing),
                ProPlayers.is_active == True,
                ProPlayers.position != "Goalie",
                ProPlayers.position != "G",
                # ProPlayers.position != "D",
                ProPlayers.player_id.in_(players_with_games_subquery),
            )
        ).all()

        if not players_playing:
            print(
                f"Could not find any active skaters with {SEASON_ID} stats for {target_date_str}."
            )
            return

        print(f"4. Generating features for {len(players_playing)} active skaters...")

        # Pre-calculate opponent strength stats (one query for all)
        opp_stats_df = calculate_opponent_strength(session)

        # Store all feature rows
        all_features = []
        player_id_list = []

        for player in players_playing:
            # Get game context for this player
            opponent_abbrev, is_home = game_info_map.get(
                player.team_abbrev, (None, False)
            )

            if not opponent_abbrev:
                continue

            # Get recent games for rolling averages
            recent_games_df = get_player_recent_games(
                session, player.player_id, limit=10
            )

            # Calculate days since last game using TARGET DATE
            if len(recent_games_df) > 0:
                last_game_date = pd.to_datetime(recent_games_df["game_date"].iloc[0])
                days_since_last = (target_date_obj.date() - last_game_date.date()).days
            else:
                days_since_last = 99

            # Generate features for this player
            player_features = generate_prediction_features_for_player(
                player=player,
                recent_games_df=recent_games_df,
                opponent_abbrev=opponent_abbrev,
                is_home=is_home,
                opp_stats_df=opp_stats_df,
                days_since_last_game=days_since_last,
            )

            all_features.append(player_features)
            player_id_list.append(player.player_id)

        if not all_features:
            print("No valid features generated. Exiting.")
            return

        # Combine all feature rows
        X_predict = pd.concat(all_features, ignore_index=True)

        # Extract player_id before dropping it
        player_ids = X_predict["player_id"].values

        # Drop player_id for prediction (model doesn't use it)
        X_predict = X_predict.drop(columns=["player_id"])

        # Ensure column order matches training
        feature_columns = [
            "prior_season_avg_fpts",
            "prior_season_games_played",
            "avg_fpts_L3",
            "avg_fpts_L5",
            "is_b2b",
            "days_rest",
            "is_home_game",
            "opp_avg_fpts_allowed",
            "opp_avg_goals_allowed",
            "pos_C",
            "pos_D",
            "pos_L",
            "pos_R",
            "pos_Goalie",
        ]

        X_predict = X_predict[feature_columns]

        print("5. Running predictions...")
        predictions = model.predict(X_predict)

        # Clip negative predictions to zero and round
        predictions = np.clip(predictions, a_min=0, a_max=None)
        predictions = np.round(predictions, 2)

        print("6. Updating ProPlayers table with predicted scores...")

        # Create prediction map
        prediction_map = dict(zip(player_ids, predictions))

        # Update database
        updated_count = 0
        for player in players_playing:
            predicted_score = prediction_map.get(player.player_id)
            if predicted_score is not None:
                player.predicted_fpts = predicted_score
                session.add(player)
                updated_count += 1

        session.commit()
        print(f"   ✅ Successfully updated predicted_fpts for {updated_count} players.")

        # ---------------------------------------------------------------------
        # FILTERED OUTPUT: Team 1 and Free Agents
        # ---------------------------------------------------------------------
        print(f"\n📊 Top Predicted Free Agents & Team 1 Players for {target_date_str}:")
        print("-" * 70)
        print(f"{'Rank':<5} {'Player Name':<25} {'Team':<5} {'FPTS':<6} {'Status':<12}")
        print("-" * 70)

        # Sort all predictions by score DESC
        sorted_predictions = sorted(
            prediction_map.items(), key=lambda item: item[1], reverse=True
        )

        rank = 1
        for player_id, predicted_score in sorted_predictions:
            player = session.get(ProPlayers, player_id)
            if player:
                # Filter: Show only if Fantasy Team is 1 OR None (Free Agent)
                # Using fantasy_team_id integer check
                if player.fantasy_team_id is None or player.fantasy_team_id == 1:
                    status = "Team 1" if player.fantasy_team_id == 1 else "Free Agent"

                    print(
                        f"{rank:<5} {player.player_name:<25} {player.team_abbrev:<5} {predicted_score:<6.2f} {status:<12}"
                    )
                    rank += 1

        if rank == 1:
            print(
                f"No players from Team 1 or Free Agents playing on {target_date_str}."
            )
        print("-" * 70)

    print("\n✅ Prediction Run Complete.")


if __name__ == "__main__":
    asyncio.run(main())
