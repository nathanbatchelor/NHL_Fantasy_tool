"""
run_predictions.py

PHASE 3: PREDICTION AND INTEGRATION

1. Loads the trained ML model (fpts_predictor.pkl).
2. Identifies all players playing tomorrow.
3. Calculates the required features for those players using up-to-the-minute DB data.
4. Uses the model to predict their fantasy score for that game.
5. Updates the 'predicted_fpts' column in the ProPlayers table.
"""

import pandas as pd
import numpy as np
import joblib
import asyncio
import pytz
from datetime import datetime, timedelta
from sqlmodel import Session, select, func, text
from src.database.database import engine, init_db
from src.database.models import ProPlayers
from src.core.constants import SEASON_ID, FANTASY_TIMEZONE
from src.api.nhl_api_utils import get_schedule
from src.utils.date_utils import get_schedule_by_date


def get_feature_data(session: Session, target_date_str: str) -> pd.DataFrame:
    """
    Fetches the necessary data (player context and game stats) to build features.
    We need data *up to the day before* the prediction date.
    """

    # We must use the same features logic as build_training_set.py
    # This query retrieves all necessary data for feature calculation (skaters only for now).
    # It excludes the prediction date itself, but includes games up to yesterday.

    SQL_PREDICTION_FEATURES = text(
        """
        SELECT 
            ps.player_id, ps.game_date, ps.total_fpts, ps.goals, ps.assists, ps.shots, 
            ps.toi_seconds, ps.opponent_abbrev, ps.team_abbrev,
            pp.position, pp.prior_season_avg_fpts, pp.prior_season_games_played
        FROM player_game_stats ps
        JOIN pro_players pp ON ps.player_id = pp.player_id
        WHERE ps.game_date < :target_date
        ORDER BY ps.player_id, ps.game_date;
    """
    )

    df = pd.read_sql(
        SQL_PREDICTION_FEATURES,
        session.connection(),
        params={"target_date": target_date_str},
    )
    df["game_date"] = pd.to_datetime(df["game_date"])
    return df


def generate_prediction_features(
    df_history: pd.DataFrame, player_ids: list, today_date: datetime
) -> pd.DataFrame:
    """
    Generates the exact feature matrix needed by the model for players playing today.
    """

    if df_history.empty:
        return pd.DataFrame()

    # --- 1. Calculate Player-Specific Features (Rolling Averages & Context) ---
    grouped = df_history.groupby("player_id")

    # Find the most recent game for each player to calculate Days Rest
    latest_games = grouped["game_date"].max().reset_index()
    latest_games = latest_games[latest_games["player_id"].isin(player_ids)]

    # Calculate Rolling Averages (Last 3 and 5 games) based on the latest data
    # NOTE: These calculations are applied to the full historical data, and we
    # extract the result for the last available game (which is the current "state").

    # This logic is fragile but necessary: we take the LAST value of the rolling window
    df_features = grouped.tail(1).copy()
    df_features["avg_fpts_L3"] = (
        grouped["total_fpts"]
        .transform(lambda x: x.shift(1).rolling(window=3, min_periods=1).mean())
        .tail(1)
    )
    df_features["avg_fpts_L5"] = (
        grouped["total_fpts"]
        .transform(lambda x: x.shift(1).rolling(window=5, min_periods=1).mean())
        .tail(1)
    )

    # --- Days Rest / B2B ---

    # Calculate days since last game (difference between current date and max history date)
    df_features = df_features.merge(
        latest_games, on="player_id", suffixes=("_old", "_latest")
    )
    df_features["days_rest"] = (
        today_date.date() - df_features["game_date_latest"].dt.date
    ).apply(lambda x: x.days)
    df_features["is_b2b"] = df_features["days_rest"].apply(lambda x: 1 if x == 1 else 0)

    # --- 2. Calculate Opponent Strength (Static for this session) ---
    # We must replicate the calculation from build_training_set.py

    # Simple Opponent Strength based on points allowed (skaters only for simplicity)
    opp_stats = (
        df_history.groupby("opponent_abbrev")
        .agg(
            opp_avg_fpts_allowed=("total_fpts", "mean"),
            opp_avg_goals_allowed=("goals", "mean"),
        )
        .reset_index()
        .rename(columns={"opponent_abbrev": "team_abbrev"})
    )

    # --- 3. Final Feature Cleanup (Merge and Encode) ---

    # Merge opponent data onto the feature set (using the player's current opponent)
    # NOTE: We skip this merge for now, as we don't have the opponent ABBREV YET.
    # We will fill opponent_abbrev with the current NHL team as a placeholder for now.

    # Select only the target players
    df_features = df_features[df_features["player_id"].isin(player_ids)].fillna(0)

    # One-Hot Encode Position
    position_dummies = pd.get_dummies(df_features["position"], prefix="pos")
    df_features = pd.concat([df_features, position_dummies], axis=1)

    # Add back the features that must be present, even if empty/zero
    for col in ["pos_C", "pos_D", "pos_L", "pos_R", "pos_Goalie"]:
        if col not in df_features.columns:
            df_features[col] = 0

    # Ensure all required columns are present in the same order as training
    # NOTE: This list MUST match the features used in train_model.py
    required_features = [
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

    # NOTE: Since we are predicting *all* players, not just one game,
    # 'is_home_game' and 'opp_avg_fpts_allowed' cannot be accurately calculated here.
    # We will zero them out for a baseline prediction.

    # Create the final feature matrix
    X_predict = pd.DataFrame(index=df_features.index)

    # Copy essential features
    X_predict["prior_season_avg_fpts"] = df_features["prior_season_avg_fpts"]
    X_predict["prior_season_games_played"] = df_features["prior_season_games_played"]
    X_predict["avg_fpts_L3"] = df_features["avg_fpts_L3"]
    X_predict["avg_fpts_L5"] = df_features["avg_fpts_L5"]
    X_predict["is_b2b"] = df_features["is_b2b"]
    X_predict["days_rest"] = df_features["days_rest"]

    # Add placeholder/zeroed features that will be updated per game later
    X_predict["is_home_game"] = 0
    X_predict["opp_avg_fpts_allowed"] = 0
    X_predict["opp_avg_goals_allowed"] = 0

    # Add encoded position columns
    for col in ["pos_C", "pos_D", "pos_L", "pos_R", "pos_Goalie"]:
        X_predict[col] = df_features[col]

    return X_predict.fillna(0)


async def main():
    init_db()

    print("=" * 70)
    print("PHASE 3: RUNNING ML PREDICTIONS")
    print("=" * 70)

    try:
        # Load the model
        model = joblib.load("models/fpts_predictor.pkl")
        print("1. Successfully loaded ML model.")
    except FileNotFoundError:
        print(
            "ERROR: Model not found at 'models/fpts_predictor.pkl'. Run train_model.py first."
        )
        return

    tz = pytz.timezone(FANTASY_TIMEZONE)
    tomorrow = datetime.now(tz) + timedelta(days=1)
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")

    print(f"2. Target Date for Prediction: {tomorrow_str}")

    # --- 3. Identify Players Playing Tomorrow ---

    # Get tomorrow's games
    schedule_by_id = await get_schedule()
    schedule_by_date = get_schedule_by_date(
        schedule_by_id
    )  # (Assuming this helper exists)
    tomorrows_games = schedule_by_date.get(tomorrow_str, [])

    teams_playing = set()
    if tomorrows_games:
        for game in tomorrows_games:
            teams_playing.add(game["home_abbrev"])
            teams_playing.add(game["away_abbrev"])

    if not teams_playing:
        print("No games scheduled for tomorrow. Prediction halted.")
        return

    # Get all players on those teams
    with Session(engine) as session:
        players_playing = session.exec(
            select(ProPlayers).where(ProPlayers.team_abbrev.in_(teams_playing))
        ).all()

        if not players_playing:
            print("Could not find any active players for tomorrow's teams.")
            return

        player_ids_to_predict = [p.player_id for p in players_playing]
        print(
            f"3. Found {len(players_playing)} players across {len(teams_playing)} teams playing tomorrow."
        )

        # --- 4. Generate Features ---
        df_history = get_feature_data(session, tomorrow_str)

        # This will create a matrix of features in the correct order
        X_predict = generate_prediction_features(
            df_history, player_ids_to_predict, tomorrow
        )

        if X_predict.empty:
            print("Could not generate prediction features. Skipping.")
            return

        # --- 5. Predict Scores ---
        print("4. Running predictions...")
        predictions = model.predict(X_predict)

        # Clip negative predictions to zero
        predictions = np.clip(predictions, a_min=0, a_max=None)

        # Round the predictions to two decimal places
        predictions = np.round(predictions, 2)

        # --- 6. Update Database ---
        print("5. Updating ProPlayers table with predicted scores...")

        # Create a mapping of player_id to predicted_fpts
        prediction_map = dict(zip(X_predict["player_id"], predictions))

        updated_count = 0
        for player in players_playing:
            predicted_score = prediction_map.get(player.player_id)
            if predicted_score is not None:
                player.predicted_fpts = predicted_score
                session.add(player)
                updated_count += 1

        session.commit()
        print(f"   ✅ Successfully updated predicted_fpts for {updated_count} players.")

    print("\nPrediction Run Complete.")


if __name__ == "__main__":
    # Ensure necessary ML libraries are installed: pip install pandas scikit-learn joblib
    asyncio.run(main())
