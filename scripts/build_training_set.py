"""
build_training_set.py

PHASE 1: FEATURE ENGINEERING

Creates the core training dataset (data/ml_training_set.csv) by:
1. Loading all player_game_stats and goalie_game_stats.
2. Calculating all time-dependent features (rolling averages, days rest).
3. Merging with ProPlayers for reputation stats.
4. Calculating opponent strength features.

Each row in the final CSV represents a single player's performance in a game,
along with all the data available *before* that game started.
"""

import pandas as pd
import numpy as np
from sqlmodel import Session, text
from src.database.database import engine
from src.core.constants import SEASON_ID, GOALIE_POSITIONS


def calculate_opponent_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates opponent strength based on points allowed per game.

    Args:
        df: DataFrame containing all game logs (skaters or goalies).

    Returns:
        DataFrame with aggregated opponent stats.
    """
    # Group by opponent and calculate stats allowed
    opponent_stats = (
        df.groupby("opponent_abbrev")
        .agg(
            opp_games_played=("game_id", "count"),
            opp_avg_fpts_allowed=("total_fpts", "mean"),
            opp_avg_goals_allowed=("goals", "mean"),
            opp_avg_shots_allowed=("shots", "mean"),
        )
        .reset_index()
    )

    return opponent_stats.rename(columns={"opponent_abbrev": "team_abbrev"})


def build_training_set():
    print("=" * 70)
    print("PHASE 1: BUILDING ML TRAINING DATASET")
    print("=" * 70)

    # --- 1. Load All Game Stats and ProPlayers ---
    # We load all data (2024 and 2025) needed to calculate features.

    SQL_LOAD_SKATERS = text(
        """
        SELECT 
            ps.player_id, ps.game_date, ps.game_id, ps.total_fpts, ps.goals, ps.assists, 
            ps.shots, ps.hits, ps.blocked_shots, ps.toi_seconds, ps.opponent_abbrev, 
            ps.team_abbrev, ps.season,
            pp.position, pp.prior_season_avg_fpts, pp.prior_season_games_played
        FROM player_game_stats ps
        JOIN pro_players pp ON ps.player_id = pp.player_id
        ORDER BY ps.player_id, ps.game_date;
    """
    )

    SQL_LOAD_GOALIES = text(
        """
        SELECT 
            gs.player_id, gs.game_date, gs.game_id, gs.total_fpts, gs.wins, gs.goals_against, 
            gs.saves, gs.save_pct, gs.decision, gs.opponent_abbrev, 
            gs.team_abbrev, gs.season,
            pp.position, pp.prior_season_avg_fpts, pp.prior_season_games_played
        FROM goalie_game_stats gs
        JOIN pro_players pp ON gs.player_id = pp.player_id
        ORDER BY gs.player_id, gs.game_date;
    """
    )

    with Session(engine) as session:
        print("1. Loading all Skater and Goalie game logs...")
        df_skaters = pd.read_sql(SQL_LOAD_SKATERS, session.connection())
        df_goalies = pd.read_sql(SQL_LOAD_GOALIES, session.connection())

    # Combine for unified processing (important for calculating opponent stats)
    df_skaters["is_goalie"] = 0
    df_goalies["is_goalie"] = 1

    # We rename columns to standardize them before calculating team strength
    df_goalies = df_goalies.rename(
        columns={
            "wins": "goals",  # Dummy variable to allow aggregation
            "goals_against": "assists",  # Dummy variable to allow aggregation
            "saves": "shots",  # Dummy variable to allow aggregation
        }
    )

    # Use only essential columns for general feature generation
    df_goalies_essential = df_goalies[
        [
            "player_id",
            "game_date",
            "game_id",
            "total_fpts",
            "opponent_abbrev",
            "team_abbrev",
            "season",
            "position",
            "is_goalie",
            "prior_season_avg_fpts",
            "prior_season_games_played",
        ]
    ]
    df_skaters_essential = df_skaters[
        [
            "player_id",
            "game_date",
            "game_id",
            "total_fpts",
            "opponent_abbrev",
            "team_abbrev",
            "season",
            "position",
            "is_goalie",
            "prior_season_avg_fpts",
            "prior_season_games_played",
        ]
    ]

    df_combined = pd.concat(
        [df_skaters_essential, df_goalies_essential], ignore_index=True
    )
    df_combined["game_date"] = pd.to_datetime(df_combined["game_date"])
    df_combined = df_combined.sort_values(by=["player_id", "game_date"]).reset_index(
        drop=True
    )

    # --- 2. Feature Calculation (Rollings & Lags) ---
    print("2. Calculating time-series features (Rolling Averages, Days Rest)...")

    # Group by player and apply rolling functions
    grouped = df_combined.groupby("player_id")

    # Rolling Averages (Previous 3 and 5 games)
    # The .shift(1) is CRITICAL: It ensures the feature for Game X only includes the average up to Game X-1 (no data leakage).
    df_combined["avg_fpts_L3"] = grouped["total_fpts"].transform(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    df_combined["avg_fpts_L5"] = grouped["total_fpts"].transform(
        lambda x: x.shift(1).rolling(window=5, min_periods=1).mean()
    )

    # Days Rest (Feature for Back-to-Backs)
    df_combined["prev_game_date"] = grouped["game_date"].shift(1)
    # Using the date_diff_days from your codebase (or pandas difference if available)
    df_combined["days_rest"] = (
        df_combined["game_date"] - df_combined["prev_game_date"]
    ).dt.days.fillna(99)
    df_combined["is_b2b"] = df_combined["days_rest"].apply(lambda x: 1 if x == 1 else 0)

    # --- 3. Opponent Strength ---
    print("3. Calculating opponent strength features...")

    # For simplicity, calculate opponent difficulty based on all *skater* data
    df_opp_stats = calculate_opponent_stats(df_skaters)

    # Merge opponent data. We merge by opponent_abbrev but rename the column
    # to team_abbrev temporarily to make the join
    df_combined = df_combined.merge(
        df_opp_stats,
        left_on="opponent_abbrev",
        right_on="team_abbrev",
        how="left",
        suffixes=("_player", "_opp"),
    )

    # Drop the redundant team_abbrev_opp column
    df_combined = df_combined.drop(columns=["team_abbrev_opp"])

    # --- 4. Final Feature Cleanup ---
    print("4. Finalizing dataset and encoding categorical features...")

    # Is Home Game
    df_combined["is_home_game"] = (
        df_combined["team_abbrev_player"] == df_combined["opponent_abbrev"]
    )
    df_combined["is_home_game"] = df_combined["is_home_game"].astype(int)

    # One-Hot Encode Position
    # This turns 'C', 'D', 'G' into columns like 'pos_C', 'pos_D', etc.
    df_combined = pd.get_dummies(
        df_combined, columns=["position"], prefix="pos", dummy_na=False
    )

    # --- 5. Save Final Dataset ---

    # Rename the target column
    df_combined = df_combined.rename(columns={"total_fpts": "target_fpts"})

    # Define the final features and target columns for the ML model
    final_features = [
        "player_id",
        "game_id",
        "game_date",
        "team_abbrev_player",
        "opponent_abbrev",  # Identifiers
        "prior_season_avg_fpts",
        "prior_season_games_played",  # Reputation
        "avg_fpts_L3",
        "avg_fpts_L5",  # Recent Form
        "is_b2b",
        "days_rest",
        "is_home_game",  # Context
        "opp_avg_fpts_allowed",
        "opp_avg_goals_allowed",  # Opponent Strength
        "pos_C",
        "pos_D",
        "pos_L",
        "pos_R",
        "pos_Goalie",  # Position Encoded
        "target_fpts",  # Target
    ]

    # Filter and drop NaNs (initial games without L3 or L5 averages, and players with no prior season data)
    df_final = df_combined[final_features].dropna(subset=["avg_fpts_L5"]).fillna(0)

    # Save to CSV
    output_path = "data/ml_training_set.csv"
    df_final.to_csv(output_path, index=False)

    print("\n✅ Dataset generation complete!")
    print(f"File saved to: {output_path}")
    print(f"Total rows (games): {len(df_final)}")
    print(f"Features: {list(df_final.columns)}")


if __name__ == "__main__":
    # Ensure pandas is installed: pip install pandas
    try:
        build_training_set()
    except Exception as e:
        print(f"\n--- ERROR ---")
        print("Database connection or feature calculation failed.")
        print(
            "Please ensure your 'requirements.txt' includes 'pandas' and your database is up to date."
        )
        print(f"Error: {e}")
