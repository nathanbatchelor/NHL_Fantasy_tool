"""
player_analyzer.py

A script to run a full fantasy analysis on a single player for the
current fantasy week, pulling all data from the local database.

Usage:
  python scripts/player_analyzer.py
"""

import sys
import pandas as pd
import pytz
from datetime import datetime
from sqlmodel import Session, select, func, text
from src.database.utils import find_player_interactive
from src.database.database import engine
from src.database.models import (
    ProPlayers,
    PlayerGameStats,
    GoalieGameStats,
    TeamSchedule,
)
from src.core.constants import SEASON_ID, FANTASY_TIMEZONE
from src.utils.date_utils import get_fantasy_week


# --- SQL Query Templates (Skater) ---

QUERY_SKATER_OPPONENT_DIFFICULTY = text(
    """
    SELECT
      opponent_abbrev,
      AVG(total_fpts) AS avg_fpts_allowed
    FROM
      player_game_stats
    WHERE
      season = :current_season
    GROUP BY
      opponent_abbrev
    ORDER BY
      avg_fpts_allowed DESC;
    """
)

QUERY_SKATER_BASELINE = text(
    """
    SELECT
      pp.player_name,
      COUNT(ps.game_id) AS games_played,
      AVG(ps.total_fpts) AS avg_fpts,
      AVG(ps.goals) AS avg_g,
      AVG(ps.assists) AS avg_a,
      AVG(ps.shots) AS avg_sog,
      AVG(ps.toi_seconds) / 60.0 AS avg_toi_min
    FROM
      player_game_stats ps
    JOIN
      pro_players pp ON ps.player_id = pp.player_id
    WHERE
      ps.player_id = :player_id
      AND ps.season = :current_season
    GROUP BY
      pp.player_name;
    """
)

QUERY_SKATER_FLOOR = text(
    """
    SELECT
      AVG(ps.hits) AS avg_hits,
      AVG(ps.blocked_shots) AS avg_blocks,
      -- This assumes your fantasy point settings from constants
      (AVG(ps.hits) * 0.1) + (AVG(ps.blocked_shots) * 0.5) AS avg_peripheral_fpts
    FROM
      player_game_stats ps
    WHERE
      ps.player_id = :player_id
      AND ps.season = :current_season
    GROUP BY
      ps.player_id;
    """
)

QUERY_SKATER_RECENT_FORM = text(
    """
    SELECT
      COUNT(ps.game_id) AS games_last_14_days,
      AVG(ps.total_fpts) AS avg_fpts_last_14_days
    FROM
      player_game_stats ps
    WHERE
      ps.game_date >= date('now', '-14 days')
      AND ps.player_id = :player_id
      AND ps.season = :current_season
    GROUP BY
      ps.player_id;
    """
)

QUERY_SKATER_HISTORY = text(
    """
    SELECT
      opponent_abbrev,
      COUNT(game_id) AS games_against,
      AVG(total_fpts) AS avg_fpts_against
    FROM
      player_game_stats
    WHERE
      player_id = :player_id
    GROUP BY
      opponent_abbrev
    ORDER BY
      avg_fpts_against DESC;
    """
)

# --- SQL Query Templates (Goalie) ---

QUERY_GOALIE_OPPONENT_DIFFICULTY = text(
    """
    SELECT
      opponent_abbrev,
      -- Ranks teams by how many fpts they give up TO GOALIES
      -- Good teams that shoot a lot and score little are high
      AVG(total_fpts) AS avg_fpts_allowed_to_goalies
    FROM
      goalie_game_stats
    WHERE
      season = :current_season
    GROUP BY
      opponent_abbrev
    ORDER BY
      avg_fpts_allowed_to_goalies DESC;
    """
)

QUERY_GOALIE_BASELINE = text(
    """
    SELECT
      pp.player_name,
      COUNT(gs.game_id) AS games_played,
      AVG(gs.total_fpts) AS avg_fpts,
      AVG(gs.saves) AS avg_saves,
      AVG(gs.save_pct) AS avg_save_pct,
      SUM(gs.wins) AS total_wins,
      SUM(gs.shutouts) AS total_shutouts
    FROM
      goalie_game_stats gs
    JOIN
      pro_players pp ON gs.player_id = pp.player_id
    WHERE
      gs.player_id = :player_id
      AND gs.season = :current_season
    GROUP BY
      pp.player_name;
    """
)

QUERY_GOALIE_RECENT_FORM = text(
    """
    SELECT
      COUNT(gs.game_id) AS games_last_14_days,
      AVG(gs.total_fpts) AS avg_fpts_last_14_days,
      AVG(gs.save_pct) AS save_pct_last_14_days
    FROM
      goalie_game_stats gs
    WHERE
      gs.game_date >= date('now', '-14 days')
      AND gs.player_id = :player_id
      AND gs.season = :current_season
    GROUP BY
      gs.player_id;
    """
)

QUERY_GOALIE_HISTORY = text(
    """
    SELECT
      opponent_abbrev,
      COUNT(game_id) AS games_against,
      AVG(total_fpts) AS avg_fpts_against,
      AVG(save_pct) AS avg_save_pct_against
    FROM
      goalie_game_stats
    WHERE
      player_id = :player_id
    GROUP BY
      opponent_abbrev
    ORDER BY
      avg_fpts_against DESC;
    """
)


def print_header(title):
    """Helper function to print clean section headers."""
    print("\n" + "=" * 60)
    print(f" {title.upper()}")
    print("=" * 60)



def main():
    with Session(engine) as session:
        # --- 0. Get Player to Analyze ---
        player = find_player_interactive(session)
        if not player:
            print("Exiting.")
            sys.exit(0)

        print_header(
            f"ANALYSIS FOR: {player.player_name} ({player.team_abbrev} - {player.position})"
        )

        # --- 1. Get Opponent Difficulty Rankings ---
        # We get both, and use the one we need later
        params_season = {"current_season": SEASON_ID}
        df_skater_diff = pd.read_sql(
            QUERY_SKATER_OPPONENT_DIFFICULTY, session.connection(), params=params_season
        )
        df_skater_diff["rank"] = (
            df_skater_diff["avg_fpts_allowed"].rank(ascending=False).astype(int)
        )
        skater_diff_dict = df_skater_diff.set_index("opponent_abbrev").to_dict("index")

        df_goalie_diff = pd.read_sql(
            QUERY_GOALIE_OPPONENT_DIFFICULTY, session.connection(), params=params_season
        )
        df_goalie_diff["rank"] = (
            df_goalie_diff["avg_fpts_allowed_to_goalies"]
            .rank(ascending=False)
            .astype(int)
        )
        goalie_diff_dict = df_goalie_diff.set_index("opponent_abbrev").to_dict("index")

        # --- 2. Get This Week's Schedule (from DB) ---
        tz = pytz.timezone(FANTASY_TIMEZONE)
        today = datetime.now(tz)
        year, week_num = get_fantasy_week(today.isoformat())
        current_week_key = f"{year}-W{week_num:02d}"

        statement = select(TeamSchedule).where(
            TeamSchedule.team == player.team_abbrev,
            TeamSchedule.week == current_week_key,
        )
        week_schedule = session.exec(statement).first()

        print_header(f"THIS WEEK'S SCHEDULE ({current_week_key})")
        if not week_schedule:
            print(f"  No schedule found for {player.team_abbrev} for this week.")
        else:
            print(f"  {player.team_abbrev} ({week_schedule.game_count} games)")
            opponents = week_schedule.opponents.split(", ")
            for opp_str in opponents:
                # opp_str is like 'vs WSH' or '@ BOS'
                team_code = opp_str.split(" ")[-1]

                # Use the correct difficulty ranking
                if player.is_goalie:
                    diff_dict = goalie_diff_dict
                    key = "avg_fpts_allowed_to_goalies"
                else:
                    diff_dict = skater_diff_dict
                    key = "avg_fpts_allowed"

                if team_code in diff_dict:
                    rank = diff_dict[team_code]["rank"]
                    fpts = diff_dict[team_code][key]
                    if rank <= 10:
                        difficulty = "Easy"
                    elif rank >= 23:
                        difficulty = "Hard"
                    else:
                        difficulty = "Average"
                    print(
                        f"    - {opp_str:<10} (Matchup: {difficulty} - {rank}th, allows {fpts:.2f} Fpts)"
                    )
                else:
                    print(f"    - {opp_str:<10} (No difficulty data)")

        # --- 3. Run Player-Specific Queries ---
        params_player = {"player_id": player.player_id, "current_season": SEASON_ID}

        if player.is_goalie:
            # --- Run GOALIE Analysis ---
            print_header(f"SEASON BASELINE (GOALIE) - {SEASON_ID}")
            df_baseline = pd.read_sql(
                QUERY_GOALIE_BASELINE, session.connection(), params=params_player
            )
            print(df_baseline.to_string(index=False))

            print_header(f"RECENT FORM (LAST 14 DAYS) (GOALIE) - {SEASON_ID}")
            df_recent = pd.read_sql(
                QUERY_GOALIE_RECENT_FORM, session.connection(), params=params_player
            )
            if df_recent.empty:
                print("  No games played in the last 14 days.")
            else:
                print(df_recent.to_string(index=False))

            print_header("PERFORMANCE HISTORY VS. ALL TEAMS (GOALIE) - ALL SEASONS")
            df_history = pd.read_sql(
                QUERY_GOALIE_HISTORY,
                session.connection(),
                params={"player_id": player.player_id},
            )
            if df_history.empty:
                print("  No game history found for this player.")
            else:
                print(df_history.to_string(index=False))

        else:
            # --- Run SKATER Analysis ---
            print_header(f"SEASON BASELINE (SKATER) - {SEASON_ID}")
            df_baseline = pd.read_sql(
                QUERY_SKATER_BASELINE, session.connection(), params=params_player
            )
            print(df_baseline.to_string(index=False))

            print_header(f"PERIPHERAL 'FLOOR' (SKATER) - {SEASON_ID}")
            df_floor = pd.read_sql(
                QUERY_SKATER_FLOOR, session.connection(), params=params_player
            )
            print(df_floor.to_string(index=False))

            print_header(f"RECENT FORM (LAST 14 DAYS) (SKATER) - {SEASON_ID}")
            df_recent = pd.read_sql(
                QUERY_SKATER_RECENT_FORM, session.connection(), params=params_player
            )
            if df_recent.empty:
                print("  No games played in the last 14 days.")
            else:
                print(df_recent.to_string(index=False))

            print_header("PERFORMANCE HISTORY VS. ALL TEAMS (SKATER) - ALL SEASONS")
            df_history = pd.read_sql(
                QUERY_SKATER_HISTORY,
                session.connection(),
                params={"player_id": player.player_id},
            )
            if df_history.empty:
                print("  No game history found for this player.")
            else:
                print(df_history.to_string(index=False))

    print("\nAnalysis complete.\n")


if __name__ == "__main__":
    main()
