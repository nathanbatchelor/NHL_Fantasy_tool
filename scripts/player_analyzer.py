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
