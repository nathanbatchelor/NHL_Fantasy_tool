"""
utils/transforms.py
Functions for transforming, merging, and calculating data.
"""

from collections import defaultdict
import constants

# --- Player Stats Transforms ---


def combine_and_get_skater_data(
    skater_summary_data: list, skater_realtime_data: list
) -> list:
    """
    Merges skater summary and realtime stats lists and calculates fantasy points.
    """
    realtime_map = {}
    for player_rt in skater_realtime_data:
        realtime_map[player_rt["playerId"]] = player_rt

    combined_stats_list = []
    weights = constants.SKATER_FPTS_WEIGHTS

    for player_summary in skater_summary_data:
        player_id = player_summary["playerId"]
        player_realtime = realtime_map.get(player_id, {})

        # Get all stats, defaulting to 0
        goals = player_summary.get("goals", 0) or 0
        assists = player_summary.get("assists", 0) or 0
        pp_points = player_summary.get("ppPoints", 0) or 0
        sh_points = player_summary.get("shPoints", 0) or 0
        shots = player_summary.get("shots", 0) or 0
        blocks = player_realtime.get("blockedShots", 0) or 0
        hits = player_realtime.get("hits", 0) or 0
        games_played = player_summary.get("gamesPlayed", 1) or 1

        total_fpts = (
            (goals * weights["goals"])
            + (assists * weights["assists"])
            + (pp_points * weights["ppPoints"])
            + (sh_points * weights["shPoints"])
            + (shots * weights["shots"])
            + (blocks * weights["blockedShots"])
            + (hits * weights["hits"])
        )
        avg_fpts = total_fpts / games_played

        combined_player = {
            "playerId": player_id,
            "skaterFullName": player_summary.get("skaterFullName"),
            "teamAbbrevs": player_summary.get("teamAbbrevs"),
            "positionCode": player_summary.get("positionCode"),
            "gamesPlayed": player_summary.get("gamesPlayed"),
            "goals": goals,
            "assists": assists,
            "ppPoints": pp_points,
            "shPoints": sh_points,
            "shots": shots,
            "blockedShots": blocks,
            "hits": hits,
            "Fpts": round(total_fpts, 2),
            "Avg": round(avg_fpts, 2),
        }
        combined_stats_list.append(combined_player)

    return combined_stats_list


def process_goalie_data(goalie_data: list) -> list:
    """
    Filters goalie data and calculates fantasy points.
    """
    filtered_goalies = []
    weights = constants.GOALIE_FPTS_WEIGHTS

    for goalie in goalie_data:
        wins = goalie.get("wins", 0) or 0
        goals_against = goalie.get("goalsAgainst", 0) or 0
        saves = goalie.get("saves", 0) or 0
        shutouts = goalie.get("shutouts", 0) or 0
        ot_losses = goalie.get("otLosses", 0) or 0
        games_played = goalie.get("gamesPlayed", 1) or 1

        total_fpts = (
            (wins * weights["wins"])
            + (goals_against * weights["goalsAgainst"])
            + (saves * weights["saves"])
            + (shutouts * weights["shutouts"])
            + (ot_losses * weights["otLosses"])
        )
        avg_fpts = total_fpts / games_played

        filtered_stats = {
            "playerId": goalie.get("playerId"),
            "goalieFullName": goalie.get("goalieFullName"),
            "teamAbbrevs": goalie.get("teamAbbrevs"),
            "gamesPlayed": goalie.get("gamesPlayed"),
            "wins": wins,
            "goalsAgainst": goals_against,
            "saves": saves,
            "shutouts": shutouts,
            "otLosses": ot_losses,
            "Fpts": round(total_fpts, 2),
            "Avg": round(avg_fpts, 2),
        }
        filtered_goalies.append(filtered_stats)

    return filtered_goalies


# --- Schedule Transforms ---


def count_games_per_team_per_week(schedule: dict) -> dict:
    """
    Count games per team per fantasy week and track opponents
    """
    # Note: This function requires get_fantasy_week,
    # so the file that calls this must also import date_utils
    from utils.date_utils import get_fantasy_week

    team_week_data = defaultdict(
        lambda: defaultdict(lambda: {"count": 0, "opponents": [], "games": []})
    )

    for game_id, game_data in schedule.items():
        year, week = get_fantasy_week(game_data["date"])
        week_key = f"{year}-W{week:02d}"

        home_team = game_data["home_abbrev"]
        away_team = game_data["away_abbrev"]

        # Track for home team
        team_week_data[home_team][week_key]["count"] += 1
        team_week_data[home_team][week_key]["opponents"].append(f"vs {away_team}")

        # Track for away team
        team_week_data[away_team][week_key]["count"] += 1
        team_week_data[away_team][week_key]["opponents"].append(f"@ {home_team}")

    return {team: dict(weeks) for team, weeks in team_week_data.items()}
