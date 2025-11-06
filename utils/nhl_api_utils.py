"""
utils/nhl_api_utils.py
All functions for fetching data from the NHL API.
"""

import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pydantic import ValidationError
from sqlmodel import Session

import constants
from models.database import PlayerGameStats, GoalieGameStats
from models.api.stats import GameBoxscoreResponse, PlayerGameLogResponse
from models.api.schedule import GamesResponse

# Import other utils
from utils.utils import load_data_from_cache, save_data_to_cache


def load_player_log_cache():
    """Loads the player log cache from disk, converting string keys to int."""
    print("Loading player log cache...")
    data = load_data_from_cache(constants.PLAYER_LOG_CACHE)
    if not data:
        print("  ! No player log cache file found. Starting fresh.")
        return {}

    try:
        # JSON saves all keys as strings. We must convert them back to integers
        # for both player IDs (outer keys) and game IDs (inner keys).
        int_key_cache = {}
        for player_id_str, games in data.items():
            int_key_cache[int(player_id_str)] = {
                int(game_id_str): game_data for game_id_str, game_data in games.items()
            }
        print(f"  ✓ Loaded {len(int_key_cache)} players into log cache.")
        return int_key_cache
    except Exception as e:
        print(f"  ! Error loading player log cache: {e}. Starting fresh.")
        return {}


# MODIFIED: Load cache from file
_player_log_cache = load_player_log_cache()


def save_player_log_cache():
    """Saves the in-memory player log cache to a file."""
    print("\nSaving player log cache to disk...")
    save_data_to_cache(_player_log_cache, constants.PLAYER_LOG_CACHE)


# --- Player Stats Fetching ---


def fetch_stats_data(url: str) -> list:
    """
    Fetches data from a specific NHL stats/rest endpoint.
    """
    print(f"  Fetching from: {url}")
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json().get("data", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []


# --- Schedule Fetching ---


def fetch_team_schedule(team_abbv: str) -> dict:
    """Fetch schedule for a single team"""
    team_schedule_url = (
        f"{constants.WEB_URL}/club-schedule-season/{team_abbv}/{constants.SEASON_ID}"
    )
    try:
        resp = requests.get(team_schedule_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        games_response = GamesResponse(**data)
        team_games = {}
        for game in games_response.games:
            if game.gameType == 2:  # Regular season only
                team_games[game.id] = {
                    "date": game.startTimeUTC,
                    "home_team": game.homeTeam.commonName.default,
                    "away_team": game.awayTeam.commonName.default,
                    "home_abbrev": game.homeTeam.abbrev,
                    "away_abbrev": game.awayTeam.abbrev,
                }
        return team_games
    except (ValidationError, requests.RequestException) as e:
        print(f"  Warning: Error fetching {team_abbv}: {e}")
        return {}


def get_all_unique_games() -> dict:
    """Get all unique regular season games using concurrent requests"""
    unique_games = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_team = {
            executor.submit(fetch_team_schedule, team): team
            for team in constants.NHL_TEAMS
        }
        for future in as_completed(future_to_team):
            team_games = future.result()
            unique_games.update(team_games)
    return unique_games


def get_schedule(force_refresh: bool = False) -> dict:
    """
    Get schedule - from cache or API.
    This is the main reusable function for other scripts.
    """
    if not force_refresh:
        cached = load_data_from_cache(constants.SCHEDULE_CACHE)
        if cached:
            return cached

    print("Fetching fresh schedule from NHL API...")
    start_time = time.time()
    unique_games = get_all_unique_games()
    end_time = time.time()
    print(
        f"Successfully Fetched {len(unique_games)} games in {end_time - start_time:.2f}s"
    )
    save_data_to_cache(unique_games, constants.SCHEDULE_CACHE)
    return unique_games


# --- Game Log Fetching & Processing ---


def get_player_game_log_data(player_id: int, game_id: int) -> dict:
    """
    Get PP/SH points AND team_abbrev for a specific player and game.
    Caches the entire game log for the player.
    """
    if player_id in _player_log_cache and game_id in _player_log_cache[player_id]:
        # Cache HIT: We have this player and this game.
        return _player_log_cache[player_id].get(game_id)

    try:
        log_url = (
            f"{constants.WEB_URL}/player/{player_id}/game-log/{constants.SEASON_ID}/2"
        )
        time.sleep(0.05)  # Rate limit
        resp = requests.get(log_url, timeout=10)
        resp.raise_for_status()
        log_data = PlayerGameLogResponse(**resp.json())

        player_games = {}
        for game in log_data.gameLog:
            player_games[game.gameId] = {
                "pp_points": game.powerPlayPoints,
                "sh_points": game.shorthandedPoints,
                "team_abbrev": game.teamAbbrev,
            }

        # Update/overwrite the player's log in the global cache
        _player_log_cache[player_id] = player_games

    except (requests.exceptions.RequestException, ValidationError) as e:
        print(f"  Warning: Could not fetch game log for player {player_id}: {e}")
        # If fetch fails, ensure we don't keep retrying this session
        if player_id not in _player_log_cache:
            _player_log_cache[player_id] = {}  # Add empty entry to prevent re-fetch

    # Return the data for the requested game_id
    return _player_log_cache[player_id].get(
        game_id, {"pp_points": 0, "sh_points": 0, "team_abbrev": "UNK"}
    )


def process_game(game_data: dict, session: Session) -> tuple[int, int]:
    """
    Processes a single game, fetches its boxscore, and saves all
    player stats to the database.

    Returns: (skaters_saved, goalies_saved)
    """
    game_id = int(game_data["game_id"])
    game_date = game_data["game_date_str"]

    try:
        boxscore_url = f"{constants.WEB_URL}/gamecenter/{game_id}/boxscore"
        time.sleep(0.3)
        resp = requests.get(boxscore_url, timeout=10)
        resp.raise_for_status()
        boxscore = GameBoxscoreResponse(**resp.json())

        home_abbrev = boxscore.homeTeam.abbrev
        away_abbrev = boxscore.awayTeam.abbrev
        game_skaters, game_goalies = 0, 0
        teams_data = [
            (boxscore.playerByGameStats.homeTeam, home_abbrev, away_abbrev),
            (boxscore.playerByGameStats.awayTeam, away_abbrev, home_abbrev),
        ]

        for team_stats, boxscore_team, boxscore_opponent in teams_data:
            # === PROCESS SKATERS ===
            for player in team_stats.forwards + team_stats.defense:
                player_id = player.playerId
                player_name = player.name.get("default", "Unknown")
                game_log_data = get_player_game_log_data(player_id, game_id)
                team_abbrev = game_log_data["team_abbrev"]
                opponent_abbrev = (
                    home_abbrev
                    if team_abbrev == away_abbrev
                    else (
                        away_abbrev if team_abbrev == home_abbrev else boxscore_opponent
                    )
                )

                total_fpts = (
                    player.goals * constants.SKATER_FPTS_WEIGHTS["goals"]
                    + player.assists * constants.SKATER_FPTS_WEIGHTS["assists"]
                    + game_log_data["pp_points"]
                    * constants.SKATER_FPTS_WEIGHTS["ppPoints"]
                    + game_log_data["sh_points"]
                    * constants.SKATER_FPTS_WEIGHTS["shPoints"]
                    + player.sog * constants.SKATER_FPTS_WEIGHTS["shots"]
                    + player.blockedShots
                    * constants.SKATER_FPTS_WEIGHTS["blockedShots"]
                    + player.hits * constants.SKATER_FPTS_WEIGHTS["hits"]
                )
                skater_log = PlayerGameStats(
                    game_id=game_id,
                    player_id=player_id,
                    game_date=game_date,
                    team_abbrev=team_abbrev,
                    opponent_abbrev=opponent_abbrev,
                    player_name=player_name,
                    goals=player.goals,
                    assists=player.assists,
                    pp_points=float(game_log_data["pp_points"]),
                    sh_points=float(game_log_data["sh_points"]),
                    shots=player.sog,
                    blocked_shots=player.blockedShots,
                    hits=player.hits,
                    total_fpts=round(total_fpts, 2),
                )
                session.merge(skater_log)
                game_skaters += 1

            # === PROCESS GOALIES ===
            for goalie in team_stats.goalies:
                if goalie.position != "G":
                    continue
                player_id = goalie.playerId
                player_name = goalie.name.get("default", "Unknown")
                game_log_data = get_player_game_log_data(player_id, game_id)
                team_abbrev = game_log_data["team_abbrev"]
                opponent_abbrev = (
                    home_abbrev
                    if team_abbrev == away_abbrev
                    else (
                        away_abbrev if team_abbrev == home_abbrev else boxscore_opponent
                    )
                )

                wins = 1 if goalie.decision == "W" else 0
                ot_losses = 1 if goalie.decision == "OT" else 0
                shutouts = 1 if (wins == 1 and goalie.goalsAgainst == 0) else 0

                total_fpts = (
                    wins * constants.GOALIE_FPTS_WEIGHTS["wins"]
                    + goalie.saves * constants.GOALIE_FPTS_WEIGHTS["saves"]
                    + goalie.goalsAgainst
                    * constants.GOALIE_FPTS_WEIGHTS["goalsAgainst"]
                    + shutouts * constants.GOALIE_FPTS_WEIGHTS["shutouts"]
                    + ot_losses * constants.GOALIE_FPTS_WEIGHTS["otLosses"]
                )
                goalie_log = GoalieGameStats(
                    game_id=game_id,
                    player_id=player_id,
                    game_date=game_date,
                    team_abbrev=team_abbrev,
                    opponent_abbrev=opponent_abbrev,
                    player_name=player_name,
                    saves=goalie.saves,
                    goals_against=goalie.goalsAgainst,
                    decision=goalie.decision,
                    wins=wins,
                    shutouts=shutouts,
                    ot_losses=ot_losses,
                    total_fpts=round(total_fpts, 2),
                )
                session.merge(goalie_log)
                game_goalies += 1

        # session.commit()
        print(f"   ✓ Staged {game_skaters} skaters, {game_goalies} goalies")
        return (game_skaters, game_goalies)
    except (requests.exceptions.RequestException, ValidationError, Exception) as e:
        print(f"   ✗ Error processing game {game_id}: {e}")
        session.rollback()
        return (0, 0)
