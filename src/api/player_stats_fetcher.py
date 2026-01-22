import time
import asyncio
import httpx

from typing import DefaultDict, Dict, List, Any, Union

import src.core.constants as constants
from sqlmodel import Session, select
from src.database.database import engine

# Import ProPlayers model for the incremental update
from src.database.models import PlayerGameStats, GoalieGameStats, ProPlayers

from src.database.utils import bulk_merge_data
from src.api.models import (
    PlayerGameLogResponse,
    GameBoxscoreResponse,
    PlayerStatsFromBoxscore,
    GoalieStatsFromBoxscore,
    FinalPlayerGameStats,
)
from collections import defaultdict
from datetime import datetime
from src.utils.cache_utils import load_data_from_cache, save_data_to_cache

from .helpers import (
    fetch_game_boxscore,
    fetch_player_log,
    merge_skater_stats,
    merge_goalie_stats,
    get_opponent_abbrev,
    calculate_fantasy_points_goalie,
    calculate_fantasy_points_skater,
    toi_to_seconds,
)

# --- MAIN PROCESSOR CLASS ---


class PlayerStatsProcessor:
    """
    Orchestrates the fetching, processing, and caching of game stats,
    and updates the ProPlayers table incrementally.
    """

    def __init__(self, use_cache: bool, perform_incremental_update: bool = True):
        self.use_cache = use_cache
        # --- NEW: Control for incremental logic ---
        self.perform_incremental_update = perform_incremental_update

        # This is the "state" that your script was passing around
        self.game_cache_internal: DefaultDict[int, Dict[int, FinalPlayerGameStats]] = (
            defaultdict(dict)
        )
        self.boxscore_map: Dict[int, GameBoxscoreResponse] = {}
        self.game_stats_cache_on_disk: Dict[str, Any] = {}
        self.game_ids_to_fetch_fresh: List[int] = []
        self.game_ids_from_cache: List[int] = []

    async def process_games(self, game_ids_to_process: List[int]):
        """
        Main orchestrator to fetch, process, and cache game stats.
        This is the primary public method.
        """
        start_time = time.perf_counter()
        print(
            f"Processing {len(game_ids_to_process)} games. Cache enabled: {self.use_cache}"
        )

        # --- 1. Load on-disk cache and partition games ---
        self._load_and_partition_games(game_ids_to_process)

        # --- 2. Fetch all fresh game data (Phases 1-3) ---
        await self._fetch_fresh_game_data()

        # --- 3. Update on-disk cache (Phase 4) ---
        self._update_on_disk_cache()

        # --- 4. Write all data (cached + fresh) to DB (Phase 5) ---
        # This now includes the incremental ProPlayer update
        self._write_data_to_db()

        end_time = time.perf_counter()
        print(f"\n--- PROCESSING COMPLETE ({end_time - start_time:.2f}s) ---")

    # --- Private Helper Methods ---

    def _load_and_partition_games(self, game_ids_to_process: List[int]):
        """
        Loads the on-disk cache and partitions games into "fresh fetch" vs "load from cache".
        Mutates: self.game_stats_cache_on_disk, self.game_ids_to_fetch_fresh,
                 self.game_ids_from_cache, self.boxscore_map, self.game_cache_internal
        """
        cache_result = load_data_from_cache(constants.GAME_STATS_CACHE)
        self.game_stats_cache_on_disk = (
            cache_result if isinstance(cache_result, dict) else {}
        )

        if self.use_cache:
            for game_id in game_ids_to_process:
                cached_game = self.game_stats_cache_on_disk.get(str(game_id))
                if cached_game and cached_game.get("status") == "final":
                    self.game_ids_from_cache.append(game_id)
                else:
                    self.game_ids_to_fetch_fresh.append(game_id)
        else:
            self.game_ids_to_fetch_fresh = list(game_ids_to_process)

        print(f"  - Loading {len(self.game_ids_from_cache)} games from cache.")
        print(
            f"  - Fetching {len(self.game_ids_to_fetch_fresh)} new/updated games from API."
        )

        # --- Load Cached Games ---
        for game_id in self.game_ids_from_cache:
            cached_data = self.game_stats_cache_on_disk.get(str(game_id))
            if not cached_data:
                continue

            try:
                self.boxscore_map[game_id] = GameBoxscoreResponse(
                    **cached_data["boxscore_raw"]
                )
                for player_id_str, player_stats_dict in cached_data["players"].items():
                    self.game_cache_internal[game_id][int(player_id_str)] = (
                        FinalPlayerGameStats(**player_stats_dict)
                    )
            except Exception as e:
                print(
                    f"Warning: Cache for game {game_id} corrupted, re-fetching. Error: {e}"
                )
                self.game_ids_to_fetch_fresh.append(game_id)
                # Note: This doesn't remove from game_ids_from_cache, but it's okay.
                # The game will just be processed twice, and the DB merge will handle it.

    async def _fetch_fresh_game_data(self):
        """
        Async function to perform Phases 1, 2, and 3:
        Fetch boxscores, fetch player logs, and merge the data.
        Reads:   self.game_ids_to_fetch_fresh
        Mutates: self.game_cache_internal, self.boxscore_map
        """
        if not self.game_ids_to_fetch_fresh:
            print("No fresh games to fetch.")
            return

        start_time = time.perf_counter()
        semaphore = asyncio.Semaphore(constants.CONCURRENCY_LIMIT)
        async with httpx.AsyncClient() as client:
            # --- PHASE 1: Fetch boxscores ---
            print(
                f"\n--- Phase 1: Fetching {len(self.game_ids_to_fetch_fresh)} boxscores..."
            )
            boxscore_tasks = [
                fetch_game_boxscore(client, semaphore, game_id)
                for game_id in self.game_ids_to_fetch_fresh
            ]
            boxscore_results = await asyncio.gather(
                *boxscore_tasks, return_exceptions=True
            )

            player_ids_to_fetch_log: set[int] = set()
            for i, result in enumerate(boxscore_results):
                if isinstance(result, Exception):
                    game_id = self.game_ids_to_fetch_fresh[i]
                    print(
                        f"Warning: Failed to fetch boxscore for game {game_id}: {result}"
                    )
                    continue
                if result is None:
                    continue

                assert isinstance(result, GameBoxscoreResponse)
                self.boxscore_map[result.id] = result  # Mutate map

                all_players: List[
                    Union[PlayerStatsFromBoxscore, GoalieStatsFromBoxscore]
                ] = (
                    result.playerByGameStats.awayTeam.forwards
                    + result.playerByGameStats.awayTeam.defense
                    + result.playerByGameStats.awayTeam.goalies
                    + result.playerByGameStats.homeTeam.forwards
                    + result.playerByGameStats.homeTeam.defense
                    + result.playerByGameStats.homeTeam.goalies
                )
                for player_stats in all_players:
                    player_ids_to_fetch_log.add(player_stats.playerId)

            phase1_time = time.perf_counter()
            print(
                f"Phase 1 complete. Found {len(player_ids_to_fetch_log)} unique players. ({phase1_time - start_time:.2f}s)"
            )

            # --- PHASE 2: Fetch player logs ---
            print(
                f"\n--- Phase 2: Fetching logs for {len(player_ids_to_fetch_log)} players..."
            )
            player_log_tasks = [
                fetch_player_log(client, semaphore, player_id)
                for player_id in player_ids_to_fetch_log
            ]
            player_log_results = await asyncio.gather(
                *player_log_tasks, return_exceptions=True
            )

            for log_result in player_log_results:
                if isinstance(log_result, Exception):
                    print(f"Warning: Failed to process player log: {log_result}")
                    continue
                if log_result is None:
                    continue

                assert isinstance(log_result, tuple) and len(log_result) == 2
                player_id: int = log_result[0]
                log_response: PlayerGameLogResponse = log_result[1]

                for game in log_response.gameLog:
                    if game.gameId in self.game_ids_to_fetch_fresh:
                        final_stats = FinalPlayerGameStats(
                            playerId=player_id,
                            gameId=game.gameId,
                            teamAbbrev=game.teamAbbrev,
                            gameDate=game.gameDate,
                            powerPlayPoints=game.powerPlayPoints,
                            shorthandedPoints=game.shorthandedPoints,
                            toi=game.toi,
                            shifts=game.shifts,
                            pim=game.pim,
                        )
                        # Mutate cache
                        self.game_cache_internal[game.gameId][player_id] = final_stats

            phase2_time = time.perf_counter()
            print(
                f"Phase 2 complete. Populated internal cache. ({phase2_time - phase1_time:.2f}s)"
            )

            # --- PHASE 3: Merge boxscore data ---
            print("\n--- Phase 3: Merging boxscore data...")
            merge_count = 0
            for game_id in self.game_ids_to_fetch_fresh:
                boxscore = self.boxscore_map.get(game_id)
                if not boxscore:
                    continue

                assert isinstance(boxscore, GameBoxscoreResponse)

                all_players_phase3: List[
                    Union[PlayerStatsFromBoxscore, GoalieStatsFromBoxscore]
                ] = (
                    boxscore.playerByGameStats.awayTeam.forwards
                    + boxscore.playerByGameStats.awayTeam.defense
                    + boxscore.playerByGameStats.awayTeam.goalies
                    + boxscore.playerByGameStats.homeTeam.forwards
                    + boxscore.playerByGameStats.homeTeam.defense
                    + boxscore.playerByGameStats.homeTeam.goalies
                )

                for player_stats in all_players_phase3:
                    player_id = player_stats.playerId
                    if player_id in self.game_cache_internal[game_id]:
                        entry_to_update = self.game_cache_internal[game_id][player_id]

                        if isinstance(player_stats, PlayerStatsFromBoxscore):
                            merge_skater_stats(entry_to_update, player_stats)
                            merge_count += 1
                        elif isinstance(player_stats, GoalieStatsFromBoxscore):
                            merge_goalie_stats(entry_to_update, player_stats)
                            merge_count += 1

            phase3_time = time.perf_counter()
            print(
                f"Phase 3 complete. Merged {merge_count} records. ({phase3_time - phase2_time:.2f}s)"
            )

    def _update_on_disk_cache(self):
        """
        PHASE 4: Update the on-disk cache (game_stats_cache.json)
        Reads:   self.game_ids_to_fetch_fresh, self.game_cache_internal,
                 self.boxscore_map
        Mutates: self.game_stats_cache_on_disk
        """
        print("\n--- Phase 4: Updating on-disk cache...")
        updated_count = 0
        for game_id in self.game_ids_to_fetch_fresh:
            boxscore = self.boxscore_map.get(game_id)
            if not boxscore:
                continue

            assert isinstance(boxscore, GameBoxscoreResponse)
            status = "final"  # Assuming all fetched games are final

            players_dict = {
                str(pid): stats.model_dump()
                for pid, stats in self.game_cache_internal[game_id].items()
            }

            # Mutate on-disk cache object
            self.game_stats_cache_on_disk[str(game_id)] = {
                "cached_at": datetime.utcnow().isoformat() + "Z",
                "status": status,
                "boxscore_raw": boxscore.model_dump(),
                "players": players_dict,
            }
            updated_count += 1

        if updated_count > 0:
            save_data_to_cache(
                self.game_stats_cache_on_disk, constants.GAME_STATS_CACHE
            )
            print(f"  ✅ Saved {updated_count} games to on-disk cache.")
        else:
            print("  - On-disk cache is already up-to-date.")

    def _write_data_to_db(self):
        """
        PHASE 5: Transform data, write game stats to DB, and
        incrementally update the ProPlayers table.
        Reads: self.game_cache_internal, self.boxscore_map, self.game_ids_to_fetch_fresh
        """
        print("\n--- Phase 5: Writing to database and updating ProPlayers...")

        skater_records_to_merge = []
        goalie_records_to_merge = []

        # --- NEW: Lists for *only* new stats ---
        skater_records_for_incremental_update = []
        goalie_records_for_incremental_update = []

        # --- 1. Transform Pydantic models to SQLModel objects ---
        for game_id, players in self.game_cache_internal.items():
            boxscore = self.boxscore_map.get(game_id)
            if not boxscore:
                print(
                    f"Warning: No boxscore found for game {game_id}. Skipping DB write for this game."
                )
                continue

            for player_id, stats in players.items():
                opponent_abbrev = get_opponent_abbrev(boxscore, stats.teamAbbrev)
                team_name = constants.TEAM_MAP.get(stats.teamAbbrev) or "Unknown"
                opponent_name = constants.TEAM_MAP.get(opponent_abbrev) or "Unknown"

                if stats.position in constants.GOALIE_POSITIONS:
                    # Goalie Record
                    goalie_record = GoalieGameStats(
                        game_id=stats.gameId,
                        player_id=stats.playerId,
                        season=constants.SEASON_ID,
                        game_date=stats.gameDate,
                        team_abbrev=stats.teamAbbrev,
                        team_name=team_name,
                        opponent_abbrev=opponent_abbrev,
                        opponent_name=opponent_name,
                        player_name=stats.name,
                        jersey_number=stats.sweaterNumber,
                        position="Goalie",
                        saves=stats.saves or 0,
                        save_pct=stats.savePctg or 0.0,
                        goals_against=stats.goalsAgainst or 0,
                        decision=stats.decision,
                        wins=1 if stats.decision == "W" else 0,
                        shutouts=(
                            1
                            if (
                                stats.goalsAgainst == 0
                                and stats.saves
                                and stats.saves > 0
                            )
                            else 0
                        ),
                        ot_losses=1 if stats.decision == "O" else 0,
                        total_fpts=calculate_fantasy_points_goalie(stats),
                    )
                    goalie_records_to_merge.append(goalie_record)

                    # NEW: Only add to incremental list if it was a freshly fetched game
                    if game_id in self.game_ids_to_fetch_fresh:
                        goalie_records_for_incremental_update.append(goalie_record)
                else:
                    # Skater Record
                    shooting_pct = None
                    if stats.sog > 0:
                        shooting_pct = stats.goals / stats.sog

                    skater_record = PlayerGameStats(
                        game_id=stats.gameId,
                        player_id=stats.playerId,
                        season=constants.SEASON_ID,
                        game_date=stats.gameDate,
                        team_abbrev=stats.teamAbbrev,
                        team_name=team_name,
                        opponent_abbrev=opponent_abbrev,
                        opponent_name=opponent_name,
                        player_name=stats.name,
                        jersey_number=stats.sweaterNumber,
                        position=stats.position,
                        goals=stats.goals,
                        assists=stats.assists,
                        pp_points=float(stats.powerPlayPoints),
                        sh_points=float(stats.shorthandedPoints),
                        shots=stats.sog,
                        shooting_pct=shooting_pct,
                        blocked_shots=stats.blockedShots,
                        hits=stats.hits,
                        total_fpts=calculate_fantasy_points_skater(stats),
                        toi_seconds=toi_to_seconds(stats.toi),
                        shifts=stats.shifts,
                    )
                    skater_records_to_merge.append(skater_record)

                    # NEW: Only add to incremental list if it was a freshly fetched game
                    if game_id in self.game_ids_to_fetch_fresh:
                        skater_records_for_incremental_update.append(skater_record)

        # --- 2. & 3. Write stats and update ProPlayers in one transaction ---
        with Session(engine) as session:
            try:
                # Step 2: Merge ALL game stats (from cache + fresh)
                # This ensures game stats are always in the DB (handles your scenario)
                print("  - Merging all game stats...")
                skater_merged = bulk_merge_data(session, skater_records_to_merge)
                goalie_merged = bulk_merge_data(session, goalie_records_to_merge)
                print(
                    f"  - Merged {skater_merged} skater and {goalie_merged} goalie records."
                )

                # --- MODIFIED: Add a guard ---
                # Step 3: Incrementally update ProPlayers
                # Only run this if the processor is told to.
                if self.perform_incremental_update:
                    self._update_pro_players_incrementally(
                        session,
                        skater_records_for_incremental_update,
                        goalie_records_for_incremental_update,
                    )
                else:
                    print(
                        "  - Skipping incremental ProPlayers update (full rebuild requested)."
                    )

                # Commit all changes at once
                print("  - Committing all database changes...")
                session.commit()

                print("✅ Database write complete!")

            except Exception as e:
                print(f"❌ Database commit failed: {e}")
                session.rollback()
                raise

    def _update_pro_players_incrementally(
        self,
        session: Session,
        new_skater_stats: List[PlayerGameStats],
        new_goalie_stats: List[GoalieGameStats],
    ):
        """
        Incrementally updates the ProPlayers table based on new game stats.
        This is far more efficient than re-calculating totals for all players.
        Runs *within* the main DB session.
        """
        print("  - Incrementally updating ProPlayers table...")
        player_ids = {s.player_id for s in new_skater_stats} | {
            g.player_id for g in new_goalie_stats
        }
        if not player_ids:
            print("    - No players to update.")
            return 0

        # Fetch all players to update in one query
        existing_players_list = session.exec(
            select(ProPlayers).where(ProPlayers.player_id in (player_ids))
        ).all()
        existing_players_map = {p.player_id: p for p in existing_players_list}

        updated_count = 0

        # Process skaters
        for stat in new_skater_stats:
            player = existing_players_map.get(stat.player_id)
            safe_player_name = stat.player_name or "Unknown"
            if not player:
                # Create a new ProPlayer if they don't exist
                player = ProPlayers(
                    player_id=stat.player_id,
                    is_active=True,
                    is_goalie=False,
                    player_name=safe_player_name,
                )
                session.add(player)
                existing_players_map[stat.player_id] = player
                print(f"    - Created new ProPlayer: {safe_player_name}")

            # Update player info (always use the latest game's info)
            player.player_name = safe_player_name
            player.team_abbrev = stat.team_abbrev
            player.position = stat.position
            player.jersey_number = stat.jersey_number

            # Increment seasonal stats
            # This logic assumes you are not re-processing old, already-processed games.
            # The bulk_merge_data for game stats handles updates, but this incremental
            # logic is additive. If you re-run on old data, this will double-count.
            # For a daily script, this is correct.
            player.season_games_played = (player.season_games_played or 0) + 1
            player.season_total_fpts = (player.season_total_fpts or 0) + stat.total_fpts
            player.season_goals = (player.season_goals or 0) + stat.goals
            player.season_assists = (player.season_assists or 0) + stat.assists
            player.season_pp_points = (player.season_pp_points or 0) + stat.pp_points
            player.season_sh_points = (player.season_sh_points or 0) + stat.sh_points
            player.season_shots = (player.season_shots or 0) + stat.shots
            player.season_blocked_shots = (
                player.season_blocked_shots or 0
            ) + stat.blocked_shots
            player.season_hits = (player.season_hits or 0) + stat.hits
            updated_count += 1

        # Process goalies
        for stat in new_goalie_stats:
            player = existing_players_map.get(stat.player_id)
            safe_player_name = stat.player_name or "Unknown"
            if not player:
                player = ProPlayers(
                    player_id=stat.player_id,
                    is_active=True,
                    is_goalie=True,
                    player_name=safe_player_name,
                )
                session.add(player)
                existing_players_map[stat.player_id] = player
                print(f"    - Created new ProPlayer: {stat.player_name}")

            # Update player info
            player.team_abbrev = stat.team_abbrev
            player.position = stat.position  # "Goalie"
            player.jersey_number = stat.jersey_number
            player.is_goalie = True  # Ensure this is set

            # Increment seasonal stats
            player.season_games_played = (player.season_games_played or 0) + 1
            player.season_total_fpts = (player.season_total_fpts or 0) + stat.total_fpts
            player.season_wins = (player.season_wins or 0) + stat.wins
            player.season_shutouts = (player.season_shutouts or 0) + stat.shutouts
            player.season_ot_losses = (player.season_ot_losses or 0) + stat.ot_losses
            player.season_saves = (player.season_saves or 0) + stat.saves
            player.season_goals_against = (
                player.season_goals_against or 0
            ) + stat.goals_against
            updated_count += 1

        print(f"    - Updated {updated_count} ProPlayer records.")
        return updated_count
