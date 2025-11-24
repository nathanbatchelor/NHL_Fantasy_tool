"""
scripts/trade_analyzer.py

Analyzes your fantasy team and generates REALISTIC trade recommendations.
Prioritizes 'Fairness', 'Market Psychology', and 'Star Power' over raw stats.
Includes Player Lookup Mode for targeted trading (Buy/Sell).
Enforces strict Roster Limits (Max 22) and Positional Balance.
v5.1: Updated Player Lookup and relaxed constraints for lower-value players.

Usage:
  python scripts/trade_analyzer.py
"""

import asyncio
from collections import Counter
from sqlmodel import Session, select, func, text, col
from src.database.database import engine
from src.database.models import (
    ProPlayers,
    FantasyTeam,
    PlayerGameStats,
    GoalieGameStats,
)
from src.core.constants import SEASON_ID, GOALIE_POSITIONS
from src.utils.date_utils import calculate_remaining_week_matchups
from datetime import datetime, timedelta
import pytz
from src.core.constants import FANTASY_TIMEZONE
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

# --- CONFIGURATION ---
REPLACEMENT_LEVEL_FPTS_F = 1.5
REPLACEMENT_LEVEL_FPTS_D = 1.2

# Increased multipliers to protect elite assets
DEFENSEMAN_MULTIPLIER = 1.15  # Base boost for all D
ELITE_D_MULTIPLIER = 1.25  # Extra boost for D with > 2.5 Avg (Fox, Makar, Hughes)

ROS_WEIGHT = 0.75  # Increased weight on long-term talent
SHORT_TERM_WEIGHT = 0.25
ROSTER_SPOT_VALUE = 10.0

# ROSTER CONSTRAINTS
MAX_ROSTER_SIZE = 22  # Active roster max (excludes IR slots usually, but for trade logic we stick to active)

# Strict Limits (Hard Cap) - If trade exceeds this, reject.
HARD_COUNTS = {"C": 6, "LW": 6, "RW": 6, "D": 7, "G": 4}

# Starter Slots for Lineup Calculation
STARTER_COUNTS = {"C": 2, "LW": 2, "RW": 2, "D": 4, "G": 2}


@dataclass
class PlayerScore:
    """Composite score for a player's value"""

    player: ProPlayers
    season_avg: float
    recent_avg: float
    avg_toi_sec: float
    usage_multiplier: float
    predicted: float
    prior_season_avg: float
    games_next_2_weeks: int
    weighted_avg: float
    vorp_avg: float
    composite_score: float
    recent_games_played: int
    risk_penalty: float
    position: str


def get_usage_multiplier(position: str, avg_toi_sec: float) -> float:
    if avg_toi_sec <= 0:
        return 1.0
    minutes = avg_toi_sec / 60.0
    if position == "D":
        if minutes >= 24.0:
            return 1.10
        if minutes >= 22.0:
            return 1.05
        if minutes < 17.0:
            return 0.90
    else:
        if minutes >= 20.0:
            return 1.10
        if minutes >= 18.0:
            return 1.05
        if minutes < 14.0:
            return 0.90
    return 1.0


def calculate_trade_value(
    season_avg: float,
    recent_avg: float,
    predicted: float,
    prior_season: float,
    games_next_2_weeks: int,
    position: str,
    usage_multiplier: float,
) -> Tuple[float, float, float]:

    weighted_avg = (
        recent_avg * 0.50 + season_avg * 0.30 + predicted * 0.15 + prior_season * 0.05
    )
    adjusted_avg = weighted_avg * usage_multiplier
    replacement_level = (
        REPLACEMENT_LEVEL_FPTS_D if position == "D" else REPLACEMENT_LEVEL_FPTS_F
    )
    vorp_avg = max(0.1, adjusted_avg - replacement_level)

    # Position Scarcity Logic
    if position == "D":
        if adjusted_avg > 2.5:  # Elite D threshold
            vorp_avg *= ELITE_D_MULTIPLIER
        else:
            vorp_avg *= DEFENSEMAN_MULTIPLIER

    ros_value = vorp_avg * 30
    short_term_value = vorp_avg * games_next_2_weeks
    composite = (ros_value * ROS_WEIGHT) + (short_term_value * SHORT_TERM_WEIGHT)

    return weighted_avg, vorp_avg, composite


def get_player_stats_data(
    session: Session, player_id: int, is_goalie: bool
) -> Tuple[float, int, float, float]:
    cutoff_date = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    if is_goalie:
        return (0.0, 0, 0.0, 0.0)

    query_recent = text(
        """
        SELECT AVG(total_fpts) as avg_fpts, COUNT(*) as games, AVG(toi_seconds) as avg_toi
        FROM player_game_stats
        WHERE player_id = :player_id AND game_date >= :cutoff_date AND season = :season
    """
    )
    res_recent = session.exec(
        query_recent,
        params={
            "player_id": player_id,
            "cutoff_date": cutoff_date,
            "season": SEASON_ID,
        },
    ).first()
    query_season = text(
        """
        SELECT AVG(toi_seconds) as avg_toi
        FROM player_game_stats
        WHERE player_id = :player_id AND season = :season
    """
    )
    res_season = session.exec(
        query_season, params={"player_id": player_id, "season": SEASON_ID}
    ).first()

    rec_avg = res_recent.avg_fpts if res_recent and res_recent.avg_fpts else 0.0
    rec_games = res_recent.games if res_recent else 0
    rec_toi = res_recent.avg_toi if res_recent and res_recent.avg_toi else 0.0
    seas_toi = res_season.avg_toi if res_season and res_season.avg_toi else 0.0
    return (rec_avg, rec_games, seas_toi, rec_toi)


async def get_upcoming_games(team_abbrev: str) -> int:
    tz = pytz.timezone(FANTASY_TIMEZONE)
    remaining_this_week = await calculate_remaining_week_matchups()
    games_this_week = len(remaining_this_week.get(team_abbrev, []))
    return games_this_week + (games_this_week if 2 <= games_this_week <= 4 else 3)


async def score_all_players(
    session: Session, players: List[ProPlayers], league_max_games: int
) -> List[PlayerScore]:
    scored_players = []
    for player in players:
        # Relaxed games played filter: allow players with fewer games if looking for trades
        if player.season_games_played < 3 and league_max_games > 10:
            continue
        if player.position in GOALIE_POSITIONS or player.is_goalie:
            continue

        recent_avg, recent_games, season_avg_toi, recent_avg_toi = (
            get_player_stats_data(session, player.player_id, False)
        )

        if recent_games < 3:
            recent_avg = player.season_total_fpts / max(player.season_games_played, 1)
            usage_toi = season_avg_toi
        else:
            usage_toi = (season_avg_toi * 0.7) + (recent_avg_toi * 0.3)

        season_avg = player.season_total_fpts / max(player.season_games_played, 1)
        predicted = player.predicted_fpts or season_avg
        prior_season = player.prior_season_avg_fpts or season_avg
        games_next_2_weeks = await get_upcoming_games(player.team_abbrev or "")

        usage_multiplier = get_usage_multiplier(player.position, usage_toi)
        weighted_avg, vorp_avg, composite = calculate_trade_value(
            season_avg,
            recent_avg,
            predicted,
            prior_season,
            games_next_2_weeks,
            player.position,
            usage_multiplier,
        )

        risk_penalty = 1.0
        if league_max_games > 10:
            participation_rate = player.season_games_played / league_max_games
            if participation_rate < 0.60:
                risk_penalty = max(0.1, min(1.0, participation_rate + 0.25))
                composite *= risk_penalty

        scored_players.append(
            PlayerScore(
                player,
                season_avg,
                recent_avg,
                usage_toi,
                usage_multiplier,
                predicted,
                prior_season,
                games_next_2_weeks,
                weighted_avg,
                vorp_avg,
                composite,
                recent_games,
                risk_penalty,
                player.position,
            )
        )
    return sorted(scored_players, key=lambda x: x.composite_score, reverse=True)


def check_roster_compliance(current_roster, sending, receiving) -> bool:
    """
    Validates roster size and positional limits.
    """
    # 1. Check Total Roster Size
    current_size = len(current_roster)
    net_change = len(receiving) - len(sending)
    new_size = current_size + net_change

    if new_size > MAX_ROSTER_SIZE:
        return False

    # 2. Check Positional Limits
    current_positions = Counter([p.player.position for p in current_roster])

    # Simulate Trade
    for p in sending:
        if current_positions[p.player.position] > 0:
            current_positions[p.player.position] -= 1
    for p in receiving:
        current_positions[p.player.position] += 1

    # Validate against Hard Caps
    for pos, count in current_positions.items():
        limit = HARD_COUNTS.get(pos)
        if limit and count > limit:
            return False

    return True


def calculate_lineup_score(roster: List[PlayerScore]) -> float:
    """
    Calculates the Total Projected Points of a starting lineup.
    Optimizes the roster to pick the best starters for each slot.
    """
    # Sort by weekly value (approximated by composite score/ROS factor)
    # We use composite_score directly as a proxy for "Value"
    roster_sorted = sorted(roster, key=lambda x: x.composite_score, reverse=True)

    starters_score = 0.0
    counts = {k: 0 for k in STARTER_COUNTS}

    for p in roster_sorted:
        pos = p.position
        # Check if slot available
        if counts.get(pos, 99) < STARTER_COUNTS.get(pos, 0):
            starters_score += p.composite_score
            counts[pos] += 1
        else:
            # Bench player - contributes minimal value (depth/insurance)
            starters_score += p.composite_score * 0.10

    return starters_score


def evaluate_trade_scenario(
    my_roster: List[PlayerScore],
    opp_roster: List[PlayerScore],
    send: List[PlayerScore],
    receive: List[PlayerScore],
) -> Dict:
    """
    Simulates the trade and calculates metrics for BOTH sides.
    Enforces "No Free Lunch" logic using Risk-Adjusted Values.
    """
    # --- MY SIDE ---
    my_new_roster = [p for p in my_roster if p not in send] + receive

    # --- OPPONENT SIDE ---
    opp_new_roster = [p for p in opp_roster if p not in receive] + send

    # Lineup Impact
    my_score_before = calculate_lineup_score(my_roster)
    my_score_after = calculate_lineup_score(my_new_roster)
    my_delta = my_score_after - my_score_before

    opp_score_before = calculate_lineup_score(opp_roster)
    opp_score_after = calculate_lineup_score(opp_new_roster)
    opp_delta = opp_score_after - opp_score_before

    # --- ASSET VALUE ---
    val_sent = sum(p.composite_score for p in send)
    val_rcvd = sum(p.composite_score for p in receive)

    # Fairness ratio (Asset value only)
    fairness = min(val_sent, val_rcvd) / max(val_sent, val_rcvd) * 100

    # --- REALISM ENGINE (FIXED: Uses composite_score to include Risk) ---
    # Previous bug: Used vorp_avg, which ignored risk penalties for players like Stone.
    best_sent = max(send, key=lambda x: x.composite_score)
    best_rcvd = max(receive, key=lambda x: x.composite_score)

    is_realistic = True
    rejection_reason = ""

    # 1. LINEUP STABILITY GUARD
    # Reject if opponent lineup score drops significantly.
    # -5.0 is roughly equivalent to losing a low-end starter or half a star.
    if opp_delta < -5.0:
        is_realistic = False
        rejection_reason = f"Opponent lineup collapses ({opp_delta:.1f} pts)"

    # 2. OPPONENT UTILITY CHECK (Soft Guard)
    # If opponent loses ANY lineup points, they must be getting the best asset.
    elif opp_delta < -0.5:
        if best_sent.composite_score <= best_rcvd.composite_score:
            is_realistic = False
            rejection_reason = "Opponent loses points and doesn't get best player"

    # 3. THE "NO FREE LUNCH" UPGRADE RULE
    # If you get the Best Player (Upgrade Quality), you CANNOT also win on Asset Value (Quantity).
    # Threshold: Best received is > 3.0 points better than best sent
    if best_rcvd.composite_score > best_sent.composite_score + 3.0:
        if val_rcvd > val_sent:
            is_realistic = False
            rejection_reason = "You upgrade Quality AND Quantity (Unrealistic)"

    # 4. THE "LATERAL MOVE" CHECK
    # If best players are similar (within 3.0 points), trade must be very even asset-wise
    if abs(best_rcvd.composite_score - best_sent.composite_score) <= 3.0:
        if fairness < 92.0:
            is_realistic = False
            rejection_reason = "Lateral move must be dead even"

    # 5. FLEECE GUARD
    # Absolute floor for asset exchange
    ratio = val_sent / val_rcvd if val_rcvd > 0 else 0
    if ratio < 0.90:  # You pay < 90 cents on the dollar
        is_realistic = False
        rejection_reason = "Underpayment (Fleece)"

    return {
        "my_delta": my_delta,
        "opp_delta": opp_delta,
        "fairness": fairness,
        "is_realistic": is_realistic,
        "val_sent": val_sent,
        "val_rcvd": val_rcvd,
        "rejection_reason": rejection_reason,
    }


def find_trades(
    my_scores, opp_scores, opp_team, target_sell=None, target_buy=None
) -> List[Dict]:
    recommendations = []

    # Define pools
    my_pool = [target_sell] if target_sell else my_scores
    # If target_buy is set, we MUST include them in receive side
    opp_pool = [target_buy] if target_buy else opp_scores[:15]

    # 1-for-1
    for m in my_pool:
        for o in opp_pool:
            # Relaxed pre-filter to find ANY valid trade for target
            if abs(m.composite_score - o.composite_score) > 20:
                continue

            if not check_roster_compliance(my_scores, [m], [o]):
                continue
            if not check_roster_compliance(opp_scores, [o], [m]):
                continue  # Check opponent roster too

            res = evaluate_trade_scenario(my_scores, opp_scores, [m], [o])

            # Relaxed logic for targeted trades: any positive value or fair trade
            if res["is_realistic"]:
                recommendations.append(
                    {
                        "type": "1-for-1",
                        "send": [m],
                        "recv": [o],
                        "opp": opp_team,
                        "stats": res,
                    }
                )

    # 2-for-1 (Consolidation)
    # If target_buy is set, we only look at receiving [target_buy]
    # If target_sell is set, m1 MUST be target_sell

    my_combo_pool = my_scores[2:] if not target_sell else my_scores

    for i, m1 in enumerate(my_combo_pool):
        if target_sell and m1 != target_sell:
            continue

        for m2 in my_combo_pool[i + 1 :]:
            # Relaxed lower bound for package pieces
            if m1.composite_score < 2 or m2.composite_score < 2:
                continue

            for o in opp_pool:
                # Logic: Consolidate UP
                if o.composite_score < max(m1.composite_score, m2.composite_score):
                    continue

                if not check_roster_compliance(my_scores, [m1, m2], [o]):
                    continue
                if not check_roster_compliance(opp_scores, [o], [m1, m2]):
                    continue

                res = evaluate_trade_scenario(my_scores, opp_scores, [m1, m2], [o])

                if res["is_realistic"]:
                    recommendations.append(
                        {
                            "type": "2-for-1",
                            "send": [m1, m2],
                            "recv": [o],
                            "opp": opp_team,
                            "stats": res,
                        }
                    )

    # 2-for-2 (Blockbuster)
    top_my = my_pool[:10] if not target_sell else [target_sell] + my_scores[:8]
    top_opp = opp_pool[:10] if not target_buy else [target_buy] + opp_scores[:8]

    for i, m1 in enumerate(top_my):
        for m2 in top_my[i + 1 :]:
            if target_sell and (m1 != target_sell and m2 != target_sell):
                continue

            for j, o1 in enumerate(top_opp):
                for o2 in top_opp[j + 1 :]:
                    if target_buy and (o1 != target_buy and o2 != target_buy):
                        continue

                    if not check_roster_compliance(my_scores, [m1, m2], [o1, o2]):
                        continue
                    if not check_roster_compliance(opp_scores, [o1, o2], [m1, m2]):
                        continue

                    res = evaluate_trade_scenario(
                        my_scores, opp_scores, [m1, m2], [o1, o2]
                    )
                    if res["is_realistic"]:
                        recommendations.append(
                            {
                                "type": "2-for-2",
                                "send": [m1, m2],
                                "recv": [o1, o2],
                                "opp": opp_team,
                                "stats": res,
                            }
                        )

    return recommendations


def print_rec(rec, rank):
    print(f"\n{'='*60}")
    print(f"TRADE #{rank} ({rec['type']}) vs {rec['opp'].team_name}")
    print(f"{'='*60}")

    print("📤 YOU SEND:")
    for p in rec["send"]:
        risk_tag = "⚠️ RISK" if p.risk_penalty < 1.0 else ""
        print(
            f"   • {p.player.player_name:<20} ({p.position}) | Val: {p.composite_score:.1f} | VORP: {p.vorp_avg:.1f} {risk_tag}"
        )

    print("📥 YOU RECEIVE:")
    for p in rec["recv"]:
        risk_tag = "⚠️ RISK" if p.risk_penalty < 1.0 else ""
        print(
            f"   • {p.player.player_name:<20} ({p.position}) | Val: {p.composite_score:.1f} | VORP: {p.vorp_avg:.1f} {risk_tag}"
        )

    s = rec["stats"]
    print(f"\n📊 IMPACT:")
    print(f"   • Your Lineup Score:  {s['my_delta']:+.1f} pts/week")
    print(f"   • Opp Lineup Score:   {s['opp_delta']:+.1f} pts/week")
    print(f"   • Asset Fairness:     {s['fairness']:.1f}%")


def find_player_interactive(
    session: Session, scores: List[PlayerScore]
) -> Optional[PlayerScore]:
    """
    Interactively prompts the user to find a player in the pro_players table.
    """
    while True:
        search_prompt = "  > Add player name (e.g., C. McDavid) or 'stop': "
        search_name = input(search_prompt).strip()
        if not search_name:
            continue
        if search_name.lower() == "stop":
            return None

        # Search the database
        search_pattern = f"%{search_name}%"
        statement = select(ProPlayers).where(
            func.lower(ProPlayers.player_name).like(search_pattern.lower())
        )
        results = session.exec(statement).all()

        if len(results) == 0:
            print(f"  No players found matching '{search_name}'. Please try again.")
            continue

        selected_player = None
        if len(results) == 1:
            player = results[0]
            confirm_res = input(
                f"  Found: {player.player_name} ({player.team_abbrev}). Select? (Y/n): "
            ).lower()
            if confirm_res in ["", "y", "yes"]:
                selected_player = player
            else:
                print("  Player not selected.")
                continue
        else:
            # More than 1 result, force user to pick
            print(
                f"\n  Found {len(results)} players matching '{search_name}'. Please pick one by ID:"
            )
            print("  " + "-" * 70)
            print(f"  {'ID':<12} | {'Name':<25} | {'Team':<5} | {'#':<3} | {'Pos':<5}")
            print("  " + "-" * 70)
            id_map = {}
            for player in results:
                id_map[str(player.player_id)] = player
                print(
                    f"  {player.player_id:<12} | {player.player_name:<25} | {player.team_abbrev:<5} | {player.jersey_number:<3} | {player.position:<5}"
                )
            print("  " + "-" * 70)

            while True:
                choice_id = input(
                    "  > Enter Player ID to select (or 'cancel'): "
                ).strip()
                if choice_id.lower() == "cancel":
                    break
                chosen_player = id_map.get(choice_id)
                if chosen_player:
                    print(f"  Selected: {chosen_player.player_name}")
                    selected_player = chosen_player
                    break
                else:
                    print(f"  Invalid ID '{choice_id}'. Please try again.")

        if selected_player:
            # Match ProPlayer result to PlayerScore object
            match = next(
                (s for s in scores if s.player.player_id == selected_player.player_id),
                None,
            )
            if match:
                return match
            else:
                print(
                    "Player found in DB but not in provided score list (likely on another team)."
                )
                return None


def find_any_player_interactive(session: Session) -> Optional[ProPlayers]:
    """
    Interactively prompts the user to find a player in the pro_players table.
    """
    while True:
        search_prompt = "  > Add player name (e.g., C. McDavid) or 'stop': "
        search_name = input(search_prompt).strip()
        if not search_name:
            continue
        if search_name.lower() == "stop":
            return None

        # Search the database
        search_pattern = f"%{search_name}%"
        statement = select(ProPlayers).where(
            func.lower(ProPlayers.player_name).like(search_pattern.lower())
        )
        results = session.exec(statement).all()

        if len(results) == 0:
            print(f"  No players found matching '{search_name}'. Please try again.")
            continue

        if len(results) == 1:
            player = results[0]
            confirm_res = input(
                f"  Found: {player.player_name} ({player.team_abbrev}). Select? (Y/n): "
            ).lower()
            if confirm_res in ["", "y", "yes"]:
                return player
            else:
                print("  Player not selected.")
                continue

        # More than 1 result, force user to pick
        print(
            f"\n  Found {len(results)} players matching '{search_name}'. Please pick one by ID:"
        )
        print("  " + "-" * 70)
        print(f"  {'ID':<12} | {'Name':<25} | {'Team':<5} | {'#':<3} | {'Pos':<5}")
        print("  " + "-" * 70)
        id_map = {}
        for player in results:
            id_map[str(player.player_id)] = player
            print(
                f"  {player.player_id:<12} | {player.player_name:<25} | {player.team_abbrev:<5} | {player.jersey_number:<3} | {player.position:<5}"
            )
        print("  " + "-" * 70)

        while True:
            choice_id = input("  > Enter Player ID to select (or 'cancel'): ").strip()
            if choice_id.lower() == "cancel":
                break
            chosen_player = id_map.get(choice_id)
            if chosen_player:
                print(f"  Selected: {chosen_player.player_name}")
                return chosen_player
            else:
                print(f"  Invalid ID '{choice_id}'. Please try again.")


async def main():
    print("FANTASY TRADE ANALYZER (v5.1 - RELAXED FOR SPECIFIC TARGETS)")
    with Session(engine) as session:
        # 1. League Context
        top_games = session.exec(
            select(func.count(PlayerGameStats.game_id))
            .where(PlayerGameStats.season == SEASON_ID)
            .group_by(PlayerGameStats.player_id)
            .order_by(func.count(PlayerGameStats.game_id).desc())
            .limit(5)
        ).all()
        league_max = top_games[2] if top_games else 0

        # 2. Select Team
        teams = session.exec(select(FantasyTeam)).all()
        for i, t in enumerate(teams, 1):
            print(f"{i}. {t.team_name}")
        try:
            my_team = teams[int(input("Select Your Team: ")) - 1]
        except:
            return

        # 3. Score Everyone
        all_players_scores = {}  # Map TeamID -> List[PlayerScore]
        for t in teams:
            r = session.exec(
                select(ProPlayers).where(ProPlayers.fantasy_team_id == t.team_id)
            ).all()
            all_players_scores[t.team_id] = await score_all_players(
                session, r, league_max
            )

        my_scores = all_players_scores[my_team.team_id]

        # 4. Menu
        print("\n1. Auto-Find Best Trades")
        print("2. Shop Specific Player (Sell)")
        print("3. Target Specific Player (Buy)")
        mode = input("> ")

        target_sell = None
        target_buy = None

        if mode == "2":
            target_sell = find_player_interactive(session, my_scores)
        elif mode == "3":
            p_raw = find_any_player_interactive(session)
            if p_raw:
                tid = p_raw.fantasy_team_id
                if tid and tid in all_players_scores:
                    target_buy = next(
                        (
                            x
                            for x in all_players_scores[tid]
                            if x.player.player_id == p_raw.player_id
                        ),
                        None,
                    )

        # 5. Run Analysis
        all_recs = []
        for opp_team in teams:
            if opp_team.team_id == my_team.team_id:
                continue
            if target_buy and opp_team.team_id != target_buy.player.fantasy_team_id:
                continue

            opp_scores = all_players_scores[opp_team.team_id]

            recs = find_trades(my_scores, opp_scores, opp_team, target_sell, target_buy)
            all_recs.extend(recs)

        # Sort by Impact
        all_recs.sort(key=lambda x: x["stats"]["my_delta"], reverse=True)

        if not all_recs:
            print("\nNo realistic trades found. (Strict logic is filtering bad trades)")
        for i, r in enumerate(all_recs[:10], 1):
            print_rec(r, i)


if __name__ == "__main__":
    asyncio.run(main())
