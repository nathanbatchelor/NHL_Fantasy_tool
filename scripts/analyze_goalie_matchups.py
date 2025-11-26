"""
analyze_goalie_matchups.py

Finds available goalies (Free Agents & Team 1) and analyzes their
upcoming schedule (next 14 days) based on opponent offensive strength.

Opponent strength is calculated from historical data in the local DB
(Average Goals Scored per Game by the opponent against goalies).
"""

import asyncio
import pandas as pd
import pytz
from datetime import datetime, timedelta
from sqlmodel import Session, select, text

# Local imports
from src.database.database import engine, init_db
from src.database.models import ProPlayers, GoalieGameStats
from src.core.constants import SEASON_ID, FANTASY_TIMEZONE
from src.api.nhl_api_utils import get_schedule
from src.utils.date_utils import get_schedule_by_date

# Configuration
LOOKAHEAD_DAYS = 14
# Thresholds for grading (relative to league average)
THRESHOLD_EASY = -0.15  # Opponent scores 0.15 less than avg
THRESHOLD_HARD = 0.15  # Opponent scores 0.15 more than avg


def get_opponent_offense_stats(session: Session) -> pd.DataFrame:
    """
    Calculates the Average Goals Scored per Game (GF/GP) for each team
    based on 'goals_against' recorded by goalies facing them.
    """
    # We first sum goals per game (in case a team faced 2 goalies in 1 game)
    query = text(
        """
        WITH GameGoals AS (
            SELECT 
                game_id,
                opponent_abbrev,
                SUM(goals_against) as game_goals
            FROM goalie_game_stats
            WHERE season = :season
            GROUP BY game_id, opponent_abbrev
        )
        SELECT
            opponent_abbrev as team,
            AVG(game_goals) as avg_goals_for,
            COUNT(game_id) as games_played
        FROM GameGoals
        GROUP BY opponent_abbrev
    """
    )

    df = pd.read_sql(query, session.connection(), params={"season": SEASON_ID})
    return df


async def main():
    init_db()

    print("\n" + "=" * 70)
    print(f"🥅 GOALIE MATCHUP ANALYZER (Next {LOOKAHEAD_DAYS} Days)")
    print("=" * 70)

    # 1. Calculate Team Offensive Strength (Opponent Difficulty)
    with Session(engine) as session:
        print("1. Calculating opponent offensive strength from DB...")
        opp_stats = get_opponent_offense_stats(session)

        if opp_stats.empty:
            print(
                "❌ No goalie stats found. Try running 'update_daily_stats.py' first."
            )
            return

        league_avg_gf = opp_stats["avg_goals_for"].mean()
        print(f"   League Average Goals/Game: {league_avg_gf:.2f}")

        # Create a map for easy lookup: { 'BOS': 3.2, ... }
        team_difficulty = dict(zip(opp_stats["team"], opp_stats["avg_goals_for"]))

    # 2. Get Upcoming Schedule
    print(f"2. Fetching schedule for the next {LOOKAHEAD_DAYS} days...")
    tz = pytz.timezone(FANTASY_TIMEZONE)
    today = datetime.now(tz)

    schedule_by_id = await get_schedule()
    schedule_by_date = get_schedule_by_date(schedule_by_id)

    # Filter schedule for the lookahead window
    upcoming_games = {}  # { '2025-11-25': [Game1, Game2...], ... }

    dates_to_check = []
    for i in range(1, LOOKAHEAD_DAYS + 1):  # Start from tomorrow
        date_obj = today + timedelta(days=i)
        date_str = date_obj.strftime("%Y-%m-%d")
        dates_to_check.append(date_str)
        if date_str in schedule_by_date:
            upcoming_games[date_str] = schedule_by_date[date_str]

    # 3. Get Available Goalies (Free Agent or Team 1)
    with Session(engine) as session:
        print("3. Finding available goalies...")
        # Get goalies who are Active and (Free Agent OR Team 1)
        # Note: We check for "G" or "Goalie"
        goalies = session.exec(
            select(ProPlayers).where(
                (ProPlayers.position == "G") | (ProPlayers.position == "Goalie"),
                ProPlayers.is_active == True,
                (ProPlayers.fantasy_team_id == None)
                | (ProPlayers.fantasy_team_id == 1),
            )
        ).all()

        print(f"   Found {len(goalies)} goalies to analyze.")

        # 4. Analyze Matchups
        print("4. Analyzing matchups...")
        results = []

        for goalie in goalies:
            team = goalie.team_abbrev
            if not team:
                continue

            # Find games for this goalie's team
            my_schedule = []
            green_matchups = 0
            red_matchups = 0

            for d_str in dates_to_check:
                games_on_date = upcoming_games.get(d_str, [])
                for game in games_on_date:
                    # Check if goalie's team is playing
                    opponent = None
                    is_home = False

                    if game["home_abbrev"] == team:
                        opponent = game["away_abbrev"]
                        is_home = True
                    elif game["away_abbrev"] == team:
                        opponent = game["home_abbrev"]
                        is_home = False

                    if opponent:
                        # Grade the matchup
                        opp_gf = team_difficulty.get(opponent, league_avg_gf)
                        diff = opp_gf - league_avg_gf

                        grade = "🟡"  # Neutral
                        if diff < THRESHOLD_EASY:
                            grade = "🟢"  # Easy (Weak offense)
                            green_matchups += 1
                        elif diff > THRESHOLD_HARD:
                            grade = "🔴"  # Hard (Strong offense)
                            red_matchups += 1

                        # Format: "vs BOS (🔴)" or "@ TOR (🟢)"
                        loc = "vs" if is_home else "@"
                        my_schedule.append(f"{grade} {loc} {opponent}")

            # Only include if they have games
            if my_schedule:
                # Basic Score: Green - Red (just for sorting)
                sort_score = green_matchups - (red_matchups * 0.5)

                results.append(
                    {
                        "name": goalie.player_name,
                        "team": team,
                        "status": "FA" if not goalie.fantasy_team_id else "Team 1",
                        "games_count": len(my_schedule),
                        "green": green_matchups,
                        "schedule_str": ", ".join(my_schedule),
                        "sort_score": sort_score,
                    }
                )

    # 5. Display Report
    # Sort by 'Best Schedule' (most green matchups, then total games)
    results.sort(key=lambda x: (x["sort_score"], x["games_count"]), reverse=True)

    print("\n" + "=" * 90)
    print(f" TOP GOALIE STREAMING OPTIONS (Sorted by Matchup Favorability)")
    print(
        f" Legend: 🟢 = Weak Offense (Easy), 🔴 = Strong Offense (Hard), 🟡 = Average"
    )
    print("=" * 90)
    print(
        f"{'Player':<25} {'Team':<5} {'St':<4} {'Gms':<3} {'Schedule (Next 14 Days)'}"
    )
    print("-" * 90)

    for r in results[:30]:  # Top 30
        # Truncate schedule string if too long
        sched = r["schedule_str"]
        if len(sched) > 50:
            sched = sched[:47] + "..."

        print(
            f"{r['name']:<25} {r['team']:<5} {r['status']:<4} {r['games_count']:<3} {sched}"
        )

    print("\n✅ Analysis Complete.")


if __name__ == "__main__":
    asyncio.run(main())
