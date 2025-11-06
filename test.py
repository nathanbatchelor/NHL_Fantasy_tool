# 1.  **Player Game Log:** `GET /v1/player/{player}/game-log/{season}/{game-type}`
#     * **Why it's better:** Your current file only has `last5Games`. This endpoint gives you the *entire season game-by-game*. You can see trends, average shots over the last 10 games, or if a player's ice time is increasing. This is far more powerful for a model.

# 2.  **Team Schedule:** `GET /v1/club-schedule-season/{team}/now`
#     * **Why it's essential:** This tells you the upcoming schedule for a player's team. A star player with 4 games in a week is almost always a better fantasy pick than an equal player with only 2 games. Your model *must* know this.


## How to Filter Your Current JSON File

import json
import csv


def filter_player_data(raw_data_file, output_csv_file):
    """
    Loads the detailed player JSON, filters it for fantasy-relevant data,
    engineers new features, and saves it to a clean CSV.
    """

    print(f"Loading data from {raw_data_file}...")
    try:
        with open(raw_data_file, "r", encoding="utf-8") as f:
            players = json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        return

    print(f"Found {len(players)} players. Filtering for model training...")

    filtered_players = []

    for player in players:
        # 1. Select Basic Info
        basic_info = {
            "playerId": player.get("id"),
            "firstName": player.get("firstName"),
            "lastName": player.get("lastName"),
            "teamAbbrev": player.get("teamAbbrev"),
            "positionCode": player.get("positionCode"),
        }

        # 2. Get Current Season Stats (from 'seasonTotals' list)
        current_season_stats = {}
        # 'seasonTotals' is a list, the last item is the most recent season
        if player.get("seasonTotals") and len(player["seasonTotals"]) > 0:
            s_stats = player["seasonTotals"][-1]  # Get last season in list

            # Only include stats for skaters (not goalies)
            if basic_info["positionCode"] != "G":
                current_season_stats = {
                    "season_gamesPlayed": s_stats.get("gamesPlayed", 0),
                    "season_goals": s_stats.get("goals", 0),
                    "season_assists": s_stats.get("assists", 0),
                    "season_points": s_stats.get("points", 0),
                    "season_plusMinus": s_stats.get("plusMinus", 0),
                    "season_ppp": s_stats.get("powerPlayPoints", 0),
                    "season_shots": s_stats.get("shots", 0),
                    "season_avgToi": s_stats.get("avgToi", "0:00"),
                    "season_hits": s_stats.get("hits", 0),
                    "season_blocks": s_stats.get("blockedShots", 0),
                }
            else:  # Goalie-specific stats
                current_season_stats = {
                    "season_gamesPlayed": s_stats.get("gamesPlayed", 0),
                    "season_wins": s_stats.get("wins", 0),
                    "season_losses": s_stats.get("losses", 0),
                    "season_otLosses": s_stats.get("otLosses", 0),
                    "season_savePctg": s_stats.get("savePctg", 0),
                    "season_gaa": s_stats.get("gaa", 0),
                    "season_shutouts": s_stats.get("shutouts", 0),
                }

        # 3. Aggregate Last 5 Games (Feature Engineering)
        last_5_stats = {
            "goalsInLast5": 0,
            "assistsInLast5": 0,
            "pointsInLast5": 0,
            "shotsInLast5": 0,
            "hitsInLast5": 0,  # Note: 'hits' is not in the last5Games keys
            "blocksInLast5": 0,  # Note: 'blockedShots' is not in the last5Games keys
        }

        if player.get("last5Games") and basic_info["positionCode"] != "G":
            for game in player["last5Games"]:
                last_5_stats["goalsInLast5"] += game.get("goals", 0)
                last_5_stats["assistsInLast5"] += game.get("assists", 0)
                last_5_stats["pointsInLast5"] += game.get("points", 0)
                last_5_stats["shotsInLast5"] += game.get("shots", 0)
                # Note: Hits and Blocks are not in the last5Games data,
                # you would need the full game-log endpoint for that.

        # 4. Combine all into one dictionary
        # We only add stats if they exist
        if current_season_stats:
            combined_stats = {**basic_info, **current_season_stats, **last_5_stats}
            filtered_players.append(combined_stats)

    # 5. Save to CSV
    if not filtered_players:
        print("No players with valid stats found to save.")
        return

    # Get headers from the first player
    headers = filtered_players[0].keys()

    try:
        with open(output_csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(filtered_players)

        print(f"\n✅ Success! Filtered {len(filtered_players)} players.")
        print(f"Saved clean data to {output_csv_file}")

    except Exception as e:
        print(f"Error writing CSV file: {e}")


if __name__ == "__main__":
    filter_player_data(
        raw_data_file="nhl_players_detailed_stats.json",
        output_csv_file="fantasy_training_data.csv",
    )
