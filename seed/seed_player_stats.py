"""
seed/seed_player_stats.py
Controller script to seed all player summary stats.
"""

import csv
import sys
from pathlib import Path
import constants

# Import from the new utility files
from utils.nhl_api_utils import fetch_stats_data
from utils.utils import save_data_to_cache, load_data_from_cache
from utils.transforms import combine_and_get_skater_data, process_goalie_data


def get_all_player_data(force_refresh: bool = False):
    """
    Main controller to fetch all player data, using cache if available.
    """
    skater_summary_cache_file = constants.SKATER_SUMMARY_CACHE
    skater_realtime_cache_file = constants.SKATER_REALTIME_CACHE
    goalie_summary_cache_file = constants.GOALIE_SUMMARY_CACHE

    if not force_refresh:
        print("Checking for cached data...")
        skater_summary_data = load_data_from_cache(skater_summary_cache_file)
        skater_realtime_data = load_data_from_cache(skater_realtime_cache_file)
        goalie_summary_data = load_data_from_cache(goalie_summary_cache_file)

        if skater_summary_data and skater_realtime_data and goalie_summary_data:
            print("✓ All data loaded from cache.")
            return skater_summary_data, skater_realtime_data, goalie_summary_data
        else:
            print("  ! Cache missing or incomplete. Fetching fresh data...")

    print(f"Fetching fresh data for {constants.SEASON_ID}...")
    skater_summary_url = f"{constants.ALL_PlAYERS_URL}/skater/summary?limit=-1&cayenneExp=seasonId={constants.SEASON_ID}"
    skater_realtime_url = f"{constants.ALL_PlAYERS_URL}/skater/realtime?limit=-1&cayenneExp=seasonId={constants.SEASON_ID}"
    goalie_summary_url = f"{constants.ALL_PlAYERS_URL}/goalie/summary?limit=-1&cayenneExp=seasonId={constants.SEASON_ID}"

    skater_summary_data = fetch_stats_data(skater_summary_url)
    skater_realtime_data = fetch_stats_data(skater_realtime_url)
    goalie_summary_data = fetch_stats_data(goalie_summary_url)

    if skater_summary_data:
        save_data_to_cache(skater_summary_data, skater_summary_cache_file)
    if skater_realtime_data:
        save_data_to_cache(skater_realtime_data, skater_realtime_cache_file)
    if goalie_summary_data:
        save_data_to_cache(goalie_summary_data, goalie_summary_cache_file)

    return skater_summary_data, skater_realtime_data, goalie_summary_data


if __name__ == "__main__":
    force_refresh = "--force" in sys.argv
    skater_summary, realtime, goalie_summary = get_all_player_data(force_refresh)

    if not skater_summary or not realtime or not goalie_summary:
        print("Error: Could not fetch data. Exiting.")
        sys.exit(1)

    print("Data loaded. Now merging and processing...")

    combined_skater_list = combine_and_get_skater_data(skater_summary, realtime)
    print(f"Successfully merged {len(combined_skater_list)} players.")

    filtered_goalie_list = process_goalie_data(goalie_summary)
    print(f"Successfully processed {len(filtered_goalie_list)} goalies.")

    # Save to CSV
    try:
        skater_csv_path = constants.SKATER_STATS_CSV
        goalie_csv_path = constants.GOALIE_STATS_CSV
        Path(skater_csv_path).parent.mkdir(parents=True, exist_ok=True)

        # (Your CSV saving logic remains here)
        if combined_skater_list:
            headers = combined_skater_list[0].keys()
            with open(skater_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(combined_skater_list)
            print(f"✅ Successfully saved merged stats to {skater_csv_path}")

        if filtered_goalie_list:
            headers = filtered_goalie_list[0].keys()
            with open(goalie_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(filtered_goalie_list)
            print(f"✅ Successfully saved goalie stats to {goalie_csv_path}")

    except Exception as e:
        print(f"Error saving CSV: {e}")
