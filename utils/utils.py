"""
utils/utils.py
General-purpose utility functions, like caching.
(No changes were needed here)
"""

import json
import os
from pathlib import Path


def save_data_to_cache(data: list | dict, cache_file: str):
    """Saves fetched data to a JSON cache file."""
    try:
        Path(cache_file).parent.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"  ✓ Cached data to {cache_file}")
    except Exception as e:
        print(f"  ! Error saving cache to {cache_file}: {e}")


def load_data_from_cache(cache_file: str) -> list | dict | None:
    """Loads data from a JSON cache file if it exists."""
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                print(f"  ✓ Loading from cache: {cache_file}")
                return json.load(f)
        except Exception as e:
            print(f"  ! Error loading cache from {cache_file}: {e}")
            return None
    return None
