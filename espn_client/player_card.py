from pydantic import BaseModel, model_validator, computed_field
from typing import List, Dict, Any, Optional

# --- IMPORTANT ---
# Make sure your new constants are in a file (e.g., espn_constants.py)
# and import them here.
try:
    from espn_constants import *
except ImportError:
    print("Warning: espn_constants.py not found. Using empty maps.")
    PRO_TEAM_MAP = {}
    STATS_MAP = {}

# --- Nested Models (Unchanged) ---
class Ownership(BaseModel):
    percentOwned: float
    percentStarted: float
    averageDraftPosition: float

# --- PlayerStats Model (Now with mapped stats!) ---
class PlayerStats(BaseModel):
    id: str
    seasonId: int
    statSplitTypeId: int
    appliedTotal: float
    stats: Dict[str, Any]  # This is the raw data, e.g., {"13": 7.0}

    # NEW: This creates a readable dictionary from the "stats" field
    @computed_field
    @property
    def mapped_stats(self) -> Dict[str, Any]:
        """
        Translates raw stat IDs (like "13") into readable names (like "G").
        """
        mapped = {}
        for stat_id, value in self.stats.items():
            # Use STATS_MAP, but fallback to the ID if not found
            stat_name = STATS_MAP.get(stat_id, f"unknown_stat_{stat_id}")
            mapped[stat_name] = value
        return mapped
    
    @computed_field
    @property
    def stat_split_name(self) -> str:
        """
        Parses the 'id' (e.g., "022026") into a readable string
        (e.g., "Last 15, Season 2026").
        """
        if len(self.id) < 6:
            return "Unknown Split"

        split_key = self.id[:2]  # "02"
        season_str = self.id[2:]  # "2026"

        # Use STATS_IDENTIFIER map
        split_name = STATS_IDENTIFIER.get(split_key, "Unknown Split")

        return f"{split_name}, Season {season_str}"

# --- PlayerInfo Model (Now with team name!) ---
class PlayerInfo(BaseModel):
    id: int
    fullName: str
    jersey: str
    proTeamId: int
    injuryStatus: str
    stats: List[PlayerStats]
    ownership: Ownership
    
    # NEW: Define a new field that will be populated
    pro_team_name: Optional[str] = None

    # NEW: This validator runs after the model is created
    # and populates the 'pro_team_name' field
    @model_validator(mode='after')
    def set_pro_team_name(self) -> 'PlayerInfo':
        if self.proTeamId != 0:
            # Use PRO_TEAM_MAP, but fallback to "Unknown" if not found
            self.pro_team_name = PRO_TEAM_MAP.get(self.proTeamId, "Unknown Team")
        else:
            self.pro_team_name = "No Team"
        return self

# --- Top-Level Models (Unchanged) ---
class Player(BaseModel):
    id: int
    onTeamId: int
    status: str
    player: PlayerInfo

class PlayerCard(BaseModel):
    players: List[Player]