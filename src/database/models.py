from sqlmodel import SQLModel, Field, Index, Relationship
from typing import Optional, List


class TeamSchedule(SQLModel, table=True):
    """
    Stores weekly schedule data for each NHL team.
    Loaded from your team_schedule.csv file.
    """

    __tablename__ = "team_schedule"

    id: int = Field(default=None, primary_key=True)
    team: str = Field(index=True)
    week: str = Field(index=True)  # e.g., '2025-W45'
    monday_date: str  # '2025-11-03'
    sunday_date: str  # '2025-11-09'
    game_count: int = 0
    opponents: str  # e.g., "@ DAL, @ VGK, vs WPG"

    __table_args__ = (Index("idx_team_week", "team", "week"),)


# --- NEW: FantasyTeam Table ---
class FantasyTeam(SQLModel, table=True):
    """
    Represents a single fantasy team in your league.
    """

    __tablename__ = "fantasy_team"

    team_id: int = Field(default=None, primary_key=True)
    team_name: str
    owner_name: str = Field(index=True)
    # This creates the "roster" relationship
    # It will be a list of ProPlayers objects
    players: List["ProPlayers"] = Relationship(back_populates="fantasy_team")


# --- NEW: ProPlayers Table (Replaces PlayerMap) ---
class ProPlayers(SQLModel, table=True):
    """
    Master table for every unique pro player (NHL).
    This is the "Rosetta Stone" that links player_id (NHL) to espn_id.
    It also stores their info, fantasy team, and accumulated season stats.
    """

    __tablename__ = "pro_players"

    # --- Core Player Info ---
    player_id: int = Field(primary_key=True)  # This is the NHL ID
    espn_id: Optional[int] = Field(default=None, unique=True, index=True)
    player_name: str = Field(index=True)
    team_abbrev: Optional[str] = Field(default=None, index=True)  # Current NHL team
    position: Optional[str] = Field(default=None, index=True)  # e.g., C, LW, D, G
    jersey_number: Optional[int] = None
    is_active: bool = Field(default=True)  # To filter out retired players
    is_goalie: bool = Field(default=False)

    # --- Status Info (from ESPN) ---
    injury_status: Optional[str] = Field(default=None)  # e.g., DAY_TO_DAY, OUT

    # --- Fantasy Team Relationship ---
    # This links a player to a fantasy team.
    # If fantasy_team_id is NULL, the player is a Free Agent.
    fantasy_team_id: Optional[int] = Field(
        default=None, foreign_key="fantasy_team.team_id"
    )
    fantasy_team: Optional[FantasyTeam] = Relationship(back_populates="players")

    # --- Accumulated Season Stats (for Skaters) ---
    season_games_played: int = Field(default=0)
    season_total_fpts: float = Field(default=0.0)
    season_goals: int = Field(default=0)
    season_assists: int = Field(default=0)
    season_pp_points: float = Field(default=0.0)
    season_sh_points: float = Field(default=0.0)
    season_shots: int = Field(default=0)
    season_blocked_shots: int = Field(default=0)
    season_hits: int = Field(default=0)

    # --- Accumulated Season Stats (for Goalies) ---
    season_wins: int = Field(default=0)
    season_shutouts: int = Field(default=0)
    season_ot_losses: int = Field(default=0)
    season_saves: int = Field(default=0)
    season_goals_against: int = Field(default=0)

    prior_season_avg_fpts: float = Field(default=0.0)
    prior_season_games_played: int = Field(default=0)

    # --- PREDICTED STATS (FOR ML) ---
    predicted_fpts: float = Field(default=0.0)


class PlayerGameStats(SQLModel, table=True):
    """Player statistics for individual games"""

    __tablename__ = "player_game_stats"

    # Composite primary key
    game_id: int = Field(primary_key=True)
    player_id: int = Field(primary_key=True)

    # Game context
    game_date: str  # ISO format: "2025-11-09"
    season: str = Field(index=True)
    team_abbrev: str  # Player's team
    team_name: str
    opponent_abbrev: str
    opponent_name: str

    # Player info (useful for querying/display)
    player_name: Optional[str] = None
    jersey_number: Optional[int] = None
    position: Optional[str] = Field(default=None, index=True)

    # Scoring stats
    goals: int = 0
    assists: int = 0
    pp_points: float = 0.0  # Power play points
    sh_points: float = 0.0  # Short-handed points

    # Other stats
    shots: int = 0
    shooting_pct: float | None = None
    blocked_shots: int = 0
    hits: int = 0

    toi_seconds: int = 0
    shifts: int = 0

    # Calculated fantasy points
    total_fpts: float = 0.0

    # Add indexes for common queries
    __table_args__ = (
        Index("idx_player_date", "player_id", "game_date"),
        Index("idx_game_date", "game_date"),
        Index("idx_team", "team_abbrev"),
        Index("idx_team_name", "team_name"),
        Index("idx_player_team", "player_id", "team_abbrev"),
    )


class GoalieGameStats(SQLModel, table=True):
    """Goalie statistics for individual games"""

    __tablename__ = "goalie_game_stats"

    # Composite primary key
    game_id: int = Field(primary_key=True)
    player_id: int = Field(primary_key=True)

    # Game context
    game_date: str
    season: str = Field(index=True)
    team_abbrev: str  # Goalie's team
    team_name: str  # Goalie's team full name
    opponent_abbrev: str
    opponent_name: str  # Opponent's team full name

    # Player info
    player_name: Optional[str] = None
    jersey_number: Optional[int] = None
    position: Optional[str] = Field(default=None, index=True)

    # Goalie stats
    saves: int = 0
    save_pct: float = 0.0
    goals_against: int = 0
    decision: Optional[str] = Field(default=None)  # W, L, or OT

    # Calculated stats for points
    wins: int = 0
    shutouts: int = 0
    ot_losses: int = 0

    # Calculated fantasy points
    total_fpts: float = 0.0

    # Add indexes for common queries
    __table_args__ = (
        Index("idx_goalie_player_date", "player_id", "game_date"),
        Index("idx_goalie_game_date", "game_date"),
        Index("idx_goalie_team", "team_abbrev"),
        Index("idx_goalie_team_name", "team_name"),
        Index("idx_goalie_player_team", "player_id", "team_abbrev"),
    )
