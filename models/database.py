from sqlmodel import SQLModel, Field, Index
from typing import Optional


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
    opponent_abbrev: str

    # Player info (useful for querying/display)
    player_name: Optional[str] = None

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
        Index(
            "idx_player_team", "player_id", "team_abbrev"
        ),  # New: query players by team
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
    opponent_abbrev: str

    # Player info
    player_name: Optional[str] = None

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
        Index("idx_goalie_player_team", "player_id", "team_abbrev"),
    )
