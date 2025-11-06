from pydantic import BaseModel
from typing import Optional


# --- Models for Boxscore (has everything except PP/SH points) ---


class TeamInfoAPI(BaseModel):
    """Basic team info"""

    abbrev: str


class PlayerStatsFromBoxscore(BaseModel):
    """Player stats from boxscore - has everything except PP/SH points"""

    playerId: int
    name: dict  # {"default": "Player Name"}
    position: str
    goals: int = 0
    assists: int = 0
    sog: int = 0  # shots on goal
    blockedShots: int = 0
    hits: int = 0


class GoalieStatsFromBoxscore(BaseModel):
    """Goalie stats from boxscore"""

    playerId: int
    name: dict  # {"default": "Goalie Name"}
    position: str
    saves: int = 0
    goalsAgainst: int = 0
    decision: Optional[str] = None  # "W", "L", or None


class TeamStatsFromBoxscore(BaseModel):
    """Team stats from game"""

    forwards: list[PlayerStatsFromBoxscore] = []
    defense: list[PlayerStatsFromBoxscore] = []
    goalies: list[GoalieStatsFromBoxscore] = []


class PlayerStatsByTeam(BaseModel):
    """Player stats by team"""

    awayTeam: TeamStatsFromBoxscore
    homeTeam: TeamStatsFromBoxscore


class GameBoxscoreResponse(BaseModel):
    """Game boxscore response"""

    id: int
    gameDate: str
    awayTeam: TeamInfoAPI
    homeTeam: TeamInfoAPI
    playerByGameStats: PlayerStatsByTeam


# --- Models for Player Game Log (for PP/SH points AND team verification) ---


class PlayerGameLogEntry(BaseModel):
    """Single game entry - get PP/SH points AND team abbrev"""

    gameId: int
    teamAbbrev: str  # This is what we want!
    powerPlayPoints: int = 0
    shorthandedPoints: int = 0


class PlayerGameLogResponse(BaseModel):
    """Player game log API response"""

    gameLog: list[PlayerGameLogEntry]
