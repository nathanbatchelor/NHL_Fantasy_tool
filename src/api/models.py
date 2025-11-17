from typing import Optional
from pydantic import BaseModel


class LocalizedName(BaseModel):
    default: str
    fr: Optional[str] = None


class Team(BaseModel):
    id: int
    commonName: LocalizedName
    placeName: LocalizedName
    placeNameWithPreposition: Optional[LocalizedName] = None
    abbrev: str
    logo: str
    darkLogo: Optional[str] = None
    awaySplitSquad: Optional[bool] = False
    score: Optional[int] = None


class Game(BaseModel):
    id: int
    season: int
    gameType: int
    gameDate: str
    startTimeUTC: str
    easternUTCOffset: str
    venueUTCOffset: str
    venueTimezone: str
    gameState: str
    gameScheduleState: str
    awayTeam: Team
    homeTeam: Team


class GamesResponse(BaseModel):
    games: list[Game]


# --- Models for Boxscore (has everything except PP/SH points) ---


class TeamInfoAPI(BaseModel):
    """Basic team info"""

    abbrev: str
    commonName: dict


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
    sweaterNumber: Optional[int] = None


class GoalieStatsFromBoxscore(BaseModel):
    """Goalie stats from boxscore"""

    playerId: int
    name: dict  # {"default": "Goalie Name"}
    position: str
    saves: int = 0
    savePctg: float = 0
    goalsAgainst: int = 0
    decision: Optional[str] = None  # "W", "L", or None
    sweaterNumber: Optional[int] = None


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
    teamAbbrev: str
    gameDate: str

    # Analysis related stats
    powerPlayPoints: int = 0
    shorthandedPoints: int = 0
    toi: str
    shifts: int = 0
    pim: int = 0


class PlayerGameLogResponse(BaseModel):
    """Player game log API response"""

    gameLog: list[PlayerGameLogEntry]


class FinalPlayerGameStats(BaseModel):
    """
    Our new model to combine stats from the PlayerLog (Phase 1)
    and the Boxscore (Phase 2).
    """

    # Common keys
    playerId: int
    gameId: int
    teamAbbrev: str
    gameDate: str

    # From PlayerGameLogEntry (Phase 1)
    powerPlayPoints: int = 0
    shorthandedPoints: int = 0
    toi: str = "00:00"
    shifts: int = 0
    pim: int = 0

    # From PlayerStatsFromBoxscore (Phase 2)
    name: str = "N/A"
    position: str = "N/A"
    goals: int = 0
    assists: int = 0
    sog: int = 0
    blockedShots: int = 0
    hits: int = 0
    sweaterNumber: Optional[int] = None

    # From GoalieStatsFromBoxscore (Phase 2)
    saves: Optional[int] = None
    savePctg: Optional[float] = None
    goalsAgainst: Optional[int] = None
    decision: Optional[str] = None
