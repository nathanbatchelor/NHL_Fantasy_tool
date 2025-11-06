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


