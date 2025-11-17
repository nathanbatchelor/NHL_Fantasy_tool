import requests
import json
from .espn_constants import FANTASY_BASE_ENDPOINT, NEWS_BASE_ENDPOINT


# --- THIS IS YOUR API CLIENT ---
class EspnRequests:

    def __init__(self, league_id: int, swid: str, espn_s2: str, year: int = 2026):
        """
        Initializes the API client with credentials and a reusable session.
        """
        self.league_id = league_id
        self.year = year

        # 1. Define your base endpoints
        self.league_endpoint = f"{FANTASY_BASE_ENDPOINT}/fhl/seasons/{self.year}/segments/0/leagues/{self.league_id}"
        self.season_endpoint = f"{FANTASY_BASE_ENDPOINT}/fhl/seasons/{self.year}"

        # 2. Create a requests.Session()
        # This will hold your cookies and reuse connections
        self.session = requests.Session()

        # 3. Set the cookies on the session
        # Now, every request made with self.session will send these cookies
        cookies = {"SWID": swid, "espn_s2": espn_s2}
        self.session.cookies.update(cookies)

    def _get(self, url: str, params: dict = None, headers: dict = None):
        """
        Internal 'GET' method that uses the session and handles errors.
        """
        try:
            # Use the session, which already has cookies
            response = self.session.get(url, params=params, headers=headers)

            # This is a best practice: it raises an error for 4xx/5xx responses
            response.raise_for_status()

            # Return the JSON data
            return response.json()

        except requests.exceptions.HTTPError as e:
            # You can handle specific errors, like the 401
            print(f"HTTP Error: {e}")
            if e.response.status_code == 401:
                print("Authentication Failed: Check your SWID and espn_s2 cookies.")
            # We'll discuss this: you might try the 'leagueHistory' endpoint here
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            return None

    # --- NOW YOU CAN ADD YOUR SPECIFIC METHODS ---
    def get_my_team_info(self):
        """
        Example method: Gets your team info
        """
        params = {"view": "mTeam"}
        return self._get(url=self.league_endpoint, params=params)

    def get_pro_players(self):
        """Gets all active professional players for the season."""

        params = {"view": "players_wl"}

        filters = {"filterActive": {"value": True}}
        headers = {"x-fantasy-filter": json.dumps(filters)}

        url = self.season_endpoint + "/players"

        return self._get(url=url, params=params, headers=headers)

    def get_player_card(self, player_id: int):
        """
        Gets the kona_playercard for a specific player.
        """
        params = {"view": "kona_playercard"}

        headers = {
            "x-fantasy-filter": json.dumps(
                {"players": {"filterIds": {"value": [player_id]}}}
            )
        }

        return self._get(url=self.league_endpoint, params=params, headers=headers)
