BASE = "https://lm-api-reads.fantasy.espn.com/apis/v3/games/fhl/seasons/2026/segments/0/leagues/1694980447"
HEADERS = {
    "accept": "application/json",
    "User-Agent": "Mozilla/5.0",
    "Cookie": "SWID={1293C51F-A944-4758-94B6-660B813DB9C2}; espn_s2={AEAbZPyyg%2FWRth%2FmtrQ5xhL0%2B89QosTPL8UIbu2d5p4MGjs8F%2BMAv2Y22GzryiGXMf1vZMTJDr9ajyspAuHCvkTQ229VqbJegO6cxrrXnr5kxU6O6wTSEC5jfddE%2F5M6l7q2K5bH1cgvgAPCPB99ox6peS5uSi3xPyJyjbdWMGURlBC8n7dYrcAfGenKHauuXXoROhd4tbuNvkcn6MRep8mJK2gdLTy8TbZ3vStepfJsQhwsDGsEjRQjJPaOaGwFwhQ2XvkZNzarTx%2BXdnDoDhRVXPnQ3lzScUWoMuPEStj6eJiDgfQ5assae%2BXas7Mugec%3D}",
}


def get_team_mapping():
    """Fetch team -> owner name mapping from ESPN API"""
    resp = requests.get(
        BASE,
        headers=HEADERS,
        params={"view": "mRoster"},
    )
    resp.raise_for_status()
    data = resp.json()
    teams = data.get("teams", [])
    mapping = {}

    for team in teams:
        team_id = team.get("id")
        owner = team.get("owners", [None])[0]
        abbrev = team.get("abbrev")
        name = team.get("location", "") + " " + team.get("nickname", "")
        mapping[team_id] = {
            "owner": owner,
            "team_name": name.strip(),
            "abbrev": abbrev,
        }

    print(f"Loaded {len(mapping)} teams with owners")
    return mapping


def fetch_players(limit=50):
    offset = 0
    all_players = []

    while True:
        filt = {
            "players": {
                "filterStatus": {"value": ["FREEAGENT", "WAIVERS", "ONTEAM"]},
                "limit": limit,
                "offset": offset,
                "sortDraftRanks": {
                    "sortPriority": 100,
                    "sortAsc": True,
                    "value": "STANDARD",
                },
                "sortPercOwned": {"sortAsc": False, "sortPriority": 1},
                "filterRanksForRankTypes": {"value": ["STANDARD"]},
            }
        }

        resp = requests.get(
            BASE,
            headers={**HEADERS, "x-fantasy-filter": json.dumps(filt)},
            params={
                "view": "kona_player_info",
                "platformVersion": "b22b223feac5f121ae1f319a720e049fdb16e0d8",
            },
        )
        resp.raise_for_status()

        data = resp.json()
        players = data.get("players", [])
        if not players:
            break

        all_players.extend(players)
        print(f"Fetched {len(players)} players (offset={offset})")
        offset += len(players)
        # time.sleep(0.25)

    return all_players


def save_to_csv(players, team_map):
    with open("espn_players.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "playerId",
                "fullName",
                "positionId",
                "teamId",
                "eligibleSlots",
                "ownedByTeam",
                "ownedByUser",
                "percentOwned",
            ]
        )

        for p in players:
            pl = p.get("player", {})
            team_id = p.get("onTeamId")
            eligible_str = ", ".join(str(s) for s in pl.get("eligibleSlots", []))
            owner_info = team_map.get(team_id, {})
            writer.writerow(
                [
                    p.get("playerId"),
                    pl.get("fullName"),
                    pl.get("defaultPositionId"),
                    pl.get("proTeamId"),
                    eligible_str,
                    owner_info.get("team_name", "FA"),
                    owner_info.get("owner", "Free Agent"),
                    p.get("ownership", {}).get("percentOwned"),
                ]
            )
    print(f"✅ Saved {len(players)} players with ownership info to espn_players.csv")
