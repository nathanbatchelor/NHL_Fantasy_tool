from connect_espn import connect


def get_team_ids(league):
    if not league:
        print("Failed to connect to ESPN league.")
        return

    teams = league.standings()
    return [team.team_id for team in teams]



def main():
    league = connect()
    team_ids = get_team_ids(league)

    for id in team_ids:
        print(league.get_team_data(id))


if __name__ == "__main__":
    main()
