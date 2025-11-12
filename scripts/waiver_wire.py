from connect_espn import connect
from collections import defaultdict
import constants


def get_free_agents(league):
    """Fetches free agents and returns a dict for fast lookup."""

    free_agents_list = defaultdict(dict)


def main():
    QUERY_HOT_PLAYERS = f"""
SELECT
  player_name,
  team_abbrev,
  COUNT(game_id) AS games_last_14_days,
  AVG(total_fpts) AS avg_fpts_last_14_days
FROM
  player_game_stats
WHERE
  game_date >= date('now', '-14 days')
  AND season = '{constants.SEASON_ID}'
GROUP BY
  player_name, team_abbrev
HAVING
  games_last_14_days > 2 -- Only show players with a decent number of games
ORDER BY
  avg_fpts_last_14_days DESC;
"""

    league = connect()
    league.free_agents()  # GETS LIST OF FREEAGENTS AND WAIVER PLAYERS

    league.player_map


if __name__ == "__main__":
    main()
