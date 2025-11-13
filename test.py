SELECT player_id, player_name, game_id, game_date, total_fpts
FROM goalie_game_stats
ORDER BY total_fpts DESC
LIMIT 1;