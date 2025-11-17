SELECT
    AllPlayers.player_id,
    COALESCE(ps.player_name, gs.player_name) AS player_name
FROM (
    SELECT player_id FROM player_game_stats
    UNION
    SELECT player_id FROM goalie_game_stats
) AS AllPlayers
LEFT JOIN player_map pm ON AllPlayers.player_id = pm.nhl_id
LEFT JOIN player_game_stats ps ON AllPlayers.player_id = ps.player_id
LEFT JOIN goalie_game_stats gs ON AllPlayers.player_id = gs.player_id
WHERE
    pm.nhl_id IS NULL
GROUP BY
    AllPlayers.player_id,
    ps.player_name,
    gs.player_name;