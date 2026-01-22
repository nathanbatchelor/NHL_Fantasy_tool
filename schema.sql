CREATE TABLE team_schedule (
	id INTEGER NOT NULL, 
	team VARCHAR NOT NULL, 
	week VARCHAR NOT NULL, 
	monday_date VARCHAR NOT NULL, 
	sunday_date VARCHAR NOT NULL, 
	game_count INTEGER NOT NULL, 
	opponents VARCHAR NOT NULL, 
	PRIMARY KEY (id)
);
CREATE INDEX ix_team_schedule_week ON team_schedule (week);
CREATE INDEX idx_team_week ON team_schedule (team, week);
CREATE INDEX ix_team_schedule_team ON team_schedule (team);
CREATE TABLE fantasy_team (
	team_id INTEGER NOT NULL, 
	espn_team_id INTEGER, 
	team_name VARCHAR NOT NULL, 
	owner_name VARCHAR, 
	PRIMARY KEY (team_id)
);
CREATE INDEX ix_fantasy_team_team_name ON fantasy_team (team_name);
CREATE UNIQUE INDEX ix_fantasy_team_espn_team_id ON fantasy_team (espn_team_id);
CREATE TABLE player_game_stats (
	game_id INTEGER NOT NULL, 
	player_id INTEGER NOT NULL, 
	game_date VARCHAR NOT NULL, 
	season VARCHAR NOT NULL, 
	team_abbrev VARCHAR NOT NULL, 
	team_name VARCHAR NOT NULL, 
	opponent_abbrev VARCHAR NOT NULL, 
	opponent_name VARCHAR NOT NULL, 
	player_name VARCHAR, 
	jersey_number INTEGER, 
	position VARCHAR, 
	goals INTEGER NOT NULL, 
	assists INTEGER NOT NULL, 
	pp_points FLOAT NOT NULL, 
	sh_points FLOAT NOT NULL, 
	shots INTEGER NOT NULL, 
	shooting_pct FLOAT, 
	blocked_shots INTEGER NOT NULL, 
	hits INTEGER NOT NULL, 
	toi_seconds INTEGER NOT NULL, 
	shifts INTEGER NOT NULL, 
	total_fpts FLOAT NOT NULL, 
	PRIMARY KEY (game_id, player_id)
);
CREATE INDEX idx_player_team ON player_game_stats (player_id, team_abbrev);
CREATE INDEX idx_player_date ON player_game_stats (player_id, game_date);
CREATE INDEX ix_player_game_stats_position ON player_game_stats (position);
CREATE INDEX idx_team_name ON player_game_stats (team_name);
CREATE INDEX ix_player_game_stats_season ON player_game_stats (season);
CREATE INDEX idx_game_date ON player_game_stats (game_date);
CREATE INDEX idx_team ON player_game_stats (team_abbrev);
CREATE TABLE goalie_game_stats (
	game_id INTEGER NOT NULL, 
	player_id INTEGER NOT NULL, 
	game_date VARCHAR NOT NULL, 
	season VARCHAR NOT NULL, 
	team_abbrev VARCHAR NOT NULL, 
	team_name VARCHAR NOT NULL, 
	opponent_abbrev VARCHAR NOT NULL, 
	opponent_name VARCHAR NOT NULL, 
	player_name VARCHAR, 
	jersey_number INTEGER, 
	position VARCHAR, 
	saves INTEGER NOT NULL, 
	save_pct FLOAT NOT NULL, 
	goals_against INTEGER NOT NULL, 
	decision VARCHAR, 
	wins INTEGER NOT NULL, 
	shutouts INTEGER NOT NULL, 
	ot_losses INTEGER NOT NULL, 
	total_fpts FLOAT NOT NULL, 
	PRIMARY KEY (game_id, player_id)
);
CREATE INDEX idx_goalie_team_name ON goalie_game_stats (team_name);
CREATE INDEX idx_goalie_player_team ON goalie_game_stats (player_id, team_abbrev);
CREATE INDEX idx_goalie_game_date ON goalie_game_stats (game_date);
CREATE INDEX ix_goalie_game_stats_position ON goalie_game_stats (position);
CREATE INDEX ix_goalie_game_stats_season ON goalie_game_stats (season);
CREATE INDEX idx_goalie_team ON goalie_game_stats (team_abbrev);
CREATE INDEX idx_goalie_player_date ON goalie_game_stats (player_id, game_date);
CREATE TABLE pro_players (
	player_id INTEGER NOT NULL, 
	espn_id INTEGER, 
	player_name VARCHAR NOT NULL, 
	team_abbrev VARCHAR, 
	position VARCHAR, 
	jersey_number INTEGER, 
	is_active BOOLEAN NOT NULL, 
	is_goalie BOOLEAN NOT NULL, 
	injury_status VARCHAR, 
	fantasy_team_id INTEGER, 
	season_games_played INTEGER NOT NULL, 
	season_total_fpts FLOAT NOT NULL, 
	season_goals INTEGER NOT NULL, 
	season_assists INTEGER NOT NULL, 
	season_pp_points FLOAT NOT NULL, 
	season_sh_points FLOAT NOT NULL, 
	season_shots INTEGER NOT NULL, 
	season_blocked_shots INTEGER NOT NULL, 
	season_hits INTEGER NOT NULL, 
	season_wins INTEGER NOT NULL, 
	season_shutouts INTEGER NOT NULL, 
	season_ot_losses INTEGER NOT NULL, 
	season_saves INTEGER NOT NULL, 
	season_goals_against INTEGER NOT NULL, prior_season_avg_fpts FLOAT DEFAULT 0.0, prior_season_games_played INTEGER DEFAULT 0, predicted_fpts FLOAT DEFAULT 0.0, 
	PRIMARY KEY (player_id), 
	FOREIGN KEY(fantasy_team_id) REFERENCES fantasy_team (team_id)
);
CREATE INDEX ix_pro_players_player_name ON pro_players (player_name);
CREATE INDEX ix_pro_players_team_abbrev ON pro_players (team_abbrev);
CREATE UNIQUE INDEX ix_pro_players_espn_id ON pro_players (espn_id);
CREATE INDEX ix_pro_players_position ON pro_players (position);
