CREATE DATABASE IF NOT EXISTS ESPORTS;

CREATE SCHEMA IF NOT EXISTS ESPORTS.RAW;
CREATE SCHEMA IF NOT EXISTS ESPORTS.ANALYTICS;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE TABLE IF NOT EXISTS ESPORTS.RAW.LEAGUES (
    league_id INTEGER,
    league_name STRING,
    league_slug STRING,
    league_url STRING,
    modified_at TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS ESPORTS.RAW.TOURNAMENTS (
    tournament_id INTEGER,
    tournament_name STRING,
    tournament_slug STRING,
    tournament_status STRING,
    begin_at TIMESTAMP_NTZ,
    end_at TIMESTAMP_NTZ,
    league_id INTEGER,
    league_name STRING,
    videogame_name STRING,
    modified_at TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS ESPORTS.RAW.TEAMS (
    team_id INTEGER,
    team_name STRING,
    team_acronym STRING,
    team_slug STRING,
    team_location STRING,
    modified_at TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS ESPORTS.RAW.MATCHES (
    match_id INTEGER,
    match_name STRING,
    match_status STRING,
    scheduled_at TIMESTAMP_NTZ,
    begin_at TIMESTAMP_NTZ,
    end_at TIMESTAMP_NTZ,
    tournament_id INTEGER,
    tournament_name STRING,
    league_id INTEGER,
    league_name STRING,
    videogame_name STRING,
    number_of_games INTEGER,
    winner_id INTEGER,
    winner_type STRING,
    opponent_1_id INTEGER,
    opponent_1_name STRING,
    opponent_2_id INTEGER,
    opponent_2_name STRING,
    modified_at TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS ESPORTS.RAW.TOURNAMENT_ROSTERS (
    tournament_id INTEGER,
    tournament_name STRING,
    team_id INTEGER,
    team_name STRING,
    player_id INTEGER,
    player_name STRING,
    player_role STRING,
    modified_at TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ
);