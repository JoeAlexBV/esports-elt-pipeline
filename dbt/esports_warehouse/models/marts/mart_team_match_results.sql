/*
  mart_team_match_results
  ───────────────────────
  One row per team per match. Combines team dimension data with
  match participation and outcome from the intermediate layer.

  Useful for: win rates, match history, team performance over time.
*/

select
    m.match_id,
    m.match_status,
    m.scheduled_at,
    m.begin_at,
    m.end_at,
    m.tournament_id,
    m.tournament_name,
    m.league_id,
    m.league_name,
    m.videogame_name,
    m.team_id,
    m.team_name,
    m.is_winner,
    t.team_acronym,
    t.team_location
from {{ ref('int_match_opponents') }} m
left join {{ ref('dim_teams') }} t
    on m.team_id = t.team_id