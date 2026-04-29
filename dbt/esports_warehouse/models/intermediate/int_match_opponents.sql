/*
  int_match_opponents
  ───────────────────
  Unpivots the opponent_1 / opponent_2 columns from fact_matches into
  one row per match-opponent pair. This makes it easier to aggregate
  stats at the team level downstream.

  Input:  fact_matches         (1 row per match, 2 opponent columns)
  Output: int_match_opponents  (2 rows per match, 1 opponent per row)
*/

with matches as (

    select
        match_id,
        match_status,
        scheduled_at,
        begin_at,
        end_at,
        tournament_id,
        tournament_name,
        league_id,
        league_name,
        videogame_name,
        winner_id,
        winner_type,
        opponent_1_id,
        opponent_1_name,
        opponent_2_id,
        opponent_2_name
    from {{ ref('fact_matches') }}

),

opponent_1 as (

    select
        match_id,
        match_status,
        scheduled_at,
        begin_at,
        end_at,
        tournament_id,
        tournament_name,
        league_id,
        league_name,
        videogame_name,
        opponent_1_id   as team_id,
        opponent_1_name as team_name,
        case when winner_id = opponent_1_id 
            then true 
            else false 
        end as is_winner
    from matches
    where opponent_1_id is not null

),

opponent_2 as (

    select
        match_id,
        match_status,
        scheduled_at,
        begin_at,
        end_at,
        tournament_id,
        tournament_name,
        league_id,
        league_name,
        videogame_name,
        opponent_2_id   as team_id,
        opponent_2_name as team_name,
        case when winner_id = opponent_2_id 
            then true 
            else false 
        end as is_winner
    from matches
    where opponent_2_id is not null

)

select * from opponent_1
union all
select * from opponent_2