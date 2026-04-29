/*
  mart_tournament_activity
  ────────────────────────
  One row per tournament enriched with participation and match counts.

  Useful for: tournament size analysis, league activity summaries,
  identifying the most active tournaments by team/match count.
*/

with team_counts as (

    select
        tournament_id,
        count(distinct team_id) as team_count
    from {{ ref('fact_tournament_team_participation') }}
    group by tournament_id

),

match_counts as (

    select
        tournament_id,
        count(distinct match_id) as total_matches,
        count(distinct case when match_status = 'finished'
                            then match_id end) as finished_matches,
        count(distinct case when match_status = 'not_started'
                            then match_id end) as upcoming_matches
    from {{ ref('fact_matches') }}
    group by tournament_id

)

select
    t.tournament_id,
    t.tournament_name,
    t.tournament_slug,
    t.tournament_status,
    t.begin_at,
    t.end_at,
    t.league_id,
    t.league_name,
    t.videogame_name,
    coalesce(tc.team_count, 0)         as team_count,
    coalesce(mc.total_matches, 0)      as total_matches,
    coalesce(mc.finished_matches, 0)   as finished_matches,
    coalesce(mc.upcoming_matches, 0)   as upcoming_matches
from {{ ref('dim_tournaments') }} t
left join team_counts  tc on t.tournament_id = tc.tournament_id
left join match_counts mc on t.tournament_id = mc.tournament_id