select
    tournament_id,
    tournament_name,
    tournament_slug,
    tournament_status,
    begin_at,
    end_at,
    league_id,
    league_name,
    videogame_name,
    modified_at
from {{ ref('stg_tournaments') }}