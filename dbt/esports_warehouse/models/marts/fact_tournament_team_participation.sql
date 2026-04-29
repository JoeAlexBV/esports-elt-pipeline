select
    tournament_id,
    tournament_name,
    team_id,
    team_name,
    player_id,
    player_name,
    player_role,
    modified_at
from {{ ref('stg_tournament_rosters') }}