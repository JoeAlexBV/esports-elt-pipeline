select
    team_id,
    team_name,
    team_acronym,
    team_slug,
    team_location,
    modified_at
from {{ ref('stg_teams') }}