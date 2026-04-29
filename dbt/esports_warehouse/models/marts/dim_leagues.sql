select
    league_id,
    league_name,
    league_slug,
    league_url,
    modified_at
from {{ ref('stg_leagues') }}