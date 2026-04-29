with source as (

    select
        tournament_id,
        tournament_name,
        team_id,
        team_name,
        player_id,
        player_name,
        player_role,
        modified_at,
        ingested_at
    from {{ source('raw', 'TOURNAMENT_ROSTERS') }}

),

deduplicated as (

    -- partition by tournament + team only since player_id is NULL in MVP
    select *
    from source
    qualify row_number() over (
        partition by tournament_id, team_id
        order by ingested_at desc
    ) = 1

)

select *
from deduplicated