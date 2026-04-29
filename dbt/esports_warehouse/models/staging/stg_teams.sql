with source as (

    select
        team_id,
        team_name,
        team_acronym,
        team_slug,
        team_location,
        modified_at,
        ingested_at
    from {{ source('raw', 'TEAMS') }}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by team_id
        order by ingested_at desc
    ) = 1

)

select *
from deduplicated