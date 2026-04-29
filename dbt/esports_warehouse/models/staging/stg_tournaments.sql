with source as (

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
        modified_at,
        ingested_at
    from {{ source('raw', 'TOURNAMENTS') }}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by tournament_id
        order by ingested_at desc
    ) = 1

)

select *
from deduplicated