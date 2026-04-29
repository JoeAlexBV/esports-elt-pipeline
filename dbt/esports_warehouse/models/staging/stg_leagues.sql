with source as (

    select
        league_id,
        league_name,
        league_slug,
        league_url,
        modified_at,
        ingested_at
    from {{ source('raw', 'LEAGUES') }}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by league_id
        order by ingested_at desc
    ) = 1

)

select *
from deduplicated