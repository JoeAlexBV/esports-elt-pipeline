with source as (

    select
        match_id,
        match_name,
        match_status,
        scheduled_at,
        begin_at,
        end_at,
        tournament_id,
        tournament_name,
        league_id,
        league_name,
        videogame_name,
        number_of_games,
        winner_id,
        winner_type,
        opponent_1_id,
        opponent_1_name,
        opponent_2_id,
        opponent_2_name,
        modified_at,
        ingested_at
    from {{ source('raw', 'MATCHES') }}

),

deduplicated as (

    select *
    from source
    qualify row_number() over (
        partition by match_id
        order by ingested_at desc
    ) = 1

)

select *
from deduplicated