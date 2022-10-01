with players as (
    select
        id as player_id
        , team as team_id
        , ep_next
        , ep_this
        , event_points
        , first_name
        , form
        , news_added
        , now_cost
        , second_name
        , selected_by_percent
        , team_code as team_code
        , total_points
        , transfers_in
        , transfers_in_event
        , transfers_out
        , transfers_out_event
        , web_name
        , bps
        , influence
        , to_timestamp_ltz(loaded_at::int) as loaded_at_est
    from {{ source('public', 'players') }}
)


select * from players


