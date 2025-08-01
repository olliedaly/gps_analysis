with activities as (

    select * from {{ ref('fct_activities')}}

),

final as (
    select
        user_id,

        -- User's lifetime activity timestamps
        min(activity_started_at) as first_activity_at,
        max(activity_ended_at) as last_activity_at,

        -- User-level aggregations
        count(distinct activity_id) as total_activity_count,
        avg(total_distance_km) as avg_distance_per_activity_km,
        sum(total_distance_km) as lifetime_distance_km,
        

    from activities
    group by 1
)

select * from final