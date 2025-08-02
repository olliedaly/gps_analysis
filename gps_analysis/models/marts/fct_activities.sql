with int_activities as (

    select * from {{ ref('int_activities_with_distance') }}

),

final as (
    select
        -- Create a new, unique primary key
        {{ dbt_utils.generate_surrogate_key(['user_id', 'activity_id']) }} as activity_sk,
        activity_id,
        user_id,

        -- Timestamps
        min(activity_timestamp) as activity_started_at,
        max(activity_timestamp) as activity_ended_at,

        -- Activity metrics
        sum(distance_from_prev_point_km) as total_distance_km,
        timestamp_diff(max(activity_timestamp), min(activity_timestamp), minute) as duration_minutes,
        count(activity_timestamp) as total_gps_points,
        
        -- Calculate average speed in km/h
        safe_divide(
            sum(distance_from_prev_point_km),
            timestamp_diff(max(activity_timestamp), min(activity_timestamp), second) / 3600
        ) as avg_speed_kph

    from int_activities
    -- Add the new surrogate key to the group by clause
    group by 1, 2, 3
)

select * from final