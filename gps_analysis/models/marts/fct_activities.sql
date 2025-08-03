with int_activities as (

    select * from {{ ref('int_activities_with_distance') }}

),

ranked_activities as (
    -- Used to get first and last lat and long per activity
    select
        *,
        row_number() over(partition by user_id, activity_id order by activity_timestamp asc) as asc_rn,
        row_number() over(partition by user_id, activity_id order by activity_timestamp desc) as desc_rn
    from int_activities
),

final as (
    select
        -- Create a new, unique primary key
        {{ dbt_utils.generate_surrogate_key(['user_id', 'activity_id']) }} as activity_sk,
        activity_id,
        user_id,

        -- Timestamps
        min(activity_timestamp) as activity_started_timestamp,
        max(activity_timestamp) as activity_ended_timestamp,
        min(date(activity_timestamp)) as activity_started_date,
        max(date(activity_timestamp)) as activity_ended_date,

        -- Activity metrics
        sum(distance_from_prev_point_km) as total_distance_km,
        timestamp_diff(max(activity_timestamp), min(activity_timestamp), minute) as duration_minutes,
        count(activity_timestamp) as total_gps_points,
        
        -- Calculate average speed in km/h
        safe_divide(
            sum(distance_from_prev_point_km),
            timestamp_diff(max(activity_timestamp), min(activity_timestamp), second) / 3600
        ) as avg_speed_kph,

        -- Position data for mapping
        min(case when asc_rn = 1 then latitude_deg end) as latitude_start,
        min(case when asc_rn = 1 then longitude_deg end) as longitude_start,

        -- Get the last value by picking the one where the descending rank is 1
        min(case when desc_rn = 1 then latitude_deg end) as latitude_end,
        min(case when desc_rn = 1 then longitude_deg end) as longitude_end,

        -- Form coordinates for mapping
        concat(min(case when asc_rn = 1 then latitude_deg end), ',', min(case when asc_rn = 1 then longitude_deg end)) as coord_start,
        concat(min(case when desc_rn = 1 then latitude_deg end), ',', min(case when desc_rn = 1 then longitude_deg end)) as coord_end
        
    from ranked_activities
    -- Add the new surrogate key to the group by clause
    group by 1, 2, 3
)

select * from final