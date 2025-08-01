with stg_activities as (

    select * from {{ ref('stg_geolife__activities') }}

),

-- Use a window function to get the previous GPS coordinate for each point
activity_points_with_previous as (

    select
        *,
        lag(latitude_deg, 1) over (partition by activity_id order by activity_timestamp) as prev_latitude_deg,
        lag(longitude_deg, 1) over (partition by activity_id order by activity_timestamp) as prev_longitude_deg

    from stg_activities
)

-- Calculate the distance from the previous point to the current one
select
    activity_id,
    user_id,
    activity_timestamp,
    -- Calculate distance in kilometers using the Haversine formula
    st_distance(
        st_geogpoint(longitude_deg, latitude_deg),
        st_geogpoint(prev_longitude_deg, prev_latitude_deg)
    ) / 1000 as distance_from_prev_point_km

from activity_points_with_previous