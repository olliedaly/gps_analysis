with source as (

    select * from {{ source('geolife_raw', 'geolife__activities') }}

),

renamed as (

    select
        user_id,
        activity_id,
        latitude as latitude_deg,
        longitude as longitude_deg,
        case 
            when altitude_ft = -777 then null else altitude_ft / 0.3048 
        end as altitude,
        timestamp as activity_timestamp

    from source
    -- Filter out rows with invalid latitude values (just 1)
    where latitude between -90 and 90
    -- no cases, but future proofing, warning set in tests to identify if occurs
    and longitude between -180 and 180
    -- 1 activity from year 2000, dataset details say 'from April 2007'. Removing
    and extract(year from timestamp) > 2006

)

select * from renamed