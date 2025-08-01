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

)

select * from renamed