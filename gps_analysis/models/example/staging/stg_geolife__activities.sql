with source as (

    select * from {{ source('geolife_raw', 'geolife__activities') }}

),

renamed as (

    select
        user_id,
        activity_id,
        latitude as latitude_deg,
        longitude as longitude_deg,
        altitude_ft,
        timestamp as activity_timestamp

    from source

)

select * from renamed