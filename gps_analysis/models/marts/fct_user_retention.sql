with dim_users as (
    select
        user_id,
        -- Truncate the first activity timestamp to the beginning of the month
        -- This creates the user's cohort month
        date_trunc(first_activity_date, month) as cohort_month
    from {{ ref('dim_users') }}
),

fct_activities as (
    select
        user_id,
        -- Truncate the activity timestamp to the beginning of the month
        date_trunc(activity_started_date, month) as activity_month
    from {{ ref('fct_activities') }}
),

-- Join the tables to link each user's activities to their cohort month
user_monthly_activity as (
    select
        u.user_id,
        u.cohort_month,
        a.activity_month
    from dim_users u
    left join fct_activities a on u.user_id = a.user_id
),

-- Calculate the number of months between a user's first month and each subsequent activity month
final as (
    select
        cohort_month,
        date_diff(activity_month, cohort_month, month) as month_number,
        count(distinct user_id) as active_users
    from user_monthly_activity
    group by 1, 2
)

select * from final