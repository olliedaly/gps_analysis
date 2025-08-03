with user_retention as (

    select * from {{ ref('fct_user_retention') }}

),

-- Cast month_number to a string to ensure the pivot macro works correctly
user_retention_casted as (

    select
        cohort_month,
        cast(month_number as string) as month_number_str,
        active_users
    from user_retention

)

-- Use the pivot macro on the casted data
select
    cohort_month,
    {{ dbt_utils.pivot(
        'month_number_str',
        dbt_utils.get_column_values(
            ref('fct_user_retention'),
            'month_number'
        ),
        agg='max',
        then_value='active_users'
    ) }}
from user_retention_casted
group by 1