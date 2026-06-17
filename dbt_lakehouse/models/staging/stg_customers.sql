{{
    config(
        materialized='view',
        tags=['staging', 'customers']
    )
}}

with source as (

    select * from {{ source('silver', 'dim_customer') }}

),

current_customers as (

    select
        customer_id,
        segment                             as customer_segment,
        region,
        signup_date,
        cast(lifetime_value as decimal(12,2)) as crm_lifetime_value,
        effective_from,
        effective_to,
        is_current

    from source
    where is_current = true

)

select * from current_customers
