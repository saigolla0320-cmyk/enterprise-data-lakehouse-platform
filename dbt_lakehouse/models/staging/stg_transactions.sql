{{
    config(
        materialized='view',
        tags=['staging', 'transactions']
    )
}}

with source as (

    select * from {{ source('silver', 'fct_transactions') }}

),

renamed as (

    select
        transaction_id,
        customer_id,
        cast(amount_usd as decimal(12,2))   as transaction_amount,
        transaction_ts                      as transaction_at,
        transaction_date,
        transaction_hour,
        _processed_at                       as processed_at

    from source
    where transaction_date >= '{{ var("reporting_start_date") }}'

)

select * from renamed
