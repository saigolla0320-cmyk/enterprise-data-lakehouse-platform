{{
    config(
        materialized='ephemeral',
        tags=['intermediate']
    )
}}

/*
    Intermediate model: enriches each transaction with customer attributes.
    Materialized as ephemeral — inlined into downstream marts, not persisted.
*/

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

enriched as (

    select
        t.transaction_id,
        t.customer_id,
        t.transaction_amount,
        t.transaction_at,
        t.transaction_date,
        t.transaction_hour,

        c.customer_segment,
        c.region,
        c.signup_date,
        c.crm_lifetime_value,

        {{ safe_divide('t.transaction_amount', 'c.crm_lifetime_value') }} as txn_to_ltv_ratio

    from transactions t
    left join customers c
        on t.customer_id = c.customer_id

)

select * from enriched
