{{
    config(
        materialized='table',
        tags=['marts', 'finance']
    )
}}

/*
    Gold mart: daily revenue rollup by customer segment and region.
    Primary consumer: Power BI executive revenue dashboard.
*/

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

daily_revenue as (

    select
        transaction_date,
        customer_segment,
        region,

        count(distinct transaction_id)              as transaction_count,
        count(distinct customer_id)                 as active_customers,
        round(sum(transaction_amount), 2)           as total_revenue,
        round(avg(transaction_amount), 2)           as avg_transaction_value,
        round(
            sum(transaction_amount)
            / nullif(count(distinct customer_id), 0),
        2)                                          as revenue_per_customer

    from enriched
    group by 1, 2, 3

)

select * from daily_revenue
