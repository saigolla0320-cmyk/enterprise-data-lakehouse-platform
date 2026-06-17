{{
    config(
        materialized='table',
        tags=['marts', 'customer']
    )
}}

/*
    Gold mart: customer 360 view combining CRM attributes with
    actual transaction behavior. Powers segmentation and retention analysis.
*/

with enriched as (

    select * from {{ ref('int_transactions_enriched') }}

),

customer_behavior as (

    select
        customer_id,
        customer_segment,
        region,
        signup_date,
        crm_lifetime_value,

        count(distinct transaction_id)          as total_transactions,
        round(sum(transaction_amount), 2)       as actual_lifetime_spend,
        round(avg(transaction_amount), 2)       as avg_order_value,
        min(transaction_date)                   as first_purchase_date,
        max(transaction_date)                   as last_purchase_date

    from enriched
    group by 1, 2, 3, 4, 5

),

final as (

    select
        *,
        date_diff('day', first_purchase_date, last_purchase_date) as active_span_days,

        case
            when actual_lifetime_spend >= 10000 then 'platinum'
            when actual_lifetime_spend >= 5000  then 'gold'
            when actual_lifetime_spend >= 1000  then 'silver'
            else 'bronze'
        end                                     as value_tier,

        case
            when last_purchase_date < current_date - interval 90 day then 'churned'
            when last_purchase_date < current_date - interval 30 day then 'at_risk'
            else 'active'
        end                                     as engagement_status

    from customer_behavior

)

select * from final
