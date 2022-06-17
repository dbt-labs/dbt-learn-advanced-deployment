with

customers as (

    select * from {{ ref('dim_customers') }}
    where lifetime_value is not null

),

orders as (

    select * from {{ ref('fct_orders') }}

),

payments as (

    select * from {{ ref('fct_payments__pivoted') }}

),

orders_breakdown as (
    
    select
        orders.customer_id,
        sum(orders.amount) as total_amount,
        sum(payments.credit_card_amount) as credit_card_total,
        sum(payments.bank_transfer_amount) as bank_transfer_total,
        sum(payments.coupon_amount) as coupon_total,
        sum(payments.gift_card_amount) as gift_card_total
    
    from orders
        left join payments using(order_id)
    group by 1
    order by total_amount desc
    limit 10

),

join_customer_info as (

    select
        row_number() over(order by customers.lifetime_value desc) as value_rank,
        customers.customer_id,
        customers.first_name || ' ' || customers.last_name as customer_name,
        customers.lifetime_value,
        orders_breakdown.total_amount,
        orders_breakdown.credit_card_total,
        orders_breakdown.bank_transfer_total,
        orders_breakdown.coupon_total,
        orders_breakdown.gift_card_total
    
    from orders_breakdown
        left join customers using(customer_id)

)

select * from join_customer_info
