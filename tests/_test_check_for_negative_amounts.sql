with

payments_pivoted as (

    select * from {{ ref('fct_payments__pivoted') }}

),

test_data as (

    select
        case
            when credit_card_amount < 0 then 1
            when bank_transfer_amount < 0 then 1
            when coupon_amount < 0 then 1
            when gift_card_amount <0 then 1
        end as is_negative
    
    from payments_pivoted
    having is_negative = 1

)

select * from test_data
