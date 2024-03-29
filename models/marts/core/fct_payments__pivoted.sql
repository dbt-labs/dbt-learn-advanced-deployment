{% set payment_methods = dbt_utils.get_column_values(table=ref('stg_stripe__payments'), column='payment_method') -%}

with payments as (

    select * from {{ ref('stg_stripe__payments') }}

),

pivoted as (

    select
        order_id,

        {%- for payment_method in payment_methods -%}

        sum
            (case 
                when payment_method = '{{ payment_method }}' 
                    and status = 'success'
                then amount else 0 end) as {{ payment_method }}_amount

        {%- if not loop.last -%}
        ,
        {% endif -%}

        {%- endfor %}

    from payments
    group by 1

)

select * from pivoted
