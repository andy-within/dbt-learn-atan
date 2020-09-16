with orders_base as (

    select * from {{ ref('stg_orders') }}

)

, payments as (

    select * from {{ ref('stg_payments') }}

)

, successful_payments as (

    select * from payments
    where status = 'success'

)

, final as (

    select
        orders_base.order_id
        , orders_base.order_date
        , orders_base.customer_id
        , sum(successful_payments.payment_amount) as amount
    from orders_base
    left join successful_payments
        on orders_base.order_id = successful_payments.order_id
        group by 1, 2, 3

)

select * from final
