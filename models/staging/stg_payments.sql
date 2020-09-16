with source as (

    select * from {{ source('stripe','payment') }}

)

, renamed as (

    select
        id as payment_id
        , orderid as order_id
        , paymentmethod as payment_method
        , status
        , round(amount / 100.0, 2) as payment_amount
        , created as created_date
    from source

)

select * from renamed
