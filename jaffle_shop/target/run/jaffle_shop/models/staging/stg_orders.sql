

  create or replace view `jaffle-shop-375704`.`bruno`.`stg_orders`
  OPTIONS()
  as with source as (
    select * from `jaffle-shop-375704`.`bruno`.`raw_orders`

),

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date,
        status

    from source

)

select * from renamed;

