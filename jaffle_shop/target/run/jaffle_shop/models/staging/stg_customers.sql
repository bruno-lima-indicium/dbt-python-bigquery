

  create or replace view `jaffle-shop-375704`.`bruno`.`stg_customers`
  OPTIONS()
  as with source as (
    select * from `jaffle-shop-375704`.`bruno`.`raw_customers`

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed;

