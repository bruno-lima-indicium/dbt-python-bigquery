select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select amount
from `jaffle-shop-375704`.`bruno`.`orders`
where amount is null



      
    ) dbt_internal_test