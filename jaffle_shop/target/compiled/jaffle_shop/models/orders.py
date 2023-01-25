import pyspark.sql.functions as F

def model(dbt, session):

    dbt.config(
        submission_method="cluster",
        dataproc_cluster_name="dbt-python"
    )

    stg_orders_df = dbt.ref('stg_orders')
    stg_payments_df = dbt.ref('stg_payments')

    payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card']

    agg_list = [F.sum(F.when(stg_payments_df.payment_method == payment_method, stg_payments_df.amount).otherwise(0)).alias(payment_method + '_amount') for payment_method in payment_methods]

    agg_list.append(F.sum(F.col('amount')).alias('total_amount'))

    order_payments_df = (
        stg_payments_df
        .groupby('order_id')
        .agg(*agg_list)
    )

    final_df = (
        stg_orders_df
        .join(order_payments_df, stg_orders_df.order_id == order_payments_df.order_id, 'left')
        .select(stg_orders_df.order_id.alias('order_id'),
                stg_orders_df.customer_id.alias('customer_id'),
                stg_orders_df.order_date.alias('order_date'),
                stg_orders_df.status.alias('status'),
                *[F.col(payment_method + '_amount') for payment_method in payment_methods],
                order_payments_df.total_amount.alias('amount')
        )
    )

    return final_df


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args,dbt_load_df_function):
    refs = {"stg_orders": "jaffle-shop-375704.bruno.stg_orders", "stg_payments": "jaffle-shop-375704.bruno.stg_payments"}
    key = ".".join(args)
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = ".".join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = 'jaffle-shop-375704'
    schema = 'bruno'
    identifier = 'orders'
    def __repr__(self):
        return 'jaffle-shop-375704.bruno.orders'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------


