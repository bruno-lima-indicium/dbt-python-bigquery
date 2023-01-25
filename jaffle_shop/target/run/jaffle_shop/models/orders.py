
  
    
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('smallTest').getOrCreate()

spark.conf.set("viewsEnabled","true")
spark.conf.set("temporaryGcsBucket","dbt_python_bigquery_bucket")

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



dbt = dbtObj(spark.read.format("bigquery").load)
df = model(dbt, spark)

# COMMAND ----------
# this is materialization code dbt generated, please do not modify

import pyspark
# make sure pandas exists before using it
try:
  import pandas
  pandas_available = True
except ImportError:
  pandas_available = False

# make sure pyspark.pandas exists before using it
try:
  import pyspark.pandas
  pyspark_pandas_api_available = True
except ImportError:
  pyspark_pandas_api_available = False

# make sure databricks.koalas exists before using it
try:
  import databricks.koalas
  koalas_available = True
except ImportError:
  koalas_available = False

# preferentially convert pandas DataFrames to pandas-on-Spark or Koalas DataFrames first
# since they know how to convert pandas DataFrames better than `spark.createDataFrame(df)`
# and converting from pandas-on-Spark to Spark DataFrame has no overhead
if pyspark_pandas_api_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = pyspark.pandas.frame.DataFrame(df)
elif koalas_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = databricks.koalas.frame.DataFrame(df)

# convert to pyspark.sql.dataframe.DataFrame
if isinstance(df, pyspark.sql.dataframe.DataFrame):
  pass  # since it is already a Spark DataFrame
elif pyspark_pandas_api_available and isinstance(df, pyspark.pandas.frame.DataFrame):
  df = df.to_spark()
elif koalas_available and isinstance(df, databricks.koalas.frame.DataFrame):
  df = df.to_spark()
elif pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = spark.createDataFrame(df)
else:
  msg = f"{type(df)} is not a supported type for dbt Python materialization"
  raise Exception(msg)

df.write \
  .mode("overwrite") \
  .format("bigquery") \
  .option("writeMethod", "direct").option("writeDisposition", 'WRITE_TRUNCATE') \
  .save("jaffle-shop-375704.bruno.orders")

  