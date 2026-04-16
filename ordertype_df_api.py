#!/usr/bin/env python
# coding: utf-8 

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

def main():

    spark = SparkSession.builder.appName("CustomerOrdersJob").getOrCreate()

    #Initilaizing the Source & Traget path
    customers_path = "/opt/spark-apps/input/customers.csv"
    orders_path = "/opt/spark-apps/input/orders.json"
    output_path_csv = "/tmp/orders_enriched_csv"
    output_path_parquet = "/tmp/orders_enriched_parquet"


    #Creating dataframe for sourcefiles
    df_customers = spark.read.option("header", True).csv(customers_path)
    df_orders = spark.read.json(orders_path)

    #logger for analysis ..should not go to prod
    df_customers.show()
    df_customers.show(4, False)
    df_orders.show(4)

    #resulatant output from sources using join
    df_joined = df_orders.join(df_customers, on="customer_id", how="inner")

    #Enriching the resulatant dataframe
    df_enriched = df_joined.withColumn(
        "order_type",
        when(df_joined.amount >= 200, "High Value")
        .when(df_joined.amount >= 100, "Medium Value")
        .otherwise("Low Value")
    )


    df_enriched.select("order_id", "name", "amount", "order_type").show()
    df_enriched_op = df_enriched.select("order_id", "name", "amount", "order_type") #selected outpout columns

    #data stored in csv & parquet format in output location
    df_enriched_op.write.mode("overwrite").option("header", True).csv(output_path_csv)
    df_enriched_op.write.mode("overwrite").parquet(output_path_parquet)

    spark.stop()
    
if __name__ == "__main__":
    main()




