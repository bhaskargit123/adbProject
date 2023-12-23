# Databricks notebook source
# DBTITLE 1,Creating Dataframes - Reading Section
channel=spark.read.format("parquet").options( header=True, sep=",", inferSchema=True).load("/mnt/silver/DimChannel.parquet")
product=spark.read.format("parquet").options( header=True, sep=",", inferSchema=True).load("/mnt/silver/DimProduct.parquet")
promotion=spark.read.format("parquet").options( header=True, sep=",", inferSchema=True).load("/mnt/silver/DimPromotion.parquet")
store=spark.read.format("parquet").options( header=True, sep=",", inferSchema=True).load("/mnt/silver/DimStore.parquet")
date=spark.read.format("parquet").options( header=True, sep=",", inferSchema=True).load("/mnt/silver/date.parquet")
transaction=spark.read.format("parquet").options( header=True, sep=",", inferSchema=True).load("/mnt/silver/transaction.parquet")

# COMMAND ----------

# DBTITLE 1,Create Temp Views
channel.createOrReplaceTempView("channel")
product.createOrReplaceTempView("product")
promotion.createOrReplaceTempView("promotion")
store.createOrReplaceTempView("store")
date.createOrReplaceTempView("date")
transaction.createOrReplaceTempView("transaction")


# COMMAND ----------

# DBTITLE 1,Transformation Logic
df = spark.sql(
        """
        SELECT 
            c.ChannelName AS Channel,
            p.BrandName AS Brand,
            p.ProductName AS Product,
            pr.PromotionName AS Promotion,
            s.StoreName AS Store,
            d.Year,
            d.Month,
            SUM(SalesAmount) AS Sales 
        FROM transaction t LEFT JOIN channel c ON t.ChannelKey=c.ChannelKey
        LEFT JOIN product p ON p.ProductKey=t.ProductKey
        LEFT JOIN promotion pr ON pr.PromotionKey=t.PromotionKey
        LEFT JOIN store s ON s.StoreKey=t.StoreKey
        LEFT JOIN date d ON d.DateKey=t.DateKey
        GROUP BY c.ChannelName,
        p.BrandName,
        p.ProductName,
        pr.PromotionName,
        s.StoreName,
        d.Year,
        d.Month
        """
    )

# COMMAND ----------

df.write.format("parquet").option("header", "true").save("/mnt/gold/finalResult")
