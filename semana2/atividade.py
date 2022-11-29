from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName('semana2').getOrCreate()

df_parquet = spark.read.parquet('semana2/dataset_transformado_parquet/part-00000-00341ba7-0a7c-4fef-a81e-1066725a64b1-c000.snappy.parquet')

# Tratamento da coluna caracteristicas
df_parquet.withColumn('caracteristicas',f.when(f.size(f.col('caracteristicas')) == 0, f.lit(None)).otherwise(f.col('caracteristicas'))).show()

