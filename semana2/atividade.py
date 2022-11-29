from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder.appName('semana2').getOrCreate()

df_parquet = spark.read.parquet('semana2/dataset_transformado_parquet/part-00000-00341ba7-0a7c-4fef-a81e-1066725a64b1-c000.snappy.parquet')

# Tratamento da coluna caracteristicas
df_caracteristicas = df_parquet.withColumn('caracteristicas',f.when(f.size(f.col('caracteristicas')) == 0, f.lit(None)).otherwise(f.col('caracteristicas')))

########## tratando valores nulos ##########
# Preenchendo com 0 valores inteiros nulos
df_caracteristicas_ = df_caracteristicas.fillna(value=0,subset=['banheiros','quartos','suites','vaga','condominio','iptu'])

# Preenchendo area_total com o valor de area_util quando estiver nulo
area_total = df_caracteristicas_.withColumn('area_total',f.when(f.isnull(f.col('area_total')) == 'true', f.col('area_util')).otherwise(f.col('area_total')))

# Dropando linhas que contenham valores nulos nas colunas 
area_total_drop = area_total.na.drop(subset=['id','tipo_unidade','bairro','zona','tipo','valor'])










