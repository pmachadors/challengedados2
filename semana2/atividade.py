from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *

spark = SparkSession.builder.appName('semana2').getOrCreate()

df = spark.read.parquet('semana2/dataset_transformado_parquet/part-00000-00341ba7-0a7c-4fef-a81e-1066725a64b1-c000.snappy.parquet')

# selecao de features
df_parquet = df.drop('area_total','tipo_anuncio','tipo_uso','tipo','tipo_unidade')

# Convertendo tipo de dados
df_parquet_ = df_parquet.withColumn('andar',df_parquet.andar.cast(IntegerType()))\
                        .withColumn('banheiros',df_parquet.banheiros.cast(IntegerType()))\
                        .withColumn('suites',df_parquet.suites.cast(IntegerType()))\
                        .withColumn('quartos',df_parquet.quartos.cast(IntegerType()))\
                        .withColumn('vaga',df_parquet.vaga.cast(IntegerType()))\
                        .withColumn('area_util',df_parquet.area_util.cast(DoubleType()))\
                        .withColumn('condominio',df_parquet.condominio.cast(DoubleType()))\
                        .withColumn('iptu',df_parquet.iptu.cast(DoubleType()))\
                        .withColumn('valor',df_parquet.valor.cast(DoubleType()))
                        
# Tratamento da coluna caracteristicas
df_caracteristicas = df_parquet_.withColumn('caracteristicas',f.when(f.size(f.col('caracteristicas')) == 0, f.lit(None)).otherwise(f.col('caracteristicas')))

# Preenchendo com 0 valores inteiros nulos
df_caracteristicas_ = df_caracteristicas.fillna(value=0,subset=['banheiros','quartos','suites','vaga','condominio','iptu'])

# Dropando linhas que contenham valores nulos nas colunas 
df_drop = df_caracteristicas_.na.drop(subset=['id','bairro','zona','zona'])

# Dropando string vazia no campo zona
df_drop_ = df_drop.where(f.trim(f.col('zona')) != '')

# Dummy Classifier na coluna Zona
df_dummy_zona = df_drop_\
    .groupBy('id')\
    .pivot('zona')\
    .agg(f.lit(1))\
    .na\
    .fill(0)
df_join = df_drop_.join(df_dummy_zona,'id',how='inner')

# Dummy Classifier na coluna caracteristicas