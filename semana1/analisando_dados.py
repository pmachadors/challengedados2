from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkAnalisandoDados') \
                    .getOrCreate()

df = spark.read.json("dataset_bruto.json")

print(df.count())
# 89083
# df.show()

df_anuncio = df.select('anuncio')

schema = StructType(
    [
        StructField('caracteristicas', StringType(), True),
        StructField('area_util', StringType(), True),
        StructField('tipo_anuncio', StringType(), True),
        StructField('tipo_unidade', StringType(), True),
        StructField('andar', StringType(), True),
        StructField('vaga', StringType(), True),
        StructField('suites', StringType(), True),
        StructField('banheiros', StringType(), True),
        StructField('tipo_uso', StringType(), True),
        StructField('area_total', StringType(), True),
        StructField('quartos', StringType(), True),
        StructField('valores', StringType(), True),
        StructField('endereco', StringType(), True),
        StructField('usuario', StringType(), True)
    ]
)

df.withColumn(df_anuncio.show(), from_json(df_anuncio, schema))\
    .select(col('caracteristicas'), col('area_util'))\
    .show()

