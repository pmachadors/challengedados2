from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkAnalisandoDados') \
                    .getOrCreate()

df = spark.read.json("dataset_bruto.json")

# print(df.count())
# 89083
# df.show()


anuncio = df.select('anuncio.*')

filtro = anuncio\
.select('*')\
.where((func.col('tipo_uso') == 'Residencial')\
& (func.col('tipo_unidade') == 'Apartamento')\
& (func.col('tipo_anuncio') == 'Usado')).show()





