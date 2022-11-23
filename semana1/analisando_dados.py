from pyspark.sql import SparkSession

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
.where('tipo_uso == "Residencial"')\
.where('tipo_unidade == "Apartamento"')\
.where('tipo_anuncio == "Usado"').show()





