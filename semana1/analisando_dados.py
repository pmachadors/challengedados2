from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkAnalisandoDados') \
                    .getOrCreate()

df = spark.read.json("dataset_bruto.json")

print(df.count())
# 89083
# df.show()

#Bucando apenas as colunas de anuncio
anuncio = df.select('anuncio.*')

anuncio.filter(anuncio.tipo_uso == 'Residencial').show()
# & anuncio.tipo_unidade == 'Apartamento' & anuncio.tipo_anuncio == 'Usado').show()





