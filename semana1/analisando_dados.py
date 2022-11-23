from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkAnalisandoDados') \
                    .getOrCreate()

df = spark.read.json("dataset_bruto.json")

print(df.count())
# 89083
# df.show()

anuncio = df.select('anuncio.*')
print (anuncio.show())


