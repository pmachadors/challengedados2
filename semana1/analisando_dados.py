from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, DoubleType


spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkAnalisandoDados') \
                    .getOrCreate()

df = spark.read.json("dataset_bruto.json")

# print(df.count())
# 89083
# df.show()

anuncio = df.select('anuncio.*')

#Filtrando resultados
filtro = anuncio\
.select('*')\
.where((func.col('tipo_uso') == 'Residencial')\
& (func.col('tipo_unidade') == 'Apartamento')\
& (func.col('tipo_anuncio') == 'Usado'))

# Mudando tipo da variavel array para int
filtro_ = filtro.withColumn("quartos",filtro.quartos[0].cast(IntegerType()))
filtro_ = filtro_.withColumn("suites",filtro.suites[0].cast(IntegerType()))
filtro_ = filtro_.withColumn("banheiros",filtro.banheiros[0].cast(IntegerType()))
filtro_ = filtro_.withColumn("vaga",filtro.vaga[0].cast(IntegerType()))
filtro_ = filtro_.withColumn("area_total",filtro.area_total[0].cast(IntegerType()))
filtro_ = filtro_.withColumn("area_util",filtro.area_util[0].cast(IntegerType()))

# Extraindo bairro e zona e excluíndo endereco
filtro_ = filtro_.withColumn('bairro',func.col('endereco.bairro'))
filtro_ = filtro_.withColumn('zona',func.col('endereco.zona'))
filtro_ = filtro_.drop(func.col("endereco"))

# Extraindo os valores do array valores
valores = filtro_.withColumn('condominio',func.explode(func.col('valores.condominio')))
valores = valores.withColumn('iptu',func.explode(func.col('valores.iptu')))
valores = valores.withColumn('tipo',func.explode(func.col('valores.tipo')))
valores = valores.withColumn('valor',func.explode(func.col('valores.valor')))

# Filtrando apenas o tipo Venda
valores_vendas = valores.select('*').where(func.col('tipo') == 'Aluguel')

# tentando entender por que os dataframes ficam com mais linhas com o explode

