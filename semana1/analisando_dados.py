from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, DoubleType


spark = SparkSession.builder.master("local[*]") \
                    .appName('SparkAnalisandoDados') \
                    .getOrCreate()

df = spark.read.json("dataset_bruto.json")

# print(df.count())
# 89083

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
valores = filtro_.withColumn('condominio',filtro_['valores']['condominio'][0].cast(DoubleType()))
valores = valores.withColumn('iptu',valores['valores']['iptu'][0].cast(DoubleType()))
valores = valores.withColumn('tipo',valores['valores']['tipo'][0])
valores = valores.withColumn('valor',valores['valores']['valor'][0].cast(DoubleType()))

valores = valores.drop('valores')

# Filtrando apenas o tipo Venda
valores_vendas = valores.select('*').where(func.col('tipo') == 'Venda')

# Salvando no formato parque
valores_vendas.write.format('parquet').save('valores_vendas_parquet')

# Limpando caracteristicas pra salvar em csv
valores_vendas_csv = valores_vendas.withColumn("caracteristicas",func.concat_ws(",",func.col("caracteristicas")))
valores_vendas_csv.write.format('csv').save('valores_vendas_csv')

valores_vendas_csv.write.format('csv').save('valores_vendas_csv')

valores_vendas_csv.write.format('csv').save('valores_vendas_csv')

