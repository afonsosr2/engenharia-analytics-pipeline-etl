# Importando as bibliotecas necessárias para utilizar o ETL Jobs do Glue com Pyspark 
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Iniciando a sessão
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Gerando um DynamicFrame a partir de uma tabela no Data Catalog
estoques_zoop_dyf = glueContext.create_dynamic_frame.from_catalog(database='db-glue-zoop', table_name='zoop-glue-estoques_zoop_bronze_parquet')

# Convertendo o DynamicFrame em um DataFrame Spark
estoques_zoop_df = estoques_zoop_dyf.toDF()

## Tratando os textos da coluna produto
# Importando a função sql do Pyspark regexp_replace para troca de expressões
from pyspark.sql.functions import regexp_replace

# Trocando "_" por " " entre os nomes dos produtos
estoques_zoop_df = estoques_zoop_df.withColumn("Produto", regexp_replace("Produto", "_", " "))

### Ajustando datas

# Importando funções sql do Spark para formatar data
from pyspark.sql.functions import to_date

# Transformar a coluna Data realmente em uma data válida
estoques_zoop_df = estoques_zoop_df.withColumn("Data", to_date("Data", "yyyy/MM/dd"))

### Convertendo o DataFrame Spark em DynamicFrame e mapeando os tipos dos dados

# Importando a função que gera um DynamicFrame
from awsglue.dynamicframe import DynamicFrame

# Converter o DataFrame Spark em DynamicFrame
estoques_zoop_dyf = DynamicFrame.fromDF(estoques_zoop_df, glueContext)

# Mapeando as colunas do DynamicFrame
estoques_zoop_dyf_mapeado = estoques_zoop_dyf.apply_mapping(
    mappings=[
        ("ID_estoque", "long", "id_estoque", "long"),
        ("ID_produto", "long", "id_produto", "long"),
        ("Produto", "string", "produto", "string"),
        ("Categoria_produto", "string", "categoria_produto", "string"),
        ("Data", "date", "data", "date"),
        ("Horario", "string", "horario", "timestamp"),
        ("Quantidade_em_estoque", "long", "quantidade_em_estoque", "int"),
        ("Quantidade_novos_produtos", "long", "quantidade_novos_produtos", "int"),
        ("Quantidade_vendida", "long", "quantidade_vendida", "int")
    ]
)

# Configurando o glueContext sink para escrever nova tabela
s3output = glueContext.getSink(
  path="s3://nome-do-bucket/silver/estoques/",  # adicione o nome do seu bucket no path
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)

# Escrevendo a tabela no Glue Data Catalog
s3output.setCatalogInfo(
  catalogDatabase="db-glue-zoop", catalogTableName="zoop-glue-estoques_zoop_silver"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(estoques_zoop_dyf_mapeado)

job.commit()