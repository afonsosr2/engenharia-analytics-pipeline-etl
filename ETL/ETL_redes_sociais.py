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
redes_sociais_zoop_dyf = glueContext.create_dynamic_frame.from_catalog(database='db-glue-zoop', table_name='zoop-glue-redes_sociais_zoop_bronze_parquet')

# Convertendo o DynamicFrame em um DataFrame Spark
redes_sociais_zoop_df = redes_sociais_zoop_dyf.toDF()

### Tratando os textos da coluna Comentario e geração da coluna Avaliacao

# Importando a função sql do Pyspark regexp_replace para troca de expressões e regexp_extract para extração de textos
from pyspark.sql.functions import regexp_extract, regexp_replace

# Extrair a nota da avaliação e salvar na coluna 'Avaliacao'
redes_sociais_zoop_df = redes_sociais_zoop_df.withColumn("Avaliacao", regexp_extract("Comentario", r"Nota (\d)", 1))

# Remover o texto "Nota valor" do comentário original
redes_sociais_zoop_df = redes_sociais_zoop_df.withColumn("Comentario", regexp_replace("Comentario", r" Nota \d", ""))

### Convertendo o DataFrame Spark em DynamicFrame e mapeando os tipos dos dados

# Importando a função que gera um DynamicFrame
from awsglue.dynamicframe import DynamicFrame

# Converter o DataFrame Spark em DynamicFrame
redes_sociais_zoop_dyf = DynamicFrame.fromDF(redes_sociais_zoop_df, glueContext, "glue_etl")

# Mapeando as colunas do DynamicFrame
redes_sociais_zoop_dyf_mapeado = redes_sociais_zoop_dyf.apply_mapping(
    mappings=[
        ("ID_social", "long", "id_estoque", "long"),
        ("Data", "string", "data", "date"),
        ("Influencia_autor", "long", "influencia_autor", "int"),
        ("Plataforma", "string", "plataforma", "string"),
        ("Nome_produto", "string", "produto", "string"),
        ("Categoria_produto", "string", "categoria_produto", "string"),
        ("Avaliacao", "string", "avaliacao", "int"),
        ("Comentario", "string", "comentario", "string")        
    ]
)

# Configurando o glueContext sink para escrever nova tabela
s3output = glueContext.getSink(
  path="s3://nome-do-bucket/silver/redes-sociais/",   # adicione o nome do seu bucket no path
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)

# Escrevendo a tabela no Glue Data Catalog
s3output.setCatalogInfo(
  catalogDatabase="db-glue-zoop", catalogTableName="zoop-glue-redes_sociais_zoop_silver"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(redes_sociais_zoop_dyf_mapeado)

job.commit()