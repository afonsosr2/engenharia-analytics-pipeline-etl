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
vendas_zoop_dyf = glueContext.create_dynamic_frame.from_catalog(database='db-glue-zoop', table_name='zoop-glue-vendas_zoop_bronze_parquet')

# Convertendo o DynamicFrame em um DataFrame Spark
vendas_zoop_df = vendas_zoop_dyf.toDF()

### Ajustando datas

# Importando funções sql do Spark para concatenar valores e formatar data
from pyspark.sql.functions import concat_ws, to_date

# Transformar as colunas dia, mes e ano em uma única coluna de data
vendas_zoop_df = vendas_zoop_df.withColumn("Data", concat_ws("-", "Ano", "Mês", "Dia"))
vendas_zoop_df = vendas_zoop_df.withColumn("Data", to_date("Data"))

# Remover as colunas dia, mês e ano
vendas_zoop_df = vendas_zoop_df.drop("Dia","Mês","Ano")

### Transformando estados por extenso em siglas

# Importando a UDF (User definition Function) - função definida pelo usuário e
# Tipos de manipulação de dados de string
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

estados_brasil = {
    "Acre": "AC", "Alagoas": "AL", "Amazonas": "AM", "Amapá": "AP", "Bahia": "BA",
    "Ceará": "CE", "Distrito Federal": "DF", "Espírito Santo": "ES", "Goiás": "GO", "Maranhão": "MA",
    "Minas Gerais": "MG", "Mato Grosso do Sul": "MS", "Mato Grosso": "MT", "Pará": "PA", "Paraíba": "PB",
    "Pernambuco": "PE", "Piauí": "PI", "Paraná": "PR", "Rio de Janeiro": "RJ", "Rio Grande do Norte": "RN",
    "Rondônia": "RO", "Roraima": "RR", "Rio Grande do Sul": "RS", "Santa Catarina": "SC", "Sergipe": "SE",
    "São Paulo": "SP", "Tocantins": "TO"
}

# Criando uma UDF (User Defined Function) para fazer a conversão de estado para sigla
def converter_estado(nome_estado):
    return estados_brasil.get(nome_estado)

# Registrando a UDF com o tipo de retorno String
converter_estado_udf = udf(converter_estado, StringType())

# Aplicando a UDF na coluna 'uf_cliente' para criar a nova coluna com as siglas
vendas_zoop_df = vendas_zoop_df.withColumn("UF_cliente", converter_estado_udf("UF_cliente"))

### Preenchendo as categorias faltantes

produtos_categorias = {
    'Smart TV 55"': 'Eletrônicos', 'Frigobar': 'Eletrodomésticos', 'Ventilador de teto': 'Eletrodomésticos',
    'Cafeteira': 'Eletrodomésticos', 'Smartphone': 'Eletrônicos', 'Liquidificador': 'Eletrodomésticos',
    'Notebook': 'Eletrônicos', 'Tablet': 'Eletrônicos', 'Micro-ondas': 'Eletrodomésticos',
    'Aspirador de pó': 'Eletrodomésticos', 'Câmera digital': 'Eletrônicos', 'Chuveiro elétrico': 'Eletrodomésticos',
    'Fone de ouvido': 'Eletrônicos', 'Ventilador de mesa': 'Eletrodomésticos', 'Impressora': 'Eletrônicos',
    'Secador de cabelo': 'Eletrodomésticos', 'Relógio inteligente': 'Eletrônicos', 'Batedeira': 'Eletrodomésticos',
    'Máquina de lavar roupa': 'Eletrodomésticos', 'Ferro de passar roupa': 'Eletrodomésticos', 'Cafeteira expresso': 'Eletrodomésticos',
    'Aparelho de som': 'Eletrônicos', 'Geladeira': 'Eletrodomésticos', 'Forno elétrico': 'Eletrodomésticos',
    'TV Box': 'Eletrônicos', 'Panela elétrica': 'Eletrodomésticos', 'Ventilador de coluna': 'Eletrodomésticos',
    'Câmera de segurança': 'Eletrônicos', 'Fritadeira elétrica': 'Eletrodomésticos', 'Máquina de café': 'Eletrodomésticos'
}

# Definindo uma UDF que usa o dicionário produtos_categorias para preencher as categorias vazias
def preencher_categoria(produto, categoria):
    if categoria is None:  # Se a categoria está faltando
        return produtos_categorias.get(produto)  # Retorna a categoria correspondente pelo nome do produto
    else:
        return categoria  # Mantém a categoria existente

# Registrando a UDF
preencher_categoria_udf = udf(preencher_categoria, StringType())

# Aplicando a UDF na coluna 'Categoria_produto'
vendas_zoop_df = vendas_zoop_df.withColumn("Categoria_produto", preencher_categoria_udf("Produto", "Categoria_produto"))

### Convertendo o DataFrame Spark em DynamicFrame e mapeando os tipos dos dados

# Importando a função que gera um DynamicFrame
from awsglue.dynamicframe import DynamicFrame

# Converter o DataFrame Spark em DynamicFrame
vendas_zoop_dyf = DynamicFrame.fromDF(vendas_zoop_df, glueContext)

# Mapeando as colunas do DynamicFrame
vendas_zoop_dyf_mapeado = vendas_zoop_dyf.apply_mapping(
    mappings=[
        ("ID_venda", "long", "id_venda", "long"),
        ("Data", "date", "data", "date"),
        ("Horario", "string", "horario", "timestamp"),
        ("Canal_venda", "string", "canal_venda", "string"),
        ("Origem_venda", "string", "origem_venda", "string"),
        ("ID_produto", "long", "id_produto", "long"),
        ("Produto", "string", "produto", "string"),
        ("Categoria_produto", "string", "categoria_produto", "string"),
        ("Preco_unitario", "double", "preco_unitario", "double"),
        ("Quantidade", "long", "quantidade", "int"),
        ("Metodo_pagamento", "string", "metodo_pagamento", "string"),
        ("ID_cliente", "long", "id_cliente", "long"),
        ("Nome_cliente", "string", "nome_cliente", "string"),
        ("Genero_cliente", "string", "genero_cliente", "string"),
        ("Idade_cliente", "long", "idade_cliente", "int"),
        ("Cidade_cliente", "string", "cidade_cliente", "string"),
        ("UF_cliente", "string", "uf_cliente", "string"),
        ("Regiao_cliente", "string", "regiao_cliente", "string"),
        ("Avaliacao", "long", "avaliacao", "int")
    ]
)

# Configurando o glueContext sink para escrever nova tabela
s3output = glueContext.getSink(
  path="s3://nome-do-bucket/silver/vendas/", # adicione o nome do seu bucket no path
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)

# Escrevendo a tabela no Glue Data Catalog
s3output.setCatalogInfo(
  catalogDatabase="db-glue-zoop", catalogTableName="zoop-glue-vendas_zoop_silver"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(vendas_zoop_dyf_mapeado)
job.commit()