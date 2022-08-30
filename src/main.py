import datetime
import os

from pyarrow import binary
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, DoubleType
import pandas as pd
import pyspark.pandas as ps
import pyarrow as pa
import psycopg2


def extract_pg(jdbc_connection: str, tableName: str, filterText: str, versionId: int, sourceSystemCd: str):
    if len(sourceSystemCd) == 0:
        return
    outDFname = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:"+jdbc_connection) \
        .option("dbtable", tableName) \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .load() \
        .withColumn('etl_extract_id', lit(versionId)) \
        .withColumn('etl_available_dttm', lit(datetime.datetime.now())) \
        .withColumn('source_system_cd', lit(sourceSystemCd))

    # outDFname.insert
    print(len(filterText))
    if len(filterText) > 0:
        outDFname.filter(filterText)
    print(type(outDFname))
    print('Count rows of final DF:', outDFname.count())
    outDFname.printSchema()
    return outDFname

def archive_pg(jdbc_in_connect: str, jdbc_out_connect: str, input_table_name: str, output_table_name:str ):
    conn = psycopg2.connect(
    host="127.0.0.1",
    database="db",
    user="admin",
    password="admin")
    cur = conn.cursor()
    cur.execute('select count(*) from first_table')
    a=cur.fetchone()
    print('type a is: ', type(a[0]))
    print('count of input table: ', a[0])
    conn.close()

if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('PySpark_Tutorial') \
        .config("spark.jars", "/Users/nb/Documents/postgresql-42.4.0.jar") \
        .getOrCreate()
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
    # See PyCharm help at https://www.jetbrains.com/help/pycharm/

    # Create DataFrame in PySpark Shell
    # data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    # df = spark.createDataFrame(data)
    # df.show()
    # nds = spark.read.csv('stocks_price_final.csv', sep=',', header=True)
    # nds.show()
    # nds.printSchema()
    # nds_schema = StructType([StructField('_c0', IntegerType(), True),
    #               StructField('symbol', StringType(), True),
    #               StructField('data', DateType(), True),
    #               StructField('open', DoubleType(), True),
    #               StructField('high', DoubleType(), True),
    #               StructField('low', DoubleType(), False),
    #               StructField('close', DoubleType(), True),
    #               StructField('volume', IntegerType(), True),
    #               StructField('adjusted', DoubleType(), False),
    #               StructField('market.cap', StringType(), True),
    #               StructField('sector', StringType(), True),
    #               StructField('industry', StringType(), True),
    #               StructField('exchange', StringType(), True)])
    # nds_structured = spark.read.csv('stocks_price_final.csv', schema=nds_schema, sep=',', header=True)
    # nds_structured.printSchema()
    # nds_structured.select('sector').show(140)
    # from pyspark.sql import functions as f
    #
    # nds_structured.filter((col('data') >= lit('2019-01-02')) | (col('data') <= lit('2020-01-31'))) \
    #     .groupBy("sector") \
    #     .agg(f.min("data").alias("С"),
    #          f.max("data").alias("По"),
    #          f.min("open").alias("Минимум при открытии"),
    #          f.max("open").alias("Максимум при открытии"),
    #          f.avg("open").alias("Среднее в open"),
    #          f.min("close").alias("Минимум при закрытии"),
    #          f.max("close").alias("Максимум при закрытии"),
    #          f.avg("close").alias("Среднее в close"),
    #          f.min("adjusted").alias("Скорректированный минимум"),
    #          f.max("adjusted").alias("Скорректированный максимум"),
    #          f.avg("adjusted").alias("Среднее в adjusted"),
    #          ).show(truncate=False)
    #
    # nds_structured.printSchema()
    # print(type(nds_structured))

    # psdf = nds_structured.to_pandas_on_spark()
    # print(type(psdf))

    # psdf1 = ps.DataFrame({'id': [1,2,3], 'score': [89, 97, 79]})
    # print(psdf1.head())

    # conn = psycopg2.connect(
    # host="127.0.0.1",
    # database="db",
    # user="admin",
    # password="admin")
    # #
    # cur = conn.cursor()
    # cur.execute('SELECT version()')
    # db_version = cur.fetchone()
    # print(db_version)
    #
    # df = pd.read_sql("select * from first_table where fld IS NOT NULL and mark IS NOT NULL", conn,index_col='id')
    # print(df.head(10))
    # print(type(df))
    # sdf1 = spark.read.jdbc()

    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/db") \
        .option("dbtable", "first_table") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .load().filter('fld IS NOT NULL and mark IS NOT NULL').fillna({'nil': 'Temp_Dummy'})

    print(type(df))

    print(df.head(20))
    outDFname = extract_pg(jdbc_connection='//localhost:5432/db', tableName='first_table', filterText='fld IS NOT NULL and mark IS NOT NULL', versionId=150, sourceSystemCd='FCC')
    outDFname.printSchema()
    print(outDFname.head(10))
    archive_pg(input_table_name='first_table', jdbc_out_connect='', jdbc_in_connect='', output_table_name='')