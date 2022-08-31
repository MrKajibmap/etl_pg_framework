import datetime
import os

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from config import HOST_NAME, DB_NAME, USER_NAME, PASSWORD
from pyarrow import binary
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, DoubleType
import pandas as pd
import pyspark.pandas as ps
import pyarrow as pa
import psycopg2
from psycopg2 import Error


def extract_pg(jdbcConnection: str, inputTableName: str, outputTableName:str, filterText: str, versionId: int, sourceSystemCd: str):
    print('Extract data (' + inputTableName + ') from PG has been started.')
    if len(sourceSystemCd) == 0:
        return
    outDf = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:" + jdbcConnection) \
        .option("dbtable", inputTableName) \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .load() \
        .withColumn('etl_extract_id', lit(versionId)) \
        .withColumn('etl_available_dttm', lit(datetime.datetime.now())) \
        .withColumn('source_system_cd', lit(sourceSystemCd))

    # outDf.insert
    if len(filterText) > 0:
        print('Filtering the input data-set...')
        outDf.filter(filterText)

    # TODO -> to config
    engine = create_engine('postgresql://admin:admin@localhost:5432/db')
    print('Loading data-set to PG "' + outputTableName + '"...' )
    outDf.toPandas().to_sql(outputTableName, engine, if_exists='replace', index=False)
    return outDf


def pg_connect_to_db(host: str, db: str, user: str, password: str):
    try:
        db_connection = psycopg2.connect(
            host=HOST_NAME,
            database=DB_NAME,
            user=USER_NAME,
            password=PASSWORD)
    except (Exception, Error) as error:
        print('Invalid db connect options.', '\n', error)
    else:
        cursor = db_connection.cursor()
        cursor.execute('select version()')
        print('Connected to PG: ', cursor.fetchall())
        cursor.close()
        return db_connection


def pg_check_exist_table(db_connection: psycopg2.extensions.connection, table_name: str):
    try:
        cursor = db_connection.cursor()
        cursor.execute('select * from ' + table_name + ' limit 1')
    except (Exception, Error) as error:
        print('Invalid table name: "', table_name, '".\n Please check name correction out.', '\n', error)
    finally:
        cursor.close()


def archive_pg(jdbcInConnect: str, jdbcOutConnect: str, inputTableName: str, outputTableName: str):
    print('Archive data (' + inputTableName + ') to PG (' + outputTableName + ') has been started.')
    conn = pg_connect_to_db(host=HOST_NAME, db=DB_NAME, user=USER_NAME, password=PASSWORD)
    pg_check_exist_table(table_name=inputTableName, db_connection=conn)
    cursor = conn.cursor()
    cursor.execute('select column_name from information_schema.columns where table_name =\'' + inputTableName +
                   '\'' + 'order by column_name asc')
    columnList = cursor.fetchall()
    generalString = ''
    count = 0
    # приводим к виду 'var1','var2','var3'.../ var1, var2, var3 ... для использования в sql
    for i in columnList:
        count = count + 1
        if count == len(columnList) + 1 or count == 1:
            coma = ''
        else:
            coma = ','
        # generalString = generalString + coma + '\'' + i[0] + '\''
        generalString = generalString + coma + i[0]

    try:
        print('Loading "' + inputTableName + '" into "'+outputTableName + '"...')
        cursor.execute('insert into ' + outputTableName + '(' + generalString + ') select ' + generalString
                       + ' from ' + inputTableName)
    except (Exception, Error) as error:
        print('Error has been defined while sql-q was processing. See the message below: \n', error)
    finally:
        conn.close()
        cursor.close()
        print('Archive step completed.')


if __name__ == '__main__':
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('PySpark_Tutorial') \
        .config("spark.jars", "/Users/nb/Documents/postgresql-42.4.0.jar") \
        .getOrCreate()
    os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
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

    extract_pg(jdbcConnection='//localhost:5432/db', inputTableName='first_table',
                           outputTableName = 'first_table_full',
                           filterText='fld IS NOT NULL and mark IS NOT NULL', versionId=150, sourceSystemCd='FCC')

    archive_pg(inputTableName='first_table_full',
               outputTableName='first_table_arch',
               jdbcOutConnect='',
               jdbcInConnect='',
               )
