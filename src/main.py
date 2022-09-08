import datetime
import os
import re

from numpy.core.defchararray import upper
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from config import HOST_NAME, DB_NAME, USER_NAME, PASSWORD, HOST_ENGINE
from pyarrow import binary
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, DoubleType
import pandas as pd
import pyspark.pandas as ps
import pyarrow as pa
import psycopg2
from psycopg2 import Error, OperationalError


def extract_pg(jdbcConnection: str, inputTableName: str, outputTableName: str, filterText: str, versionId: int,
               sourceSystemCd: str):
    print('Extracting data (' + inputTableName + ') from PG has been started.')
    if len(sourceSystemCd) == 0:
        print('Invalid value for parameter SourceSystemCD.')
        exit()
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

    engine = create_engine('postgresql://' + USER_NAME + ':' + PASSWORD + '@' + HOST_ENGINE)
    print('Loading data-set to PG "' + outputTableName + '"...')
    outDf.toPandas().to_sql(outputTableName, engine, if_exists='replace', index=False)
    return outDf


def pg_connect_to_db(host: str, db: str, user: str, password: str):
    print('Trying to connect to PG...')
    try:
        db_connection = psycopg2.connect(
            host=HOST_NAME,
            database=DB_NAME,
            user=USER_NAME,
            password=PASSWORD)
    except OperationalError as error:
        print('Invalid db connect options were passed.', '\n', error)
        print('Type of error:', type(error))
        exit(print('End of processing.'))
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
        print('Loading "' + inputTableName + '" into "' + outputTableName + '"...')
        cursor.execute('insert into ' + outputTableName + '(' + generalString + ') select ' + generalString
                       + ' from ' + inputTableName)
    except (Exception, Error) as error:
        print('Error has been defined while sql-q was processing. See the message below: \n', error)
    finally:
        conn.close()
        cursor.close()
        print('Archive step completed.')


def generate_bk(inputTableName: str,
                outputTableName: str,
                businessKeyCd: str,
                optionalFlg: bool
                ):
    conn = pg_connect_to_db(host=HOST_NAME, db=DB_NAME, user='etl_sys', password='etl_sys')
    cursor = conn.cursor()
    pg_check_exist_table(table_name='etl_sys.etl_bk', db_connection=conn)
    pg_check_exist_table(table_name='etl_sys.etl_bk_type', db_connection=conn)
    cursor.execute('SELECT bk_cd, bk_type_cd, bk_field_nm, bk_format_txt, bk_column_list_txt FROM etl_sys.etl_bk '
                   'where upper(bk_cd)=\'%s\'' % upper(businessKeyCd))
    columns_list = ['bk_cd', 'bk_type_cd', 'bk_field_nm', 'bk_format_txt', 'bk_column_list_txt']
    res = cursor.fetchall()
    # print('>>>> original view: ', res, '\nlength = ', len(list(res[0])))
    # if execute-response is empty:
    if len(res) == 0:
        print('Invalid businessKeyCd. Check out input parameters. Current invalid value == \'', businessKeyCd, '\'')
        exit(print('End of processing.'))
    if len(list(res[0])) == len(columns_list):
        column_value_list = {columns_list[i]: list(res[0])[i] for i in range(len(columns_list))}
    else:
        print('Invalid data for business key (%s) has defined in etl_sys.etl_bk.' % businessKeyCd)
    # parse bk_format_txt
    print('>>>> ', str(column_value_list['bk_format_txt']), ' ', type(column_value_list['bk_format_txt']))
    print('len: ', len(column_value_list['bk_format_txt']))
    print(type(str(column_value_list['bk_format_txt']).split(sep='_')))
    format_component_list = {}
    # razmetka spiska na konstanta (0) and input columns values (1)
    for i in range(len(str(column_value_list['bk_format_txt']).split(sep='_'))):
        print('INFO: main cycle: i = ', i)
        flg = 0
        print(str(column_value_list['bk_format_txt']).split(sep='_')[i])
        # format_component_list[str(column_value_list['bk_format_txt']).split(sep='_')[i]] = 0
        if str(column_value_list['bk_format_txt']).split(sep='_')[i].find('{') != -1 and \
                str(column_value_list['bk_format_txt']).split(sep='_')[i].find('}') != -1:
            format_component_list[str(column_value_list['bk_format_txt']).split(sep='_')[i]] = 1
            # comparing regexp with column value
            # firstly, validate regexp formula
            formula = str(column_value_list['bk_format_txt']).split(sep='_')[i]
            if len(str(re.findall(r'{[C|Z|N|T|D]\d+}', formula))) > 0:
                ctype = str(re.findall(r'C|Z|N|T|D', formula)[0])
                clength = str(re.findall(r'\d+', str(formula)[2:])[0])
            else:
                exit(print('TechError: [errinfo] Invalid bk_format_txt: \'', str(column_value_list['bk_format_txt']),
                           '\'.'))
    # transformation to form {'{C3}': 'FINANCIAL_INSTR_ASSOC_TYPE_CD', '{N26}': 'BLOCKNUMBER'}
    count = 0
    for i in format_component_list:
        print(i)
        print(column_value_list['bk_column_list_txt'])
        print(str(column_value_list['bk_column_list_txt']).split(sep=' ')[count])
        format_component_list[i] = str(column_value_list['bk_column_list_txt']).split(sep=' ')[count]
        count = count + 1
    print(inputTableName.split(sep='.')[0], '+', inputTableName.split(sep='.')[1])
    print('INFO: format_component_list: ', format_component_list)
    bk_elements = {}
    for i in format_component_list:
        print('>> 1 >> : %s' % i)
        print('>> 2 >>: %s' % format_component_list[i])
        print('INFO: index of I : ', list(format_component_list.keys()).index(i))
        cursor.execute(''' select upper(data_type) , coalesce(character_maximum_length, 0)
                        from information_schema.columns
                        where upper(table_schema) = \'%s\'
                        and upper(table_name) = \'%s\'
                        and upper(column_name) = \'%s\' ''' % (
            upper(str(inputTableName.split(sep='.')[0])), upper(str(inputTableName.split(sep='.')[1])),
            upper(str(format_component_list[i]))))
        sql_response = cursor.fetchall()
        if len(sql_response) == 0:
            exit(print('TechError: [errinfo] Invalid sql response has been received.'))
        print('INFO: list(list(sql_response)[0])[0]: ', list(list(sql_response)[0])[0])
        print('INFO: list(list(sql_response)[0])[1]: ', list(list(sql_response)[0])[1])
        print('INFO: len(str(i)): ', len(str(i)))
        if str(i)[1] == 'C':
            if str(list(sql_response)[0]).find('CHAR'):
                print('INFO: pg_type: ', str(list(sql_response)[0]), ' is CHAR')
                el_len = str(re.findall(r'\d+', str(i)[2:])[0])
                # print('INFO: el len :', el_len)
                # print('INFO: str(list): ',
                #       str(list(sql_response)[0]).split(sep=',')[1].replace('(', '').replace(')', ''))
                # присваиваем элементу Дамми значение (повтор литеры F на всю длину элемента из формулы)
                # if int(el_len) != int(str(list(sql_response)[0]).split(sep=',')[1].replace('(', '').replace(')', '')):
                if int(el_len) != int(list(list(sql_response)[0])[1]):
                    print(
                        'ETLWarning: [warninginfo] Invalid length defined in ETL_BK for column \' %s \'' % upper(
                            str(format_component_list[i])), '. Dummy will be defined.')
                    bk_elements[list(format_component_list.keys()).index(i)] = 'F' * int(el_len)
        if str(i)[1] == 'N':
            if str(list(list(sql_response)[0])[0]) in ['SMALLINT', 'INTEGER', 'BIGINT', 'DECIMAL', 'NUMERIC', 'REAL',
                                               'SMALLSERIAL', 'SERIAL', 'BIGSERIAL']:
                print('INFO: pg_type: ', list(list(sql_response)[0])[0], ' is NUM.')
                el_len = str(re.findall(r'\d+', str(i)[2:])[0])
                if int(list(list(sql_response)[0])[1]) != 0 and int(el_len) != int(list(list(sql_response)[0])[1]):
                    print(
                        'ETLWarning: [warninginfo] Invalid length defined in ETL_BK for column \' %s \'' % upper(
                            str(format_component_list[i])), '. Dummy will be defined.')
                    bk_elements[list(format_component_list.keys()).index(i)] = 'F' * int(el_len)
        if str(i)[1] == 'D':
            if str(list(list(sql_response)[0])[0]) in ['DATE', 'TIMESTAMP']:
                print('INFO: pg_type: ', list(list(sql_response)[0])[0], ' is Date/Timestamp.')

        if str(i)[1] == 'T':
            if str(list(list(sql_response)[0])[0]) in ['TIME']:
                print('INFO: pg_type: ', list(list(sql_response)[0])[0], ' is Time.')

        # присваиваем элементу имя колонки входной таблицы
        bk_elements[list(format_component_list.keys()).index(i)] = format_component_list[i]

    cursor.close()
    conn.close()


if __name__ == '__main__':
    # spark = SparkSession.builder \
    #     .master("local[*]") \
    #     .appName('PySpark_Tutorial') \
    #     .config("spark.jars", "/Users/nb/Documents/postgresql-42.4.0.jar") \
    #     .getOrCreate()
    # os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
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

    # extract_pg(jdbcConnection='//localhost:5432/db', inputTableName='first_table',
    #            outputTableName='first_table_full',
    #            filterText='fld IS NOT NULL and mark IS NOT NULL', versionId=150, sourceSystemCd='FCC')

    archive_pg(inputTableName='first_table_full',
               outputTableName='first_table_arch',
               jdbcOutConnect='',
               jdbcInConnect='',
               )
    generate_bk(inputTableName='fcc.fcc_test_table', outputTableName='', businessKeyCd='FIN_INSTR_ASSOC_KPS_FSP_OPT',
                optionalFlg='')
