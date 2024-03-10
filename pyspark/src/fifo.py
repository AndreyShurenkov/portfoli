###############################################
# ФИФО
###############################################

"""
    Запус из терминала командой
    source ./venv/bin/activate
    ./venv/bin/spark-submit ./src/python/example/fifo.py
    ./venv/bin/python3 ./src/python/example/fifo.py
"""
from pyspark.sql import SparkSession, types
import random
import string
import datetime


# Источник данны
PATH_IMPORT_DAT = './resources/import/fifo_dat/fifo_dat.'
# Целевая таблица для результата
PATH_EXPORT_RES = './resources/export/fifo_res/fifo_res.'

# Общая схема
FIFO_SCHEMA = types.StructType([types.StructField("agrmnt", types.StringType(),False),
                                types.StructField("period", types.DateType(),False),
                                types.StructField("agrmnt_sum", types.IntegerType(),False),
                                types.StructField("type_operation", types.StringType(),False)
                                ])
    

# Генерируем данные для ФИФО
def get_user_dataset(spark: SparkSession):
    letters = string.ascii_uppercase
    fifo_dataset = []
    for agr in ['01/002/'+''.join(random.choice(letters) for i in range(10)) for i2 in range(2)]:
        agrmnt_sum = random.randint(1000,2000)
        # Создаем начисления
        fifo_dataset = fifo_dataset + [(agr, datetime.date(datetime.datetime.now().year,pr+1,1), agrmnt_sum, "A") for pr in range(12)]
        # Создаем оплаты, делаем так что может образоваться по итогу оплат либо перебор либо недобор
        fifo_dataset = fifo_dataset + [(agr, datetime.date(datetime.datetime.now().year,pr+1,random.randint(1,20)), random.randint(agrmnt_sum-100,agrmnt_sum+100), "P") for pr in range(12)]

    return spark.createDataFrame(fifo_dataset, FIFO_SCHEMA)

# Импортируем данные для ФИФО из файла
def get_from_file_dataset(spark: SparkSession, source: str):
    if source == 'csv':
        df1 = (spark.read.format(source)
               .option("header", True)
               .option("sep", ';')
               .option("quote", '"')
               .schema(FIFO_SCHEMA)
               .load(PATH_IMPORT_DAT+source))
    elif source == 'json':
        df1 = (spark.read.format('json')
               .option("multiline", True)
               .schema(FIFO_SCHEMA)
               .load(PATH_IMPORT_DAT+source))

# Расчет ФИФО
def get_result_fifo(df: SparkSession):
    df.createOrReplaceTempView("fifo")
    # Фрейм по начислению
    df_a = spark.sql("SELECT * FROM fifo WHERE type_operation = 'A'")
    # Фрейм по платежам
    df_p = spark.sql("SELECT * FROM fifo WHERE type_operation = 'P'")

###############################################
spark = (SparkSession
            .builder
            .appName("Example of getting data from various data sources")
            .getOrCreate())

df = get_user_dataset(spark)
get_result_fifo(df)


    
