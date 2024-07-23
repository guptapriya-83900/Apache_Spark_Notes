import configparser

from pyspark import SparkConf

def get_spark_confs():
    spark_conf = SparkConf()
    config = configparser.RawConfigParser()
    config.optionxform = str
    config.read("spark.conf")
    for k, v in config.items("SPARK_APP_CONFIGS"):
        print(f"k={k}, v={v}")
        spark_conf.set(k, v)
    return spark_conf

def load_data(spark, data_file):
    df = spark.read \
        .option("header","true") \
        .option("inferSchema","true") \
        .csv(data_file)

    return df

def count_by_country(df):
    filtered_survey_df = df.where("Age < 40") \
        .select("Age", "Gender", "Country", "state")
    grouped_df = filtered_survey_df.groupBy("Country")
    count = grouped_df.count()

    return count