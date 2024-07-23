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

