import sys

from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J
from lib.utilis import get_spark_confs
from lib.utilis import load_data
from lib.utilis import count_by_country

if __name__ == "__main__":
    conf = get_spark_confs()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Starting HelloSpark")
    #Your Processing Code

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    survey_df = load_data(spark,sys.argv[1])
    partitioned_df = survey_df.repartition(2)
    # filtered_survey_df = survey_df.where("Age < 40") \
    #     .select("Age", "Gender", "Country", "state")
    # grouped_df = filtered_survey_df.groupBy("Country")
    # count_df = grouped_df.count()
    # count_df.show()
    count_df = count_by_country(partitioned_df)
    logger.info(count_df.collect())
    

    logger.info("Ending HelloSpark")

    spark.stop()