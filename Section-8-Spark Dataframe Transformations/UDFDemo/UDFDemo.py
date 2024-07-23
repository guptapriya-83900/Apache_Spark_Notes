import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import Log4j

#create your function
def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UDF Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")

    survey_df.show(10)

#Register it as UDF and get the reference.
#Now your function is registered in the Spark Session.
#And your driver will serialize and send this function to the executors.So, executors can smoothly run this function.
    #The first one is to register your function as a Dataframe UDF. This method will not register the UDF in the catalog. It will only create a UDF and serialize the function to the executors. If you want to use your function in a Dataframe column object expressions,then you must register it like this.
    parse_gender_udf = udf(parse_gender, returnType=StringType())
    logger.info("Catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]

    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    #The second type of UDF registration is to register it as a SQL function.This one will also create one entry in the catalog.  if you want to use your function in a SQL expression, then you must register it like this.
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info("Catalog Entry:")
    [logger.info(r) for r in spark.catalog.listFunctions() if "parse_gender" in r.name]

    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)
