from pyspark.sql import SparkSession, Row
from distutils.util import strtobool
import os.path
import yaml

# First Step : Create SparkSession object
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark Dataframe example") \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .getOrCreate()

# Second Step : set diretory and paths

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

# Third Step : load into yaml

    config = open(app_config_path)
    app_conf = yaml.load(config, Loader=yaml.FullLoader)

    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

# Fourth Step : create hadoop conf object
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    demographics_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/demographic.csv")

    demographics_pair_rdd = demographics_rdd \
        .map(lambda line: line.split(",")) \
        .map(lambda lst: (
    int(lst[0]), (int(lst[1]), strtobool(lst[2]), lst[3], lst[4], strtobool(lst[5]), strtobool(lst[6]), int(lst[7]))))

    demographics_pair_rdd.foreach(print)

#  spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" rdd/WorkingExample.py