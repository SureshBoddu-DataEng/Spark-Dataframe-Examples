from pyspark.sql import SparkSession, Row
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
    app_config = yaml.load(config, Loader=yaml.FullLoader)

    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

# Fourth Step : create hadoop conf object
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key",app_secret["s3_config"]["aceess_key"])
    hadoop_conf.set("fs.s3a.secret.access.key", app_secret["s3_config"]["secret_aceess_key"])

    txn_fct_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/txn_fct.csv") \
        .filter(lambda record: record.find("txn_id")) \
        .map(lambda record: record.split("|")) \
        .map(lambda record: (int(record[0]), record[1], float(record[2]), record[3], record[4], record[5], record[6]))

    # RDD[(Long, Long, Double, Long, Int, Long, String)]
    for rec in txn_fct_rdd.take(5):
        print(rec)

#  spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" rdd/WorkingExample.py