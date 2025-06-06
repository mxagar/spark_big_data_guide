from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("HelloLocalSpark").getOrCreate()
    df = spark.range(10)
    df.show()
    spark.stop()

if __name__ == "__main__":
    main()