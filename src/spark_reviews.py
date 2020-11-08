from pyspark.sql import SparkSession
# $example on:schema_merging$
from pyspark.sql import Row
# $example off:schema_merging$
from pyspark.sql.functions import col, concat_ws
import sys
import datetime



reviews_path = sys.argv[1]
metadata_path = sys.argv[2]
#window_start_date = sys.argv[3]
#window_end_date = sys.argv[4]
out = sys.argv[3]

def json_dataset_example(spark):
    # $example on:json_dataset$
    # spark is from the previous example.
    sc = spark.sparkContext

    # A JSON dataset is pointed to by path.
    # The path can be either a single text file or a directory storing text files
    df = spark.read.option("mode", "DROPMALFORMED").json(reviews_path)

    df.createOrReplaceTempView("reviews")
    df = spark.read.option("mode", "DROPMALFORMED").json(metadata_path)
    df.createOrReplaceTempView("metadata")

    sqlDF = spark.sql(
        """SELECT
                r.reviewerID
              , replace(r.reviewerName, '\n', ' ')
              , r.asin              
              , cast(r.helpful as string) helpful
              , cast(r.overall as integer)
              , r.summary
              , r.unixReviewTime
              , to_date(r.reviewTime, 'mm d, yyyy') reviewTime        
              , p.title
              , nvl(p.price, 0)
              , p.imUrl              
              , 'bla' as sales_rank
              , p.brand
              , element_at(element_at(p.categories, 1), 1) categories             
        FROM reviews r INNER JOIN  metadata p WHERE r.asin = p.asin""")
        #WHERE r.asin = p.asin AND from_unixtime(r.unixReviewTime, 'yyyy-MM-dd') >= {0} AND from_unixtime(r.unixReviewTime, 'yyyy-MM-dd') < {1}""".format(window_start_date, window_end_date))

    sqlDF.printSchema()
    sqlDF.show()
        
    sqlDF.write.option("sep", "\t").mode("overwrite").csv(out)

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL data source example") \
        .getOrCreate()
    json_dataset_example(spark)

    spark.stop()
