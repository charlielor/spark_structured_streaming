from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType

spark = SparkSession.builder.appName("StructuredFileSourceStream").getOrCreate()

# sets log level to ERROR
spark.sparkContext.setLogLevel("ERROR")

df = (
    spark.readStream.format("json")
    .option("path", "data/stream")
    .option("cleanSource", "delete")
    .schema(
        StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField(
                    "address",
                    StructType(
                        [
                            StructField("street", StringType(), True),
                            StructField("city", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("zip", StringType(), True),
                        ]
                    ),
                ),
            ]
        )
    )
    .load()
)

showDf = df.groupBy("address.city").count()

(
  showDf
  .writeStream
  .outputMode("update")
  .option("truncate", False)
  .format("console")
  .start()
  .awaitTermination()
)
