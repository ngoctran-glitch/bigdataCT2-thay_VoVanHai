import pyspark

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import col, explode, split, lower\

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("WordCountSample")
             .getOrCreate()
             )

    data = spark.read.text("c:/sample/bigdata.txt")
    data.show()

# data.show() # Dòng này đang bị comment trong hình

word_counts = (
    data.withColumn("word", explode(split(col("value"), " ")))
        #.withColumn("word", lower(col("word"))) # Chuyển về chữ thường (Optional)
        .groupBy("word")
        .count()
)

# Hiển thị kết quả
word_counts.show()