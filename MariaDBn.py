
#do code em làm theo video không được, và lúc thầy giảng thì thầy có kêu lên maven tải file jar về bỏ vào và cấu hình SPARKHOME, nhưng nó vẫn
# bị lỗi, nên em có nhờ AI gợi  là nên chỉ dẫn đường dẫn tuyệt đối vào file ạ
import os
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = r"C:\Java\jdk-17.0.12"
os.environ["SPARK_HOME"] = r"C:\Java\spark-3.5.3-bin-hadoop3\spark-3.5.3-bin-hadoop3"
os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" +
    os.environ["SPARK_HOME"] + r"\bin;" +
    os.environ["PATH"]
)


def mariadb_connect():
    jar_path = r"C:\Java\jdbc\mariadb-java-client-3.5.8.jar"

    spark = (
        SparkSession.builder
        .appName("Mariadb")
        .master("local[*]")
        .config("spark.jars", jar_path)
        .config("spark.driver.extraClassPath", jar_path)
        .config("spark.executor.extraClassPath", jar_path)
        .getOrCreate()
    )

    jdbcDF = (
        spark.read
        .format("jdbc")
        .option("driver", "org.mariadb.jdbc.Driver")
        .option("url", "jdbc:mariadb://localhost:3306/retails")
        .option("dbtable", "users")
        .option("user", "root")
        .option("password", "123456")
        .load()
    )

    jdbcDF.printSchema()
    jdbcDF.show(10, truncate=False)

    jdbcDF.createOrReplaceTempView("newretail")

    spark.sql("""
        SELECT *
        FROM newretail
        LIMIT 10
    """).show(truncate=False)

    spark.stop()

mariadb_connect()

def read_table(table_name):
    return (
        spark.read
        .format("jdbc")
        .option("driver", "org.mariadb.jdbc.Driver")
        .option("url", "jdbc:mariadb://127.0.0.1:3306/retails_new")
        .option("dbtable", table_name)
        .option("user", "root")
        .option("password", "123456")
        .load()
    )

users = read_table("users")

# Exercise 1: Số user tạo theo từng năm
print("=== Exercise 1 ===")

users.withColumn("created_year", year(col("created_ts"))) \
    .groupBy("created_year") \
    .agg(count("*").alias("user_count")) \
    .orderBy("created_year") \
    .show()

# Exercise 2: Tên ngày sinh của user sinh trong tháng 5
print("=== Exercise 2 ===")

users.filter(month(col("user_dob")) == 5) \
    .withColumn("user_day_of_birth", date_format(col("user_dob"), "EEEE")) \
    .select("user_id", "user_dob", "user_email_id", "user_day_of_birth") \
    .orderBy(dayofmonth(col("user_dob"))) \
    .show()

# Exercise 3: User được thêm vào năm 2019
print("=== Exercise 3 ===")

users.withColumn("created_year", year(col("created_ts"))) \
    .filter(col("created_year") == 2019) \
    .withColumn(
        "user_name",
        upper(concat(col("user_first_name"), lit(" "), col("user_last_name")))
    ) \
    .select("user_id", "user_name", "user_email_id", "created_ts", "created_year") \
    .orderBy("user_name") \
    .show()

# Exercise 4: Số user theo giới tính
print("=== Exercise 4 ===")

users.withColumn(
        "user_gender",
        when(col("user_gender") == "M", "Male")
        .when(col("user_gender") == "F", "Female")
        .otherwise("Not Specified")
    ) \
    .groupBy("user_gender") \
    .agg(count("*").alias("user_count")) \
    .orderBy(col("user_count").desc()) \
    .show()

# Exercise 5: Lấy 4 chữ số cuối của unique ID
print("=== Exercise 5 ===")

users.withColumn(
        "digits_only",
        when(col("user_unique_id").isNull(), None)
        .otherwise(regexp_replace(col("user_unique_id"), "-", ""))
    ) \
    .withColumn(
        "user_unique_id_last4",
        when(col("user_unique_id").isNull(), "Not Specified")
        .when(length(col("digits_only")) < 9, "Invalid Unique Id")
        .otherwise(substring(col("digits_only"), -4, 4))
    ) \
    .select("user_id", "user_unique_id", "user_unique_id_last4") \
    .orderBy("user_id") \
    .show(30, truncate=False)

# Exercise 6: Số user theo country code
print("=== Exercise 6 ===")

users.filter(col("user_phone_no").isNotNull()) \
    .withColumn(
        "country_code",
        regexp_extract(col("user_phone_no"), r"^\+?(\d+)", 1)
    ) \
    .groupBy("country_code") \
    .agg(count("*").alias("user_count")) \
    .orderBy(col("country_code").cast("int")) \
    .show(30, truncate=False)