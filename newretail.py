from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower

spark = SparkSession.builder.getOrCreate()
df2 = spark.read.csv("C:/sample/bigdata/new_retail_data.csv", header=True)
df2.show()
df2.printSchema()
df2.createOrReplaceTempView("newretail")
spark.catalog.listTables()


# Câu 1: Đếm số đơn hàng của từng khách hàng trong tháng 1 năm 2024

spark.sql("SELECT Customer_ID, Name, COUNT(Transaction_ID) AS customer_order_count FROM newretail WHERE Year = '2024' AND Month = '1' GROUP BY Customer_ID, Name ORDER BY customer_order_count DESC, Customer_ID ASC").show()
# Câu 2: Khách hàng không có đơn hàng trong tháng 1 năm 2024

spark.sql("SELECT DISTINCT Customer_ID, Name, Email, Phone, Address, City, State, Zipcode, Country, Age, Gender, Income, Customer_Segment FROM newretail WHERE Customer_ID NOT IN (SELECT Customer_ID FROM newretail WHERE Year = '2024' AND Month = '1') ORDER BY Customer_ID ASC").show()

# Câu 3: Doanh thu của từng khách hàng trong tháng 1 năm 2024

spark.sql("SELECT Customer_ID, Name, SUM(CAST(Total_Amount AS DOUBLE)) AS customer_revenue FROM newretail WHERE Year = '2024' AND Month = '1' AND Order_Status IN ('COMPLETE', 'CLOSED') GROUP BY Customer_ID, Name ORDER BY customer_revenue DESC, Customer_ID ASC").show()
# Câu 4: Doanh thu theo từng loại sản phẩm trong tháng 1 năm 2024

spark.sql("SELECT Product_Category, SUM(CAST(Total_Amount AS DOUBLE)) AS category_revenue FROM newretail WHERE Year = '2024' AND Month = '1' AND Order_Status IN ('COMPLETE', 'CLOSED') GROUP BY Product_Category ORDER BY Product_Category ASC").show()
# Câu 5: Đếm số sản phẩm theo từng loại sản phẩm

spark.sql("SELECT Product_Category, COUNT(products) AS product_count FROM newretail GROUP BY Product_Category ORDER BY Product_Category ASC").show()