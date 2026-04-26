import os

os.environ["JAVA_HOME"] = r"C:\Java\jdk-17.0.12"
os.environ["SPARK_HOME"] = r"C:\Java\spark-3.5.3-bin-hadoop3\spark-3.5.3-bin-hadoop3"
os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;" +
    os.environ["SPARK_HOME"] + r"\bin;" +
    os.environ["PATH"]
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("C:/sample/bigdata/countries.csv", header=True)
df.show()
df.printSchema()
#%%
df.createOrReplaceTempView("countries")
#%%
spark.catalog.listTables()
#%%
# Câu 1: Hiển thị toàn bộ thông tin của một quốc gia khi biết tên quốc gia

spark.sql("SELECT * FROM countries WHERE Country = 'Vietnam'").show()
#%%
# Câu 2: Liệt kê các quốc gia thuộc vùng ASIA (EX. NEAR EAST)

spark.sql("SELECT * FROM countries WHERE Region = 'ASIA (EX. NEAR EAST)'").show()
#%% md

#%%
# Câu 3: Liệt kê 5 quốc gia có dân số đông nhất

spark.sql("SELECT Country, Population FROM countries ORDER BY Population DESC LIMIT 5").show()
#%%
# Câu 4a: Liệt kê 5 quốc gia có diện tích nhỏ nhất

spark.sql("SELECT Country, `Area (sq. mi.)` FROM countries ORDER BY `Area (sq. mi.)` ASC LIMIT 5").show()
#%%
# Câu 4b: Liệt kê 5 quốc gia có diện tích lớn nhất

spark.sql("SELECT Country, `Area (sq. mi.)` FROM countries ORDER BY `Area (sq. mi.)` DESC LIMIT 5").show()
#%%
# Câu 5a: Liệt kê 5 quốc gia có mật độ dân số nhỏ nhất

spark.sql("SELECT Country, `Pop. Density (per sq. mi.)` FROM countries ORDER BY `Pop. Density (per sq. mi.)` ASC LIMIT 5").show()
#%%
# Câu 5b: Liệt kê 5 quốc gia có mật độ dân số lớn nhất

spark.sql("SELECT Country, `Pop. Density (per sq. mi.)` FROM countries ORDER BY `Pop. Density (per sq. mi.)` DESC LIMIT 5").show()
#%%
# Câu 6: Liệt kê các quốc gia thuộc vùng NEAR EAST có dân số lớn hơn 15 triệu

spark.sql("SELECT Country, Region, Population FROM countries WHERE Region = 'NEAR EAST' AND Population > 15000000").show()
#%%
# Câu 7: Liệt kê 5 quốc gia châu Phi có tỷ lệ tử vong trẻ sơ sinh cao nhất

spark.sql("SELECT Country, Region, `Infant mortality (per 1000 births)` FROM countries WHERE Region = 'SUB-SAHARAN AFRICA' ORDER BY `Infant mortality (per 1000 births)` DESC LIMIT 5").show()
#%%
# Câu 8: Hiển thị khu vực có trung bình GDP cao nhất

spark.sql("SELECT Region, AVG(`GDP ($ per capita)`) FROM countries GROUP BY Region ORDER BY AVG(`GDP ($ per capita)`) DESC LIMIT 1").show()
#%%
# Câu 9a: Câu hỏi tự nghĩ - 5 quốc gia có GDP cao nhất

spark.sql("SELECT Country, Region, `GDP ($ per capita)` FROM countries ORDER BY `GDP ($ per capita)` DESC LIMIT 5").show()
#%%
# Câu 9b: Câu hỏi tự nghĩ - Đếm số quốc gia theo từng vùng

spark.sql("SELECT Region, COUNT(*) FROM countries GROUP BY Region ORDER BY COUNT(*) DESC").show()
#%%
# Câu 9c: Câu hỏi tự nghĩ - Các quốc gia có dân số trên 100 triệu

spark.sql("SELECT Country, Population FROM countries WHERE Population > 100000000 ORDER BY Population DESC").show()