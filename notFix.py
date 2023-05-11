from pyspark.sql import SparkSession

# buat SparkSession
spark = SparkSession.builder.appName("Contoh Program Apache Spark").getOrCreate()

# baca file CSV
df = spark.read.csv("/home/cloudera/spark-2.0.0-bin-hadoop2.7/death-rates-from-air-pollution.csv", header=True, inferSchema=True)

# filter data untuk daerah Indonesia
df = df.filter(df.Entity == "Indonesia")

# hapus kolom-kolom yang tidak dibutuhkan
df = df.drop("Code")

# ubah nama kolom
df = df.withColumnRenamed("Year", "Tahun").withColumnRenamed("Air pollution (total) (deaths per 100,000)", "Jumlah Kematian")

# urutkan data berdasarkan tahun secara ascending
df = df.orderBy("Tahun")

# atur ulang indeks setelah diurutkan
from pyspark.sql.functions import monotonically_increasing_id
df = df.withColumn("index", monotonically_increasing_id()).drop("index")

# membuat file csv setelah di proses
df.write.csv("file:///home/cloudera/spark-2.0.0-bin-hadoop2.7/death-rates-from-air-pollution-terbaru.csv", header=True)
df = spark.read.csv("file:///home/cloudera/spark-2.0.0-bin-hadoop2.7/death-rates-from-air-pollution-terbaru.csv", header=True, inferSchema=True)

# tampilkan lima data pertama
df.show()