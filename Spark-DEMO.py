import findspark

findspark.init("G:/hadoop/Github/Hadoop-Fundamentals/venv/Lib/site-packages/pyspark")

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Linear Regression Model").config("spark.executor.memory", "1gb").getOrCreate()

sc = spark.sparkContext

rdd = sc.textFile('/Users/lemon/Downloads/CaliforniaHousing/cal_housing.data')

header = sc.textFile('Users/lemon/Downloads/CaliforniaHousing/cal_housing.domain')


rdd = rdd.map(lambda line: line.split(","))

rdd.first()

rdd.take(2)

from pyspark.sql import Row

df = rdd.map(lambda line: Row(longitude=line[0],
                              latitude=line[1],
                              housingMedianAge=line[2],
                              totalRooms=line[3],
                              totalBedRooms=line[4],
                              population=line[5],
                              households=line[6],
                              medianIncome=line[7],
                              medianHouseValue=line[8])).toDF()

df.show()

