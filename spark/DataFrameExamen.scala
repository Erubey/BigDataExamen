import org.apache.spark.sql.SparkSession
//1
val spar = SparkSession.builder().getOrCreate()

//2
val df = spark.read.option("header", "true").option("inferSchema","true")csv("Netflix_2011_2016.csv")

//3
df.columns

//4
df.printSchema()

//5
df.show(5)

//6
df.describe().show()

//7
val df2 = df.withColumn("HighPlusLow", df("High")+df("Low"))

//8 para despues

//9 el profe dijo que era el precio de cuando se salio de la bolsa netflix
df.select("Volume").show()

//10
val df2min = df2.("Volume").min()
df2min.show()

val df2max = df2.("Volume").max()
df2max.show()
