import org.apache.spark.sql.SparkSession
//1
val spar = SparkSession.builder().getOrCreate()

//2
val df = spark.read.option("header", "true").option("inferSchema","true")csv("Netflix_2011_2016.csv")

//3
//df.schema.fields.foreach(x => println(x))
df.columns

//4
df.printSchema()

//5
df.show(5)

//6
df.describe().show()

//7
val df2 = df.withColumn("HV Ratio", df("High")/df("Volume"))
df2.show()

//8 para despues
df.groupBy("Date").agg(max("High")).show

//9 el profe dijo que era el precio de cuando se salio de la bolsa netflix
df.select("Close").describe().show()

//10
df.select(min("Volume"), max("Volume")).show()

//11
  //a
  df.filter("Close < 600").count()

  //b
  val porcentage = df.filter("High > 500").count().toDouble / df.select("High").count() * 100

  //c
  df.select(corr("High","Volume")).show()

  //d
  df.groupBy(year(df("Date"))).agg(max("High")).show

  //e
  df.groupBy(month(df("Date"))).agg(avg("Close")).show
