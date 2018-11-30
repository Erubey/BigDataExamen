import org.apache.spark.sql.SparkSession

//2
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

//3
val spark = SparkSession.builder().getOrCreate()

//4
import org.apache.spark.ml.clustering.KMeans

//5
//val data  = spark.read.option("inferSchema", "true").format("csv").load("Archivos/Iris.csv")
val data = spark.read.option("inferSchema","true").csv("Iris.csv").toDF("SepalLength","SepalWidth","PetalLength","PetalWidth","class")

//6
val newcol = when($"class".contains("Iris-setosa"), 1.0).
    otherwise(when($"class".contains("Iris-virginica"), 3.0).
    otherwise(2.0))

val newdf = data.withColumn("etiqueta", newcol)
newdf.select("etiqueta","SepalLength","SepalWidth","PetalLength","PetalWidth","class").show(5, false)
//7
////Limpieza de los datos

//8
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

//9
val assembler = new VectorAssembler().setInputCols(Array("SepalLength","SepalWidth","PetalLength","PetalWidth","etiqueta")).setOutputCol("features")
//1
//val training_data = assembler.transform(feature_data).select($"features")
val features = assembler.transform(newdf)
features.show(5)
//1
val kmeans = new KMeans().setK(2).setSeed(1L)
val model = kmeans.fit(features)

//1
val WSSEw = model.computeCost(features)
println(s"Within set sum of Squared Errors = $WSSEw")

//1
println("Cluster Centers: ")
model.clusterCenters.foreach(println)
