import org.apache.spark.sql.SparkSession


//decarted posibilite error
import org.apache.log4j._
Logger.getLogger("org").setLevel(Level.ERROR)

val spark = SparkSession.builder().getOrCreate()

// impor libeery for task and ejercise
import org.apache.spark.ml.clustering.BisectingKMeans
// Loads data.
//val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")
val data = spark.read.option("inferSchema","true").csv("Iris.csv").toDF("SepalLength","SepalWidth","PetalLength","PetalWidth","class")

//6
val newcol = when($"class".contains("Iris-setosa"), 1.0).
    otherwise(when($"class".contains("Iris-virginica"), 3.0).
    otherwise(2.0))

val newdf = data.withColumn("etiqueta", newcol)
newdf.select("etiqueta","SepalLength","SepalWidth","PetalLength","PetalWidth","class").show(5, false)
//7
////Limpieza de los datos

//val feature_data = (data.select(data("Clicked on Ad").as("feature_data"), $"Fresh", $"Milk",$"Grocery", $"Frozen", $"Detergents_Paper",$"Delicassen")))
//val feature_data = data.select($"SepalLength", $"SepalWidth", $"PetalLength", $"PetalWidth", $"Detergents_Paper", $"etiqueta")

//8
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors

//9
val assembler = new VectorAssembler().setInputCols(Array("SepalLength","SepalWidth","PetalLength","PetalWidth","etiqueta")).setOutputCol("features")
//1
//val training_data = assembler.transform(feature_data).select($"features")
val features = assembler.transform(newdf)
features.show(5)



// Trains a bisecting k-means model.
val bkm = new BisectingKMeans().setK(2S).setSeed(1)
val model = bkm.fit(features)

// Evaluate clustering.
val cost = model.computeCost(features)
println(s"Within Set Sum of Squared Errors = $cost")

// Shows the result.
println("Cluster Centers: ")
val centers = model.clusterCenters
centers.foreach(println)
