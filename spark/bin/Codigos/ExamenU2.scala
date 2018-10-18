import org.apache.spark.sql.Encoders //sirve para poder utilizar el codigo de schema y colocar el nombre al encabezado
import org.apache.spark.sql.{DataFrame, SparkSession} //sirve para la conexion de los daots
import org.apache.spark.{SparkConf, SparkContext} //sirve para la conexion de los datos
import org.apache.spark.ml.feature.VectorAssembler //para crear los vectores de ml
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import MultilayerPerceptronClassifier._

/*Comienzo limpieza de datos */
//se crea dos clases para hacer la limpieza de datos, una es con el valor de la variable y la otra con el nuevo valor, para transformar el string a double
case class Initial(sepalLength: Option[Double], sepalWidth: Option[Double], petalLength: Option[Double], petalWidth: Option[Double], label: Option[String])
case class Final(sepalLength : Double, sepalWidth : Double, petalLength : Double, petalWidth : Double, label: Double)

//se crea el inicio de sesion
val conf = new SparkConf().setMaster("local[*]").setAppName("IrisSpark")
val sparkSession = SparkSession.builder.config(conf = conf).appName("spark session example").getOrCreate()
val path = "Iris.csv"
//se coloca nombre al encabezado de la tabla para que sea mas facil su modificacion
var irisSchema2 = Encoders.product[Initial].schema
val iris: DataFrame = sparkSession.read.option("inferSchema", "true").schema(irisSchema2).csv(path)
iris.show()


// Un ensamblador convierte los valores de entrada a un vector
// Un vector es lo que el algoritmo ML lee para entrenar un modelo.
// Establece las columnas de entrada de las cuales se supone que debemos leer los valores
// Establece el nombre de la columna donde se almacenará el vector
val assembler = new VectorAssembler().setInputCols(Array("sepalLength", "sepalWidth", "petalLength", "petalWidth", "label")).setOutputCol("features")


/* Antes de que podamos llamar al ensamblador, tendremos que convertir todos
 * Los valores de cadena para doblar y eliminar cualquier valor nulo.
 * Esta función limpiará esos datos por nosotros.
 */

def autobot(in: Initial) = Final(
    in.sepalLength.map(_.toDouble).getOrElse(0),
    in.sepalWidth.map(_.toDouble).getOrElse(0),
    in.petalLength.map(_.toDouble).getOrElse(0),
    in.petalWidth.map(_.toDouble).getOrElse(0),
    in.label match {
      case Some("Iris-versicolor") => 0.3;
      case Some("Iris-virginica") => 0.2;
      case Some("Iris-setosa") => 0.1;
      case _ => 0.4;
    }
  )

  // Una vez que tengamos todas las funciones listas, este es el momento de
  // aplicarlos y limpiar los datos.
val dataclean = assembler.transform(iris.as[Initial].map(autobot))
dataclean.show()

//val spark = SparkSession.builder.getOrCreate()

// $example on$
// Load the data stored in LIBSVM format as a DataFrame.
val datafull = dataclean


// Split the data into train and test
val splits = datafull.randomSplit(Array(0.6, 0.4), seed = 123L)
val train = splits(0)
val test = splits(1)


// especificar capas para la red neuronal:
// capa de entrada de tamaño 4 (características), dos intermedias de tamaño 5 y 4
val layers = Array[Int](5, 5, 4, 6)
// and output of size 3 (classes)

// create the trainer and set its parameters
val trainer = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(12).setSeed(System.currentTimeMillis).setMaxIter(99)

//  Convierte los labels indexados devuelta a los labels originales
//val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

// Encadena los indexados y la  MultilayerPerceptronClassifier en una  Pipeline.
//se usa para que se procese el flujo de trabajo, aprende la prediccion del modelo
//usando los features de los vectores o labels
//val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, trainer, labelConverter))

// train the model
val model = trainer.fit(train)

// compute accuracy on the test set
val result = model.transform(test)
result.show(5)
val predictionAndLabels = result.select("prediction", "label")
val evaluator = new MulticlassClassificationEvaluator().setLabelCol("probability").setPredictionCol("prediction").setMetricName("accuracy")
println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
// $example off$

spark.stop()
