import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val splitLinesWithKey = textFile.map(line => line.split(','))
    val splitLinesWithoutKey = splitLinesWithKey.map(line => line.drop(1))
    val nonEmptyLines = splitLinesWithoutKey.map(line => line.filter(_.nonEmpty))
    val integerLines = nonEmptyLines.map(line => line.map(_.toInt))
    val lineSizes = integerLines.map(line => line.size)

    val result = sc.parallelize(Seq(lineSizes.reduce(_+_)))
    result.saveAsTextFile(args(1))
  }
}
