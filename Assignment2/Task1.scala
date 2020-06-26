import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val movieTitles = textFile.map(line => line.split(","))
                              .map(line => line(0))

    val output = textFile.map(line => line.split(","))
                         .map(line => line.drop(1))
                         .map(line => line.map(x => if (x == "") "0" else x))
                         .map(line => line.map(x => x.toInt))
                         .map(line => line.map(x => if (x == line.max) 5 else 0))
                         .map(line => line.zipWithIndex.map { case (value, index) => Array(value, index) })
                         .map(line => line.filter(x => x(0) == 5))
                         .map(line => line.map(x => (x(1)+1).toString).mkString(","))

    movieTitles.zip(output).map{ case (title, users) => s"$title,$users" }.saveAsTextFile(args(1))
  }
}
