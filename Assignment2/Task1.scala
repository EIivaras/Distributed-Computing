import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile.map(line => line.split(","))
                         .map(line => line.map(x => if (x == "") "0" else x))
                         .map(line => (line(0), line.drop(1)))
                         .map(t => (t._1, t._2.map(x => x.toInt)))
                         .map(t => (t._1, t._2, t._2.max))
                         .map(t => (t._1, t._2.map(x => if (x == t._3) 5 else 0)))
                         .map(t => (t._1, t._2.zipWithIndex))
                         .map(t => (t._1, t._2.filter(x => x._1 == 5)))
                         .map(t => (t._1, t._2.map(x => (x._2 + 1).toString).mkString(",")))
                         .map(t => t._1 + "," + t._2)

    output.saveAsTextFile(args(1))
  }
}
