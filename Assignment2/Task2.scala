import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val result = sc.textFile(args(0))
                   .map(line => line.split(","))
                   .map(line => line.drop(1))
                   .map(line => line.filter(_.nonEmpty))
                   .map(line => line.map(_.toInt))
                   .map(line => ("key", line.size))
                   .reduceByKey(_+_, 1)
                   .map{ case (k, v) => v }

    result.saveAsTextFile(args(1))
  }
}
