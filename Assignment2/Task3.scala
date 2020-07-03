import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name

// For each user, output the user's column number and the total number of ratings for that user
// If a user has no ratings, then output 0 for that user
// Ex/ 1,2 // user 1 has 2 ratings

object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val result =  sc.textFile(args(0))
                   .map(line => line.split(",",-1))
                   .map(line => line.drop(1))
                   .map(line => line.map(x => if (x == "") "0" else "1"))
                   .map(line => line.map(_.toInt))
                   .collect.transpose
                   .map(_.sum)
                   .zipWithIndex
                   .map{ case (value, index) => s"${index+1},$value"}

    sc.parallelize(result).saveAsTextFile(args(1))
  }
}
