import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val movieTitles = textFile.map(line => line.split(","))
                              .map(line => line(0))
                              .collect()

    var output = textFile.map(line => line.split(","))
                        .map(line => line.drop(1))
                        .map(line => line.map(x => if (x == "") "0" else x))
                        .map(line => line.map(x => x.toInt))
                        .map(line => line.map(x => if (x == line.max) 5 else 0))
                        .zipWithIndex
                        // .map(line => line.zipWithIndex.filter { case (value, index)  => value == 5 })
                        // .map(line => line(0)._2 + 1)
                        // .map(line => line.filter(_.nonEmpty))
    output.persist()
    
    // var maxRating = 1
    // find maxRating
    // val maxRating = output.map(_.max)
    // output.map(line => line.max(r => if(r.toInt > maxRating) maxRating = r.toInt))

    // output = output.map(line => line.zipWithIndex.filter{ case (value, index)  => value.toInt == maxRating })
                  //  .map{ case (value, index) => index }
    
    output.saveAsTextFile(args(1))
  }
}
