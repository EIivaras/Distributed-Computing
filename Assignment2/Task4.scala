import org.apache.spark.{SparkContext, SparkConf}

// Note: When does spark split up work?
// --> When we do a transformation on an RDD to make a new RDD, the transformation DOES NOT HAPPEN RIGHT AWAY
// --> Once an ACTION is performend, THEN spark gives the program context to a driver that creates a directed acyclic graph (DAG)
// --> The driver then divides the DAG into a number of stages, which are then divided into smaller tasks and the tasks are given to the executors.
// What this means is that if broadcast variables are defined BEFORE any actions are executed, ALL TASKS WILL HAVE ACCESS TO IT
// --> I think this means we want to put the textFile RDD into a broadcast variable which can then be joined/accessed by each worker executing each task.

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    val allMovies = sc.broadcast(textFile.map(line => line.split(",", -1)).collect())

    val result = textFile.map(line => line.split(",", -1))
                          // Cross-product:
                          // Takes a single list, turns it into a list of tuples of tuples (one tuple for each pair of movies)
                         .map(line => allMovies.value.map(record => ((line(0), record(0)), (line.drop(1), record.drop(1)))))
                         // A single line now looks like: [ (("LotR", "LotR"), (["5", "4"], ["5", "4"])), (("LotR","Hype"), (["5", "4"], ["2","5"]))]
                         // Remove duplicates and those with the wrong lexicographical ordering
                         .map(line => line.filter(tuple => tuple._1._1 < tuple._1._2))
                         // .zipped function takes a tuple of lists and transposes it into a list of tuples, each tuple of which we convert to 1 or 0 before summing
                         .map(line => line.map(tuple => (tuple._1, tuple._2.zipped.map((a, b) => if (a == b && a != "" && b != "") 1 else 0).sum)))
                         // Format output correctly
                         .flatMap(line => line.map(tuple => s"${tuple._1._1},${tuple._1._2},${tuple._2}"))
                         
    result.saveAsTextFile(args(1))
  }
}
