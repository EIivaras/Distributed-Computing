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
    val allMovies = sc.broadcast(textFile.map(line => line.split(",")).collect())

    val result = textFile.map(line => line.split(","))
                                     // Cross-product:
                                     // Takes a single list, turns it into a list of lists of lists (one list for each pair of movies)
                                     .map(line => allMovies.value.map(record => Array(line, record)))
                                     // A single line now looks like: [ [["LotR","5","4"],["LotR","5","4"]], [["LotR","5","4"],["Hype","2","5"]], [["LotR","5","4"],["Lit","1","3"]]]
                                     // Let's remove duplicates, using the "deep" property to see if arrays are equal
                                     .map(line => line.filter(outerArray => outerArray(0)(0) != outerArray(1)(0)))
                                     // We now transpose each array of arrays in the line so that we get one array for each user's ratings (which will have only 2 elements)
                                     .map(line => line.map(outerArray => outerArray.transpose))
                                     // A single line now looks like: [[["LotR","Hype"], ["5", "2"], ["4", "5"]], [["LotR", "Lit"], ["5", "1"], ["4", "3"]]
                                     // Now we need to clear out any arrays who have their lexicographical ordering wrong
                                     .map(line => line.filter(outerArray => outerArray(0)(0) < outerArray(0)(1)))
                                     // Now we need to clear out any arrays where at least one of the ratings is empty
                                     .map(line => line.map(outerArray => outerArray.filter(innerArray => innerArray(0) != "" && innerArray(1) != "")))
                                     // Now we need to split the names up from the ratings
                                     .map(line => line.map(outerArray => (outerArray(0), outerArray.drop(1))))
                                     // Now we need to map each list of ratings to 1s and 0s if their scores match
                                     .map(line => line.map(tuple => (tuple._1, tuple._2.map(innerArray => if (innerArray(0) == innerArray(1)) 1 else 0))))
                                     // Now we sum each list of ratings
                                     .map(line => line.map(tuple => (tuple._1, tuple._2.sum)))
                                     // Let's now convert the output to the form we want it in
                                     .flatMap(line => line.map(tuple => s"${tuple._1(0)}, ${tuple._1(1)}, ${tuple._2}"))
                                        
    result.saveAsTextFile(args(1))
  }
}
