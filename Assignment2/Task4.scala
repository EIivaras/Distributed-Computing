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
    // TODO: Is this right? If collect counts as an action is this split up between multiple tasks?
    val allMovies = sc.broadcast(textFile.map(line => line.split(",")).collect())

    // The problem with how they are doing the join in the stack overflow is that they are joining on a key that they expect to match
    // However, in our case we do NOT want to join on movie title, because we want all pairs of movies (more like a cartesian product)
    // The only thing we could possible join on is "similar ratings", but that doesn't make much sense either
    // In order to join, the broadcast var you are joining with needs to be composed of key-value pairs
    // But in our case we don't really have a "key-value pair" because it's a list composed of movie-title and a bunch of ratings
    // And even if we did, the key is the movie title

    // With that said, in order to compute a similarity score, we need to, for each PAIR of movies, loop through each pair's users and check the user's ratings to see if they are identical
    // Control flow I imagine looks like:
    // 1. Start with list of lines
    // 2. Split list of lines into list of lists of values
    // 3. Take a cartesian product (or something) with the broadcast var that combines the list of lists into a list of lists of pairs of lists
    // 4. Within each pair, drop the movie title and transpose it -- this converts the list of lists of pairs of lists (tuples) into a list of lists of x-many lists (each with a size of 2)
    // 5. Map the lists created by the transposition such that any empty values are converted to 0s (as a string)
    // 6. Filter the lists created by the transposition to keep inner lists of size 2 that have the same value for both contained elements
    // 7. For each list of x-many lists, we want to then take a sum of the # of elements remaining
    // 8. Then, we get a list of lists, each of which contains the result of the sum for the movie in question
    // --> The question at this point is: how do we figure out which movie the score belongs to?

    // This is now an array with tuples that store Array[String]s
    // Considering we populated the broadcast variable identically to this idk if how we set up the BC var was right though...
    val result = textFile.map(line => line.split(","))
                                     // Cross-product:
                                     // Takes a single list, turns it into a list of tuples of lists (one tuple for each pair of movies)
                                     .map(line => allMovies.value.map(record => Array(line, record)))
                                     // A single line now looks like: [ [["LotR","5","4"],["LotR","5","4"]], [["LotR","5","4"],["Hype","2","5"]], [["LotR","5","4"],["Lit","1","3"]]]
                                     // Let's remove duplicates, using the "deep" property to see if array are equal
                                     .map(line => line.filter(outerArray => outerArray(0).deep != outerArray(1).deep))
                                     // We now transpose each array of arrays in the line so that we get one array for each user's ratings (which will have only 2 elements)
                                     .map(line => line.map(outerArray => outerArray.transpose))
                                     // A single line now looks like: [[["LotR","Hype"], ["5", "2"], ["4", "5"]], [["LotR", "Lit"], ["5", "1"], ["4", "3"]]
                                     // Now we need to clear to clear out any arrays who have their lexicographical ordering wrong
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
                                     .map(line => line.map(tuple => s"${tuple._1(0)}, ${tuple._1(1)}, ${tuple._2}"))
                              
    // Last thing I need to do is merge the RDD of arrays into an RDD of a single array?
    // OTHERWISE IT'S WORKING!
                                        
    sc.parallelize(result.first).saveAsTextFile(args(1))
  }
}
