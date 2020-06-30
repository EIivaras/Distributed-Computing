import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Goal:
// -For each pair of movies, compute the number of users who assigned the same (non-blank) rating to both movies
// -You will have a "similarity" score that will be equal to the # of users who rated a pair of movies the same value
// -The output will be of the form: Movie Title 1, Movie Title 2, Similarity Score (can be zero)
// -For each pair of movies, you will output them in increasing lexicographic order

// NOTE:
// -Follow an approach similair to map-side join
// -Throw your input dataset into the "Hadoop Distributed Cache" and share that with all your map tasks

public class Task4 {

  public static class Task4Mapper extends Mapper<Object, Text, Text, IntWritable> {
    TreeMap<String, List<String>> allMovieRatings = null; /* Map sorted lexicographically by key (movieTitle) */
    private Text moviePair = new Text();
    private IntWritable score;

    public void setup(Context context) throws IOException, InterruptedException {
      allMovieRatings = new TreeMap<>();
      
      URI[] cacheFiles = context.getCacheFiles();

      if (cacheFiles != null && cacheFiles.length > 0){
        try {
          FileSystem fs = FileSystem.get(context.getConfiguration());
          Path filePath = new Path(cacheFiles[0].toString());
          BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath))); 

          String line = "";
          while ((line = reader.readLine()) != null) {
            String[] values = line.split(",");
            List<String> ratings = new ArrayList<>();
            for (int i = 1; i < values.length; i++) {
              ratings.add(values[i]);
            }
            allMovieRatings.put(values[0], ratings);
          }
        } catch (Exception e) {
          //TODO: handle exception
          System.out.println("Failed to read cached file");
          System.exit(1);
        }
      }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      score = new IntWritable(0);
      String[] firstMovie = value.toString().split(",");
      
      // Iterate through second movies in the pair (all movies appearing lexicographically after firstMovie)
      Iterator<Map.Entry<String, List<String>>> iter = allMovieRatings.tailMap(firstMovie[0]).entrySet().iterator();
      if (iter.hasNext()) iter.next();
      while (iter.hasNext()) {
        Map.Entry<String, List<String>> secondMovie = iter.next();

        // Compute similarity score between both movies
        int i = 1;
        for (String m2Rating : secondMovie.getValue()) {
          if (!m2Rating.equals(null) && !m2Rating.equals("") && i < firstMovie.length) {
            String m1Rating = firstMovie[i];
            if (m2Rating.equals(m1Rating)) {
              score.set(score.get() + 1);
            }
          }
          i++;
        }
        
        moviePair.set(firstMovie[0] + "," + secondMovie.getKey());
        context.write(moviePair, score);
        score.set(0);
      }
    }
  }  
    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task4");
    job.setJarByClass(Task4.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // Attempt to cache input file on each node (Distributed Cache) 
    // Files are automatically deleted when job is finished
    try {
      // find complete HDFS path string and convert it to a URI (required input type for addCacheFile)
      job.addCacheFile(new URI((new Path(otherArgs[0])).toString()));
    } catch (Exception e) {
      //TODO: handle exception
      System.out.println("Distributed cache file failed to add");
      System.exit(1);
    }

    job.setMapperClass(Task4Mapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setNumReduceTasks(0);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
