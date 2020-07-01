import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

// Goal:
// -Compute the total number of (non-blank) ratings
// -Output the total as a SINGLE NUMBER on a SINGLE LINE

// NOTE:
// The shuffle process between the mapper and reducer combines all values with matching keys into a list that can be iterated over by the reducer

public class Task2 {

  public static class Task2Mapper extends Mapper<Object, Text, NullWritable, LongWritable> {
    private LongWritable result = new LongWritable(0);
    private static final NullWritable nullKey = NullWritable.get();
  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");
      result.set(0);
      for (int i = 0; i < tokens.length; i++) {
        if (!tokens[i].equals(null) && !tokens[i].equals("")) {
          try {
            Integer.parseInt(tokens[i]);
            result.set(result.get() + 1);
          } catch (NumberFormatException e) {
            continue;
          }
        }
      }
      context.write(nullKey, result);
    }
  } 

  public static class Task2Reducer extends Reducer<NullWritable,LongWritable,NullWritable,LongWritable> {
    private LongWritable result = new LongWritable(0);

    public void reduce(NullWritable nullKey, Iterable<LongWritable> numRatings, Context context) throws IOException, InterruptedException {
      for (LongWritable rating : numRatings) {
        result.set(result.get() + rating.get());
      }
      context.write(nullKey, result);
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task2");
    job.setJarByClass(Task2.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(LongWritable.class);

    job.setMapperClass(Task2Mapper.class);
    job.setCombinerClass(Task2Reducer.class);
    job.setReducerClass(Task2Reducer.class);
    job.setNumReduceTasks(1);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
