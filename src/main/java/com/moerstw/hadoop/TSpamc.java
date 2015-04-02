package com.moerstw.hadoop;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.

public class TSpamc {
  /**
   * Mapper type : 
   * input: default
   * output: key: String -> String, 
   * value: arraylist-> MapWritable<UUID, timestamp>
   *                    xxx ArrayWritable<MapWritable<UUID, ArrayWritable<timestamp>>> 
   *                    xxx TwoDArrayWritable<MapWritable<UUID, timestamp>>
   * one customer per line
   */
  public static class SpamcMap extends Mapper<LongWritable, Text, Text, MapWritable> {
    /**
     * key, value(one in one) for map output
     */
    private Text outKey = new Text();
    private MapWritable mapWritable = new MapWritable();
    private Text uuidString = new Text();
    @Override
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
      uuidString.set(UUID.randomUUID().toString().replaceAll("-", ""));
      String[] splitLine = value.toString().split(" ");
      int counter = 0;
      int numberOfTrans = Integer.parseInt(splitLine[counter++]);
      for (int i = 0; i < numberOfTrans; ++i) {
        int numberOfItems = Integer.parseInt(splitLine[counter++]);
        for (int j = 0; j < numberOfItems; ++j){
          outKey.set(splitLine[counter++]);
          mapWritable.put(uuidString, new Text(String.valueOf(i)));
          context.write(outKey, mapWritable);
        }
      }
    } // end map
    // @Override
    // setup
    // run
    // cleanup
  } // end class Mapper
  /**
   * Reducer
   */

  public static void main(String args[]) throws Exception {
    /**
     * job set up
     */
    if(args.length < 2) {
      System.out.println("Usage: [input] [output]");
      System.exit(-1);
    }
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(SpamcDemo.class);
    job.setJobName("SpamcDemo");
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(SpamcMap.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ArrayWritable.class);
    FileSystem fs = FileSystem.newInstance(conf);
    if (fs.exists(new Path(args[1]))) {
      fs.delete(new Path(args[1]), true);
    }
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
