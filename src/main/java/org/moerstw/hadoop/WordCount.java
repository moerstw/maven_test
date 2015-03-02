package org.moerstw.hadoop;
//import org.joda.time.LocalTime;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.util.GenericOptionsParser;
//GenericOptionsParser is a utility to parse command line arguments generic to the Hadoop framework. 
//GenericOptionsParser recognizes several standarad command line arguments, 
//enabling applications to easily specify a namenode, a jobtracker, additional configuration resources etc.


public class WordCount {
  public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one  = new IntWritable(1);
    private Text word = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
    
    @Override
    public void run(Context context) throws IOException, InterruptedException {
      setup(context);  // Override
      while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
      cleanup(context);  // Override
    }
  }
  
  public static class WordCountReduce extends Reducer<Text, Iterable<IntWritable>, Text, IntWritable> {
    
    private IntWritable totalSumSet = new IntWritable();
    
//     @Override dont know why can't work
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum =0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      totalSumSet.set(sum);
      context.write(key, totalSumSet);
    }
    
  }
  
  public static class WordCountConfiguredDriver extends Configured implements Tool {
    
    public int run(String[] args) throws Exception {
      
      if (args.length < 2) {
        System.out.println("Usage: [input] [output]");
		  System.exit(-1);
      }
      
      // use Tool <- Configurable; Configuration processed by ToolRunner
      Configuration conf = getConf(); 
      
      // Create a JobConf using the processed conf
      // Job job = new Job(conf, "wordcount"); All construct Deprecated
      // JobConf is old API
      
      // Create a new Job
      Job job = Job.getInstance(conf); //getInstance(Configuration, String)
      job.setJarByClass(WordCount.class);
      
      // Specify various job-specific parameters
      job.setJobName("wordcount");
      
      /* This line is to accept the input recursively */
		FileInputFormat.setInputDirRecursive(job, true);
      
      // job.setInputPath(new Path("in"));
      FileInputFormat.addInputPath(job, new Path(args[0])); // add; set(recover)
      FileOutputFormat.setOutputPath(job, new Path(args[1])); // only set
            
      //setPartitionerClass
      job.setMapperClass(WordCountMap.class);
      job.setCombinerClass(WordCountReduce.class);
      job.setReducerClass(WordCountReduce.class);
      
      
      //FixedLengthInputFormat, KeyValueTextInputFormat, MultiFileInputFormat, 
      //NLineInputFormat, SequenceFileInputFormat, TextInputFormat
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      
      // will set the types expected as output from both the map and reduce phases.
      job.setOutputKeyClass(Text.class);  // K2 K3  setMapOutputKeyClass K2 setInputKeyClass(JobConf) K1
		job.setOutputValueClass(IntWritable.class);  // V2 V3 setMapOutputValueClass V2 setOutputKeyClass(JobConf) K1
      
      /*
		 * Delete output filepath if already exists
		 */
		FileSystem fs = FileSystem.newInstance(getConf());

		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
      
		// true print the progress to the user & true if the job succeeded
      return job.waitForCompletion(true) ? 0 : 1;  
    }
    
  }

  public static void main(String args[]) throws Exception {
    WordCountConfiguredDriver wcConfDriver = new WordCountConfiguredDriver();
    int res = ToolRunner.run(wcConfDriver, args);
    System.exit(res);
    //System.out.println("madada");
  }
  
}

 /**
  * Reference
  * http://www.aichengxu.com/view/39755
  * http://blog.puneethabm.in/wordcount-mapreduce-program-using-hadoop-new-api-2/
  * https://hadoop.apache.org/docs/current/api/org/apache/hadoop/util/Tool.html
  * https://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/Job.html
  */