package org.moerstw.hadoop;
//import org.joda.time.LocalTime;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

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

  public static void main(String args[]) {
    System.out.println("madada");
  }  
}

 /**
  * Reference
  * http://www.aichengxu.com/view/39755
  * http://blog.puneethabm.in/wordcount-mapreduce-program-using-hadoop-new-api-2/
  */