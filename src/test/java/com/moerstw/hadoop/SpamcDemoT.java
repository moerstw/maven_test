package com.moerstw.hadoop;

import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.UUID;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.io.IOException;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
// import org.apache.hadoop.

public class SpamcDemoT {
  /**
   * Mapper type : 
   * input: default
   * output: key: String -> String item
   * value: arraylist-> MapWritable<UUID + number of transactions, ArrayWritable<timestamp>>
   *                    xxx ArrayWritable<MapWritable<UUID, ArrayWritable<timestamp>>> 
   *                    xxx TwoDArrayWritable<MapWritable<UUID, timestamp>>
   * one customer per line
   */
  public static class SpamcMap extends Mapper<LongWritable, Text, Text, MapWritable> {
    Log log = LogFactory.getLog(SpamcDemo.class);
    /**
     * key, value(one in one) for map output
     */
    private Text outKey = new Text();
    private MapWritable mapWritable = new MapWritable();
    @Override
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
      Map<String, List<String>> taskTable = new HashMap<String, List<String>>();
      // int numberOfTrans = StringSplite(taskTable, value.toString().split(" "));
      /**
       * Each key pair write to reduce task
       */
      Text uuidString = new Text();
      uuidString.set(UUID.randomUUID().toString().replaceAll("-", "") + "\t" + StringSplite(taskTable, value.toString().split(" ")));
      // ArrayWritable arrayWritable = new ArrayWritable(Text.class); 
      ArrayWritable arrayWritable = new TextArrayWritable(); 
      Iterator iter = taskTable.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry entryTemp = (Map.Entry) iter.next();
        outKey.set((String) entryTemp.getKey());
        Text[] itemTimestampForValue = new Text[((ArrayList<String>)entryTemp.getValue()).size()];
        int i = 0;
        // ArrayList<String> list = (ArrayList<String>) entryTemp.getValue();
        for (String sTemp : (ArrayList<String>) entryTemp.getValue()) {
          itemTimestampForValue[i++] = new Text(sTemp);
        }
        arrayWritable.set(itemTimestampForValue);
        mapWritable.put(uuidString, arrayWritable);
        log.info(mapWritable.size());
        context.write(outKey, mapWritable);
        mapWritable.clear();
      }
    } // end map
    // @Override
    // setup
    // run
    // cleanup
    /**
     *
     * @param taskTable hashtable for storing one customer's list of pair(item, timestamp) 
     * @param splitLine the origin text line split(" ") into
     * @return numberOfTrans number of transaction for recording new array number
     */
    public int StringSplite(Map<String, List<String>> taskTable, String[] splitLine) {
      int counter = 0;
      int numberOfTrans = Integer.parseInt(splitLine[counter++]);
      for (int i = 0; i < numberOfTrans; ++i) {
        int numberOfItems = Integer.parseInt(splitLine[counter++]);
        for (int j = 0; j < numberOfItems; ++j){
          String itemKey = splitLine[counter++];
          if (taskTable.containsKey(itemKey)) {
            taskTable.get(itemKey).add(String.valueOf(i));
          } else {
            taskTable.put(itemKey, new ArrayList<String>());
            taskTable.get(itemKey).add(String.valueOf(i));
          }
        }
      }
      return numberOfTrans;
    } // end void StringSplit

  } // end class Mapper
  public static class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
      super(Text.class);
    }
  }
  /**
   * Reducer
   */
  public static class SpamcReduce extends Reducer<Text, MapWritable, Text, MapWritable> {
    private Text outKey = new Text();
    private MapWritable outValue = new MapWritable();
    private int MINSUP;
    @Override 
    public void setup (Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      /**
       * set default min_support = 100000
       */
      MINSUP = Integer.parseInt(conf.get("minSup", "100000")); 
      
    }
    @Override 
    public void reduce (Text itemKey, Iterable<MapWritable> value, Context context) throws IOException, InterruptedException{
      Log log = LogFactory.getLog(SpamcDemo.class);
      List<MapWritable> arrayMapWritable = new ArrayList<MapWritable>();
      /**
       * insert Unique itemKey to hbase
       */
      SpamcHbase spamcTable = new SpamcHbase();
      spamcTable.insertItemFamilyToTable(itemKey.toString()); 


      for (MapWritable mapWritable : value) {
        arrayMapWritable.add(new MapWritable(mapWritable));
      }
      if (arrayMapWritable.size() >= MINSUP) {
        // log.info("test3");
        for (MapWritable mapWritable : arrayMapWritable) {
          log.info(mapWritable.size());
          Map.Entry<Writable, Writable> pairValue = mapWritable.entrySet().iterator().next();
          
          String[] customerId = ((Text)pairValue.getKey()).toString().split("\t");
          outKey.set(customerId[0]);
          String[] arrayWritableTimeStamp = ((ArrayWritable)pairValue.getValue()).toStrings();
          ArrayWritable arrayWritableBooleanBitmap = new ArrayWritable(BooleanWritable.class);
          BooleanWritable[] booleanWritableArray = new BooleanWritable[Integer.parseInt(customerId[1])];
          boolean[] testBoolean = new boolean[Integer.parseInt(customerId[1])];
          for(int i = 0; i < Integer.parseInt(customerId[1]); ++i) {
            booleanWritableArray[i] = new BooleanWritable(false);
          }
          for(int i = 0; i < arrayWritableTimeStamp.length; ++i) {
            booleanWritableArray[Integer.parseInt(arrayWritableTimeStamp[i])].set(true);
            testBoolean[Integer.parseInt(arrayWritableTimeStamp[i])] = true;
          }
          
          /**
           * write back to disk; key: customerId; velue: itemKey \t bitmap
           */
           // context.write(new Text(customerId[0]), new Text(itemKey.toString() + "\t" + Arrays.toString(testBoolean)));
          outValue.put(itemKey, arrayWritableBooleanBitmap);
          context.write(outKey, outValue); 
          outValue.clear(); 
          /**
           * write to hbase;
           */
          // spamcTable.insertRowToTable(customerId[0], itemKey.toString(), testBoolean);
        }
      }
    }
  }
  public static class BooleanArrayWritable extends ArrayWritable {
    public BooleanArrayWritable () {
      super(BooleanWritable.class);
    }
  }

  public static void main(String args[]) throws Exception {
    /** 
     * creating table in hbase
     */
     SpamcHbase spamcTable = new SpamcHbase();
     spamcTable .createTable();

    /**
     * job set up
     */
    if(args.length < 3) {
      System.out.println("Usage: [input] [output] [minsup]");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    conf.set("minSup", args[2]);
    Job job = Job.getInstance(conf);
    job.setJarByClass(SpamcDemo.class);
    job.setJobName("SpamcDemo");
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(SpamcMap.class);
    job.setReducerClass(SpamcReduce.class);
    // job.setNumReduceTasks(0);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    // job.setMapOutputValueClass(MapWritable.class);
    // job.setOutputValueClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    FileSystem fs = FileSystem.newInstance(conf);
    if (fs.exists(new Path(args[1]))) {
      fs.delete(new Path(args[1]), true);
    }
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
