package com.moerstw.hadoop;

import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.UUID;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.NavigableMap;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.io.IOException;
import org.apache.hadoop.mapreduce.ID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

// import org.apache.hadoop.

public class SpamcDemo {
  /**
   * Mapper type : 
   * input: default
   * output: key: String - String item
   * value: arraylist - MapWritable<UUID + number of transactions, ArrayWritable<timestamp>>
   *                    xxx ArrayWritable<MapWritable<UUID, ArrayWritable<timestamp>>> 
   *                    xxx TwoDArrayWritable<MapWritable<UUID, timestamp>>
   * one customer per line
   */
  public static class SpamcMap1 extends Mapper<LongWritable, Text, Text, MapWritable> {
    Log log = LogFactory.getLog(SpamcDemo.class);
    /**
     * key, value(one in one) for map output
     */
    private Text outKey = new Text();
    private MapWritable mapWritable = new MapWritable();
    @Override
    public void map(LongWritable key, Text value, Context context) 
      throws IOException, InterruptedException {
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
      /** 
       * start using hashTable to every item and combine their timestamp to a bitmap
       */
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
    /**
     * @Override
     * setup
     * run
     * cleanup
     */
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

  } // end class Mapper1

  /**
   * this TextArrayWritable class is for Mapper1  pass to Reducer1 need
   */
  public static class TextArrayWritable extends ArrayWritable {
    public TextArrayWritable() {
      super(Text.class);
    }
  }

  /**
   * Reducer
   * input key: item number, input value: id + item arrive time
   * output key: id, output value: frequence item + bitmap(int)
   * output hbase row key: id, family key: null, qualifier key: frequence item, value: bitmap(int string)
   */
  public static class SpamcReduce1 extends Reducer<Text, MapWritable, Text, MapWritable> {
    Log log = LogFactory.getLog(SpamcDemo.class);
    private Text outKey = new Text();
    private MapWritable outValue = new MapWritable();
    private int MINSUP;
    @Override 
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      /**
       * set default min_support = 100000
       */
      MINSUP = Integer.parseInt(conf.get("minSup", "100000")); 
      
    }
    @Override 
    public void reduce(Text itemKey, Iterable<MapWritable> value, Context context) 
      throws IOException, InterruptedException{
      List<MapWritable> arrayMapWritable = new ArrayList<MapWritable>();
      /**
       * insert Unique itemKey to hbase
       */
      SpamcHbase spamcTable = new SpamcHbase();
      // spamcTable.insertItemFamilyToTable(itemKey.toString()); 

      /**
       * collect all same item and calculate their support write to disk
       */
      for (MapWritable mapWritable : value) {
        arrayMapWritable.add(new MapWritable(mapWritable));
      }
      if (arrayMapWritable.size() >= MINSUP) {
        for (MapWritable mapWritable : arrayMapWritable) {
          // log.info(mapWritable.size());
          Map.Entry<Writable, Writable> pairValue = mapWritable.entrySet().iterator().next();
          /**
           * Id[0]: Id, Id[1]: bitmap maxLength
           */
          String[] customerId = ((Text)pairValue.getKey()).toString().split("\t");
          outKey.set(customerId[0]);
          String[] arrayWritableTimeStamp = ((ArrayWritable)pairValue.getValue()).toStrings();
          int bitMapLength = Integer.parseInt(customerId[1]);
          BigInteger bitMap = new BigInteger("0");
          BigInteger int2 = new BigInteger("2");
          for(int i = 0; i < arrayWritableTimeStamp.length; ++i) {
            bitMap = bitMap.or(int2.pow(Math.abs(Integer.valueOf(arrayWritableTimeStamp[i]) - bitMapLength) - 1));
          }
          
          /**
           * write back to disk; key: customerId; velue: itemKey \t bitmap
           */
          // context.write(new Text(customerId[0]), new Text(itemKey.toString() + "\t" + Arrays.toString(testBoolean)));
          outValue.put(itemKey, new Text(bitMap.toString()));
          context.write(outKey, outValue); 
          outValue.clear(); 
          /**
           * write to hbase;
           */
          spamcTable.insertRowToTable(customerId[0], itemKey.toString(), bitMap.toString());
        }
        /**
         * Ans of frequence item number
         */
        context.getCounter(MyCounters.Counter).increment(1);
      } // end if >= MINSUP
    } // end reduce
  } // end class Reducer1

  /**
   * input: id; pattern, bitmap
   * output: pattern; cid, bitMap
   */
  public static class SpamcMap2 extends Mapper<Text, MapWritable, Text, MapWritable> {
    Log log = LogFactory.getLog(SpamcDemo.class);
    private MapWritable outValue = new MapWritable();
    @Override
    public void map(Text cusID, MapWritable pattern_BitMap, Context context) 
      throws IOException, InterruptedException {
      /** 
       * search cusID in hbase table
       * @return a list of frequence itemset + bitmap that belong to this id
       */
      NavigableMap<byte[], byte[]> treeMap = new TreeMap<byte[], byte[]>();
      SpamcHbase spamcTable = new SpamcHbase();
      treeMap = spamcTable.getRowData(cusID.toString());
      /**
       * getKey: Text - no, getValue: Text - bigInt
       */
      Map.Entry<Writable, Writable> patternBitmap = pattern_BitMap.entrySet().iterator().next();
      String outKey = ((Text)patternBitmap.getKey()).toString();
      BigInteger patternInt = new BigInteger(((Text)patternBitmap.getValue()).toString());
      /**
       * extend it generator new bitMap
       */
      BigInteger andResult;
      for (Map.Entry<byte[], byte[]> entry : treeMap.entrySet()) {
        String itemKey = new String(entry.getKey());
        BigInteger itemBitmap = new BigInteger(new String(entry.getValue()));
        /**
         * I-step P & I
         * check the last pattern is greater than item in order, \
         * pattern format: item item \t item item \t item item item
         * if is:
         *   Bitwise AND directly between patternBitmap and itemBitmap 
         *   check it is greater than 0 and commit
         * fi
         */
        // if (itemKey.compareTo(outKey.substring(outKey.lastIndexOf(' ') + 1)) > 0) {
        if (itemKey.compareTo(outKey.split("\\W+")[outKey.split("\\W+").length - 1]) > 0) {
          andResult =  patternInt.and(itemBitmap);
          // log.info(outKey + " " + itemKey + " " + patternInt + " " + itemBitmap + " " + andResult);
          if (andResult.compareTo(BigInteger.ZERO) > 0) {
            // context.write(new Text(outKey + " " + itemKey), new Text(andResult.toString()));
            outValue.put(cusID, new Text(andResult.toString()));
            context.write(new Text(outKey + " " + itemKey), outValue);
            outValue.clear();
          }
        }
        /**
         * S-step P's & I 
         * need to let pattern first 1 set to 0, and after set to 1 from pattern
         * after that we can do Bitwise AND between patternBitmap and itemsBitmap
         * check it is greater than 0 and commit
         */
        andResult = itemBitmap.and(new BigDecimal(
          Math.pow(2, Math.floor(Math.log(patternInt.doubleValue()) / Math.log(2))))
            .toBigInteger().subtract(BigInteger.ONE));
        if (andResult.compareTo(BigInteger.ZERO) > 0) {
            // context.write(new Text(outKey + "\t" + itemKey), new Text(andResult.toString()));
            outValue.put(cusID, new Text(andResult.toString()));
            context.write(new Text(outKey + "\t" + itemKey), outValue);
            outValue.clear();
        }
      } // end for item from uuid
    } // end of void map2
  } // end of class Mapper2

  /**
   * Reducer2
   * input key: pattern, input value: id + bitmap
   * output key: id, output value: frequence pattern + bitmap(int)
   */
  public static class SpamcReduce2 extends Reducer<Text, MapWritable, Text, MapWritable> {
    Log log = LogFactory.getLog(SpamcDemo.class);
    private Text outKey = new Text();
    private MapWritable outValue = new MapWritable();
    private int MINSUP;
    @Override 
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      /**
       * set default min_support = 100000
       */
      MINSUP = Integer.parseInt(conf.get("minSup", "100000")); 
    }
    @Override 
    public void reduce(Text patternKey, Iterable<MapWritable> idBitMap, Context context) 
      throws IOException, InterruptedException{
      List<MapWritable> arrayListCollectAns = new ArrayList<MapWritable>();
      /**
       * collect ans and >=support write to disk
       */
      for (MapWritable mapWritable : idBitMap) {
        arrayListCollectAns.add(new MapWritable(mapWritable));
      }
      if (arrayListCollectAns.size() >= MINSUP) {
        for (MapWritable mapWritable : arrayListCollectAns) {
          Map.Entry<Writable, Writable> pairValue = mapWritable.entrySet().iterator().next();
          /**
           * write to disk; key: customerId; velue: pattern \t bitmap
           */
          // context.write((Text)pairValue.getKey(), new Text(patternKey.toString() + "\t" + ((Text)pairValue.getValue()).toString()));
          outValue.put(patternKey, (Text)pairValue.getValue());
          context.write((Text)pairValue.getKey(), outValue); 
          outValue.clear(); 
        }
        /**
         * Ans of frequence pattern number
         */
        context.getCounter(MyCounters.Counter).increment(1);
      } // end if >= MINSUP
    } // end reduce2
  } // end class Reducer2

  /**
   * for record number of frequence pattern in global in every job
   */
  public enum MyCounters {
    Counter
  }

  public static void main(String args[]) throws Exception {
    if(args.length < 4) {
      System.out.println("Usage: [input] [output] [minsup] [switch mode]");
      System.exit(-1);
    }
    boolean jobSuccessOrNot = true;
    /**
     * round n
     */
    int iter = 0;
    String originInput = args[0];
    /**
     * record all levels numb of fre pat
     */
    ArrayList<String> numOfFrePat = new ArrayList<String>();
    /**
     * switch for test
     * 1: only phase1
     * 2: only phase2
     * 3: full 
     */
    int swch = Integer.parseInt(args[3]);
    /**
     * set timer start
     */
    long startTime = System.currentTimeMillis();
    /**
     * Job for phase1: find the frequence items and 
     * write to disk and hbase
     */
    if (swch == 1 || swch == 3) { 
      SpamcHbase spamcTable = new SpamcHbase();
      /**
       * delete table if exist
       */
      spamcTable.deleteTable();
      /** 
       * creating table in hbase
       */
      spamcTable.createTable();

      /**
       * job set up
       */
      Configuration conf = new Configuration();
      conf.set("minSup", args[2]);
      Job job = Job.getInstance(conf);
      job.setJarByClass(SpamcDemo.class);
      job.setJobName("SpamcDemoP1");
      FileInputFormat.setInputDirRecursive(job, true);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass(SpamcMap1.class);
      job.setReducerClass(SpamcReduce1.class);
      // job.setNumReduceTasks(0);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      // job.setMapOutputValueClass(MapWritable.class);
      // job.setOutputValueClass(Text.class);
      job.setOutputValueClass(MapWritable.class);
      FileSystem fs = FileSystem.newInstance(conf);
      if (fs.exists(new Path(args[1]))) {
        fs.delete(new Path(args[1]), true);
      }

      jobSuccessOrNot = job.waitForCompletion(true);
      /** 
       * next file reading in hdfs
       */
      args[0] = args[1];
      args[1] = originInput + iter++;
      /**
       * check if need to exit if no frequence pattern
       */
      int numberOfFrePattern = (int)job.getCounters().findCounter(MyCounters.Counter).getValue();
      numOfFrePat.add("frequence pattern length" + iter + " : " + numberOfFrePattern);
      if (numberOfFrePattern == 0)
        System.exit(jobSuccessOrNot ? 0 : 1);
    } // end of phase1 job 

    /**
     * Job for phase 2: extends pattern 2~n iteratively
     */
    while (swch == 2 || swch == 3 && jobSuccessOrNot) { 
      Configuration conf = new Configuration();
      conf.set("minSup", args[2]);
      Job job = Job.getInstance(conf);
      job.setJarByClass(SpamcDemo.class);
      job.setJobName("SpamcDemoP2_" + iter);
      FileInputFormat.setInputDirRecursive(job, true);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass(SpamcMap2.class);
      job.setReducerClass(SpamcReduce2.class);
      // job.setNumReduceTasks(0);
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      // job.setMapOutputValueClass(MapWritable.class);
      // job.setOutputValueClass(Text.class);
      job.setOutputValueClass(MapWritable.class);
      FileSystem fs = FileSystem.newInstance(conf);
      if (fs.exists(new Path(args[1]) )) {
        fs.delete(new Path(args[1]), true);
      }
      jobSuccessOrNot = job.waitForCompletion(true);
      /** 
       * next file reading in hdfs
       */
      args[0] = args[1];
      args[1] = originInput + iter++;
      /**
       * check if need to exit if no frequence pattern
       */
      int numberOfFrePattern = (int)job.getCounters().findCounter(MyCounters.Counter).getValue();
      numOfFrePat.add("frequence pattern length" + iter + " : " + numberOfFrePattern);
      if (numberOfFrePattern == 0) {
        numOfFrePat.forEach(b -> {System.out.println(b);});
        break;
      }
    } // end of phase2 job
    System.out.println("total time: " + ((System.currentTimeMillis() - startTime) / 1000) + " sec");
    System.exit(jobSuccessOrNot ? 0 : 1);
  } // end of main
}
