package com.moerstw.hadoop;

import java.util.Set;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.ID;

public class SpamcDemo {
    public static class SpmcMap entend Mapper<Object, Text, Text, Text> {
        public void map (Object key, Text value) throws IOException, InteruptedException {

            for(String sTemp: value) {
                ID id = new ID();
                sTemp.
            }
        }


        /*
         * original ascii dataset
         */
        public void StringSplite(String sTemp[]) {
            int k = 1; // start value
            HashSet hs = new HashSet();
            ArrayList al = new ArrayList();
            for (int i = 0; i < Integer.parseInt(sTemp[0]); ++i) {
                for (int j = Integer.parseInt(sTemp[k]); j > 0; --j) {
                    ++k;
                    if (hs.contains(sTemp[k])) {
                        //write to bitmap
                           
                    } else {
                        hs.add(sTemp[k]);
                        //creat a bitmap
                        //add a ArrayList<bool> to ArrayList<ArrayList<bool>>
                    }
                }
            }
        }
    }
  public static void main(String args[]) {
    
    System.exit(0);
     
  }

}
