package com.moerstw.hadoop;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
public class FileReadingTest {
  /**
   *@param no
   *@return no
   */
  public static void main(String args[]) 
    throws IOException {
    InputStream in = null;
    BufferedReader inp = null;
    /**
     * taskTable save all result to reduce side
     */
    Map<String, List<Map.Entry<UUID, Integer>>> taskTable = new HashMap<String, List<Map.Entry<UUID, Integer>>>();
    /** 
     * map side 
     */
    try {
      /**
       * Readfile from jar source package to stream & 
       * into buffer to read charactors
       */
      in = Class.forName("com.moerstw.hadoop.FileReadingTest").getClassLoader().getResourceAsStream("D10C10T8N26ascii.data");
      inp = new BufferedReader(new InputStreamReader(in));
      String readLine;
      // byte -> short -> int -> long -> float -> double
      /** 
       * Read line(customer) into hashmap for split to key value pair
       * key: item, value: arraylist of pair(id, timestamp) 
       */
      while ((readLine = inp.readLine()) != null){
        String[] splitLine = readLine.split(" ");
        // System.out.printf("%s\n\n", c);
        int counter = 0;
        int numberOfTrans = Integer.parseInt(splitLine[counter++]);
        // List<List<String>> list = new ArrayList<List<String>>(numberOfTrans);
        UUID cusID = UUID.randomUUID();
        // Map.Entry<UUID, Integer> entry = new SimpleEntry<UUID, Integer>(cusID, 0); 
        for (int i = 0; i < numberOfTrans; ++i) {
          int numberOfItems = Integer.parseInt(splitLine[counter++]);
          // list.add(new ArrayList<Integer>(numberOfItems));
          // entry.setValue(i);
          for (int j = 0; j < numberOfItems; ++j){
            String itemKey = splitLine[counter++];
            if (taskTable.containsKey(itemKey)) {
              taskTable.get(itemKey).add(new SimpleEntry<UUID, Integer>(cusID, i));
            } else {
              taskTable.put(itemKey, new ArrayList<Map.Entry<UUID, Integer>>());
              taskTable.get(itemKey).add(new SimpleEntry<UUID, Integer>(cusID, i));
            }
            // list.get(i).add(Integer.parseInt(splitLine[counter++]));
            // list.get(i).add(splitLine[counter++]);
          }
        }
        
        // for (List<String> tempList : list) {
        // for (String tempString : tempList) {
        //   System.out.printf(tempInt + " ");
        // }
        // System.out.printf("\n");
        // }
        // System.out.printf("\n");
      }
      Iterator iter = taskTable.entrySet().iterator();
      /**
       * Wrint result to Reduce side in hadoop
       */
      while (iter.hasNext()) {
        Map.Entry entryTemp = (Map.Entry) iter.next();
        System.out.println(entryTemp.getKey() + ": ");
        for( Map.Entry<UUID, Integer> entryTemp2 : (ArrayList<Map.Entry<UUID, Integer>>)entryTemp.getValue()) {
          System.out.printf("%s %d\n", entryTemp2.getKey().toString(), entryTemp2.getValue());
        }
        System.out.printf("\n");
      }
    } catch (Exception e) {
        System.out.println(e);
    } finally {
      /**
       * InputFile close
       */
      if(inp != null) {
        inp.close();
      }
    }

    /**
     * reduce side
     */



  } // end main

}
