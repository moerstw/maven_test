package com.moerstw.hadoop;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.HColumnDescriptor;

public class SpamcHbase {

  public void createTable() throws IOException {
    Configuration hconf = HBaseConfiguration.create();
  
    // Example of setting zookeeper values for HDInsight
    //   in code instead of an hbase-site.xml file
    //
    // config.set("hbase.zookeeper.quorum",
    //            "zookeepernode0,zookeepernode1,zookeepernode2");
    //config.set("hbase.zookeeper.property.clientPort", "2181");
    //config.set("hbase.cluster.distributed", "true");

    // create an admin object using the config
    HBaseAdmin admin = new HBaseAdmin(hconf);
    // create the table
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("DHT"));
    admin.createTable(tableDescriptor);
  } 
  /**
   * end of createTable
   */


  public void insertItemFamilyToTable(String itemKey) throws IOException {
    Configuration hconf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(hconf);
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("DHT"));
    HColumnDescriptor hcDes = new HColumnDescriptor(Bytes.toBytes(itemKey));
    admin.addColumn(TableName.valueOf("DHT"), hcDes);
  } 
  /**
   * end of iinsertItemFamilyToTable
   */


  public void insertRowToTable(String customerId, String itemKey, boolean[] bitMap) throws IOException {
    Configuration hconf = HBaseConfiguration.create();
    HTable table = new HTable(hconf, "DHT");
    //   row key: customerId; family: itemKey value: bitmap
    Put aRow = new Put(Bytes.toBytes(customerId));
    aRow.add(Bytes.toBytes(itemKey), Bytes.toBytes(""), Bytes.toBytes(Arrays.toString(bitMap)));
    table.put(aRow);
    // flush commits and close the table
    table.flushCommits();
    table.close();
  }
  /**
   * end of insertRowToTable
   */


  public void SearchByEmail (String args[]) throws IOException {
    Configuration hconf = HBaseConfiguration.create();
    // Use GenericOptionsParser to get only the parameters to the class
    // and not all the parameters passed (when using WebHCat for example)
    String[] otherArgs = new GenericOptionsParser(hconf, args).getRemainingArgs();
    if(otherArgs.length != 2) {
      System.out.println("Usage:[regular expression]");
      System.exit(-1);
    }
    
    // Open the Table
    HTable table = new HTable(hconf, "people");
    
    // Define the family and qualifiers to be used
    byte[] contactFamily = Bytes.toBytes("contactinfo");
    byte[] emailQualifier = Bytes.toBytes("email");
    byte[] nameFamily = Bytes.toBytes("name");
    byte[] firstNameQualifier = Bytes.toBytes("first");
    byte[] lastNameQualifier = Bytes.toBytes("last");
    
    // Create a new regex filter
    RegexStringComparator emailFilter = new RegexStringComparator(otherArgs[1]);
    // Attach the regex filter to a filter
    // for the email column
    SingleColumnValueFilter filter = new SingleColumnValueFilter(
     contactFamily,
     emailQualifier,
     CompareOp.EQUAL,
     emailFilter
    );
    
    // Create a scan and set the filter
    Scan scan = new Scan();
    scan.setFilter(filter);
    
    // Get the results
    ResultScanner results = table.getScanner(scan);
    // Iterate over results and print values
    for(Result result : results) {
      String id = new String(result.getRow());
      byte[] firstNameObj = result.getValue(nameFamily, firstNameQualifier);
      String firstName = new String(firstNameObj);
      byte[] lastNameObj = result.getValue(nameFamily, lastNameQualifier);
      String lastName = new String(lastNameObj);
      //System.out.println(firstName + " " + lastName + " - ID: " + id);
      byte[] emailObj = result.getValue(contactFamily, emailQualifier);
      String email = new String(emailObj);
      System.out.println(firstName + " " + lastName + " - " + email + " - ID: " + id);        
    }
    results.close();
    table.close();
    
  } 
  /**
   * end of SearchByEmail 
   */


  public void DeleteTable () throws IOException {
    Configuration hconf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(hconf);
    admin.disableTable("people");
    admin.deleteTable("people");
    
  } 
  /**
   * end of DeleteTable 
   */
  
  
   /** 
    *public static void main(String args[]) throws IOException {
    *  Configuration hconf = HBaseConfiguration.create();
    *  String[] otherArgs = new GenericOptionsParser(hconf, args).getRemainingArgs();
    * 
    * if (otherArgs[0].equals("1")) {
    *   // 1. yarn jar xxx.jar 1
    *   CreateTable createTable = new CreateTable();
    *   createTable.create();
    * } 
    * else if (otherArgs[0].equals("2")) {
    *   // 2. yarn jar xxx.jar 2 nnn
    *   SearchByEmail searchByEmail = new SearchByEmail();
    *   searchByEmail.create(args);
    * }
    * else if (otherArgs[0].equals("3")) {
    *   // 3. yarn jar xxx.jar 3
    *   DeleteTable deleteTable = new DeleteTable();
    *   deleteTable.create();
    * }
    *
    *  
    * 
    * System.exit(0);
    *}
    */
}
