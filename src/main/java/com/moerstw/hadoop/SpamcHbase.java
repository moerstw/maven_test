package com.moerstw.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.NavigableMap;
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.HColumnDescriptor;

public class SpamcHbase {

  public void createTable() throws IOException {
    System.out.println("6");
    Configuration hconf = HBaseConfiguration.create();
  
    // Example of setting zookeeper values for HDInsight
    //   in code instead of an hbase-site.xml file
    //
    // config.set("hbase.zookeeper.quorum",
    //            "zookeepernode0,zookeepernode1,zookeepernode2");
    //config.set("hbase.zookeeper.property.clientPort", "2181");
    //config.set("hbase.cluster.distributed", "true");

    // create an admin object using the config
    System.out.println("7");
    HBaseAdmin admin = new HBaseAdmin(hconf);
    System.out.println("8");
    // create the table
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("DHT"));
    tableDescriptor.addFamily(new HColumnDescriptor("construct"));
    System.out.println("9");
    admin.createTable(tableDescriptor);
    System.out.println("0");
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


  public void insertRowToTable(String customerID, String itemKey, String bitMap) throws IOException {
    Configuration hconf = HBaseConfiguration.create();
    HTable table = new HTable(hconf, "DHT");
    //   row key: customerID; family: itemKey value: bitmap
    Put aRow = new Put(Bytes.toBytes(customerID));
    aRow.add(Bytes.toBytes("construct"), Bytes.toBytes(itemKey), Bytes.toBytes(bitMap));
    table.put(aRow);
    // flush commits and close the table
    table.flushCommits();
    table.close();
  }
  /**
   * end of insertRowToTable
   */


  public NavigableMap<byte[], byte[]> getRowData(String customerID) throws IOException {
    Configuration hconf = HBaseConfiguration.create();
    // Open the Table
    HTable table = new HTable(hconf, "DHT");
    // Create a get and set the filter
    Get getR = new Get(Bytes.toBytes(customerID));
    // Get the results
    Result results = table.get(getR);
    table.close();
    return results.getFamilyMap(Bytes.toBytes("construct"));
  } 
  /**
   * end of getRowData
   */


  public void deleteTable () throws IOException {
    Configuration hconf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(hconf);
    /**
     * check if exist
     */
    if (admin.tableExists("DHT")) {
      admin.disableTable("DHT");
      admin.deleteTable("DHT");
    }
    
  } 
  /**
   * end of DeleteTable 
   */
  
}
