package org.moerstw.hadoop;
import java.io.IOException;

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

public class HbaseTest {
  public static class CreateTable {
    public void create() throws IOException{
      
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
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("people"));
      // with two column families
      tableDescriptor.addFamily(new HColumnDescriptor("name"));
      tableDescriptor.addFamily(new HColumnDescriptor("contactinfo"));
      admin.createTable(tableDescriptor);
      // define some people
      String[][] people = {
          { "1", "Marcel", "Haddad", "marcel@fabrikam.com"},
          { "2", "Franklin", "Holtz", "franklin@contoso.com" },
          { "3", "Dwayne", "McKee", "dwayne@fabrikam.com" },
          { "4", "Rae", "Schroeder", "rae@contoso.com" },
          { "5", "Rosalie", "burton", "rosalie@fabrikam.com"},
          { "6", "Gabriela", "Ingram", "gabriela@contoso.com"} };
  
      HTable table = new HTable(hconf, "people");
  
      // Add each person to the table
      //   Use the `name` column family for the name
      //   Use the `contactinfo` column family for the email
      for (int i = 0; i< people.length; i++) {
        Put person = new Put(Bytes.toBytes(people[i][0]));
        person.add(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(people[i][1]));
        person.add(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(people[i][2]));
        person.add(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes(people[i][3]));
        table.put(person);
      }
      // flush commits and close the table
      table.flushCommits();
      table.close();
    
    }
  }
  public static class SearchByEmail {
    public void create(String args[]) throws IOException {
      Configuration hconf = HBaseConfiguration.create();
      // Use GenericOptionsParser to get only the parameters to the class
      // and not all the parameters passed (when using WebHCat for example)
      String[] otherArgs = new GenericOptionsParser(hconf, args).getRemainingArgs();
      if(otherArgs.length != 1) {
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
      RegexStringComparator emailFilter = new RegexStringComparator(otherArgs[0]);
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
  }
  public static class DeleteTable {
    public void create(String args[]) throws IOException {
      Configuration hconf = HBaseConfiguration.create();
      HBaseAdmin admin = new HBaseAdmin(hconf);
      admin.disableTable("people");
      admin.deleteTable("people");
      
    }
  }
  
  
  
  public static void main(String args[]) throws IOException {
    /* 1
     * CreateTable createTable = new CreateTable();
     * createTable.create();
     */ 
    /* 2
     * SearchByEmail searchByEmail = new SearchByEmail();
     * searchByEmail.create(args);
     */ 
    /* 3
     * DeleteTable deleteTable = new DeleteTable();
     * deleteTable.create();
     */
    
    System.exit(0);
  }
}


/*
 * reference
 * http://azure.microsoft.com/zh-tw/documentation/articles/hdinsight-hbase-build-java-maven/
 */