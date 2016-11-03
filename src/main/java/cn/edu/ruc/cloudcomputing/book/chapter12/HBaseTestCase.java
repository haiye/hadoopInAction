package cn.edu.ruc.cloudcomputing.book.chapter12;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTestCase {

    // create HBase Table
    public static void creat(String tableName, String columnFamily) throws Exception {
        // 1. create
        // hbaseConfiguration(org.apache.hadoop.hbase.HBaseConfiguration)
        Configuration hbaseConfiguration = HBaseConfiguration.create();

        // 2. create hbaseAdmin(org.apache.hadoop.hbase.client.HBaseAdmin)
        HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConfiguration);

        if (hbaseAdmin.tableExists(tableName)) {

            System.out.println("table Exists!");

            System.exit(0);
        } else {
            // 3. create hbaseTableName(org.apache.hadoop.hbase.TableName)
            TableName hbaseTableName = TableName.valueOf(tableName);
            // 4. create
            // hbaseTableDescriptor(org.apache.hadoop.hbase.HTableDescriptor)
            HTableDescriptor hbaseTableDescriptor = new HTableDescriptor(hbaseTableName);
            // 5. create
            // hbaseColumnDescriptor(org.apache.hadoop.hbase.HColumnDescriptor)
            HColumnDescriptor hbaseColumnDescriptor = new HColumnDescriptor(columnFamily);
            // 6. addColumnFamily
            hbaseTableDescriptor.addFamily(hbaseColumnDescriptor);
            // 7. create hbaseTable
            hbaseAdmin.createTable(hbaseTableDescriptor);

            System.out.println("create table success!");
        }

        hbaseAdmin.close();
    }

    // add one row data in hbaseTable
    public static void put(String tablename, String row, String columnFamily, String column, String data)
            throws Exception {
        // 1. create
        // hbaseConfiguration(org.apache.hadoop.hbase.HBaseConfiguration)
        Configuration hbaseConfiguration = HBaseConfiguration.create();

        // 2. get hbaseTable(org.apache.hadoop.hbase.client.HTable)
        HTable hbaseTable = new HTable(hbaseConfiguration, tablename);

        // 3. create putAction
        Put putAction = new Put(Bytes.toBytes(row));
        putAction.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));

        // 4. take put action
        hbaseTable.put(putAction);
        System.out.println("put '" + row + "','" + columnFamily + ":" + column + "','" + data + "'");

        // 5. close hbaseTable
        hbaseTable.close();
    }

    // get one row data in hbaseTable
    public static void get(String tablename, String row) throws IOException {
        // 1. create
        // hbaseConfiguration(org.apache.hadoop.hbase.HBaseConfiguration)
        Configuration hbaseConfiguration = HBaseConfiguration.create();

        // 2. get hbaseTable(org.apache.hadoop.hbase.client.HTable)
        HTable hbaseTable = new HTable(hbaseConfiguration, tablename);

        // 3. create getAction(org.apache.hadoop.hbase.client.Get)
        Get getAction = new Get(Bytes.toBytes(row));

        // 4. take get action and get
        // result(org.apache.hadoop.hbase.client.Result)
        Result result = hbaseTable.get(getAction);
        System.out.println("Get: " + result);

        // 5. close hbaseTable
        hbaseTable.close();
    }

    // get all rows data in hbaseTable(Scan)
    public static void scan(String tablename) throws Exception {
        // 1.
        // createhbaseConfiguration(org.apache.hadoop.hbase.HBaseConfiguration)
        Configuration hbaseConfiguration = HBaseConfiguration.create();

        // 2. get hbaseTable(org.apache.hadoop.hbase.client.HTable)
        HTable hbaseTable = new HTable(hbaseConfiguration, tablename);

        // 3. create scanAction(org.apache.hadoop.hbase.client.ResultScanner)
        Scan scan = new Scan();

        // 4. take scan action and get
        // ResultScanner(org.apache.hadoop.hbase.client.ResultScanner)
        ResultScanner resultScanner = hbaseTable.getScanner(scan);
        for (Result result : resultScanner) {
            System.out.println("Scan: " + result);
        }

        // 5. close hbaseTable
        hbaseTable.close();
    }

    // delete hbaseTable
    public static boolean delete(String tablename) throws IOException {
        // 1. create
        // hbaseConfiguration(org.apache.hadoop.hbase.HBaseConfiguration)
        Configuration hbaseConfiguration = HBaseConfiguration.create();

        // 2. create hbaseAdmin(org.apache.hadoop.hbase.client.HBaseAdmin)
        HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConfiguration);

        if (hbaseAdmin.tableExists(tablename)) {
            try {
                // 3. disable hbaseTable
                hbaseAdmin.disableTable(tablename);

                // 4. drop hbaseTable
                hbaseAdmin.deleteTable(tablename);
            } catch (Exception ex) {
                ex.printStackTrace();
                hbaseAdmin.close();
                return false;
            }

        }

        // 5. close hbaseTable
        hbaseAdmin.close();

        return true;
    }

    public static void main(String[] agrs) {
        String tablename = "hbase_tb";
        String columnFamily = "cf";

        try {
            System.out.println("111");
            HBaseTestCase.creat(tablename, columnFamily);
            System.out.println("222");

            HBaseTestCase.put(tablename, "row1", columnFamily, "cl1", "data");
            System.out.println("333");

            HBaseTestCase.get(tablename, "row1");
            System.out.println("444");

            HBaseTestCase.scan(tablename);
            System.out.println("555");

            if (true == HBaseTestCase.delete(tablename))
                System.out.println("Delete table:" + tablename + "success!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
