package cn.edu.ruc.cloudcomputing.book.chapter12;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/* Java多线程读取本地磁盘上的文件，以HTable.put(put)的方式完成数据写入*/

public class LoadDataInHbaseByJava {

    public static long startTime;
    
    public static long rowkey = 0; // 起始rowkey

    public static final int lineCount = 100000; // 每次提交时录入的行数

    public static String tableName = "usercontact_kang"; // 录入目的表名

    public static int countLie = 8; // 表的列数

    public static void main(String[] args) throws IOException {

        startTime = System.currentTimeMillis() / 1000;
        System.out.println("start time = " + startTime);

        Thread t1 = new Thread() {
            @Override
            public void run() {

                try {
                    insert_one("/run/jar/123");
                    // loadByLieWithVector("/run/jar/123");
                    // loadByLieWithArrayList("/run/jar/123");
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        };

        t1.start();
    }

    public static void insert_one(String path) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        HTable table = new HTable(conf, tableName);

        File f = new File(path);

        ArrayList<Put> list = new ArrayList<Put>();

        BufferedReader br = new BufferedReader(new FileReader(f));

        String tmp = br.readLine();

        int count = 0;

        while (tmp != null) {

            if (list.size() > 10000) {

                table.put(list);

                table.flushCommits();

                list.clear();

            } else {

                String arr_value[] = tmp.toString().split("/t", 10);

                String first[] = arr_value[0].split("~", 5);

                String second[] = arr_value[1].split("~", 5);

                String rowname = getIncreasRowKey();

                String firstaccount = first[0];

                String firstprotocolid = first[1];

                String firstdomain = first[2];

                String inserttime = System.currentTimeMillis() / 1000 + "";

                String secondaccount = second[0];

                String secondprotocolid = second[1];

                String seconddomain = second[2];

                String timescount = Integer.valueOf(arr_value[2]).toString();

                Put p = new Put(rowname.getBytes());

                p.add(("ucvalue").getBytes(), "FIRSTACCOUNT".getBytes(),

                firstaccount.getBytes());

                p.add(("ucvalue").getBytes(), "FIRSTDOMAIN".getBytes(),

                firstdomain.getBytes());

                p.add(("ucvalue").getBytes(), "FIRSTPROTOCOLID".getBytes(),

                firstprotocolid.getBytes());

                p.add(("ucvalue").getBytes(), "INSERTTIME".getBytes(),

                inserttime.getBytes());

                p.add(("ucvalue").getBytes(), "SECONDACCOUNT".getBytes(),

                secondaccount.getBytes());

                p.add(("ucvalue").getBytes(), "SECONDDOMAIN".getBytes(),

                seconddomain.getBytes());

                p.add(("ucvalue").getBytes(), "SECONDPROTOCOLID".getBytes(),

                secondprotocolid.getBytes());

                p.add(("ucvalue").getBytes(), "TIMESCOUNT".getBytes(),

                timescount.getBytes());

                list.add(p);

            }

            tmp = br.readLine();

            count++;

        }

        if (list.size() > 0) {

            table.put(list);

            table.flushCommits();

        }

        table.close();

        System.out.println("total = " + count);

        long endTime = System.currentTimeMillis() / 1000;

        long costTime = endTime - startTime;

        System.out.println("end time = " + endTime);

        System.out.println(path + ": cost time = " + costTime);

    }

    public static String getIncreasRowKey() {
        //To Do
        return null;
    }
}