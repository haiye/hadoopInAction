package cn.edu.ruc.cloudcomputing.book.chapter11;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
    /**
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        // ע��JDBC��
        try {
            Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.exit(1);
        }

        // ��������
        Connection con = DriverManager.getConnection("jdbc:hive://master:10000/default", "", "");

        // statement����ִ��SQL���
        Statement stmt = con.createStatement();

        // ����ΪHive�������
        String tableName = "u1_data";
        stmt.executeQuery("drop table " + tableName);
        ResultSet res = stmt.executeQuery("create table " + tableName + " (userid int, " + "movieid int,"
                + "rating int," + "city string," + "viewTime string)" + "row format delimited "
                + "fields terminated by '\t' " + "stored as textfile"); // ������
        // show tables���
        String sql = "show tables";
        System.out.println("Running: " + sql + ":");
        res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // describe table���
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        // load data���
        String filepath = "/home/hadoop/Downloads/u.data.new";
        sql = "load data local inpath '" + filepath + "' overwrite into table " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);

        // select query: ѡȡǰ5����¼
        sql = "select * from " + tableName + " limit 5";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(3) + "\t" + res.getString(4)));
        }

        // regular hive query
        sql = "select count(*) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}
