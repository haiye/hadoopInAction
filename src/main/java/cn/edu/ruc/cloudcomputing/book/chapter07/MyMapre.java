package cn.edu.ruc.cloudcomputing.book.chapter07;

import java.io.*;

import org.apache.hadoop.io.*;

public class MyMapre {
	public static void strings(){
		String s="\u0041\u00DF\u6771\uD801\uDC00";
		System.out.println("s="+s);
		System.out.println(s.length());
		System.out.println(s.indexOf("\u0041"));
		System.out.println(s.indexOf("\u00DF"));
		System.out.println(s.indexOf("\u6771"));
		System.out.println(s.indexOf("\uD801\uDC00"));
	}
	public static void texts(){
		Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00\uaaaa");
	      System.out.println("t="+t);

		System.out.println(t.getLength());
//		String a="\u0041";
		try {
            System.out.println("\u0041.byteLength="+"\u0041".getBytes("utf-8").length);
            System.out.println("\u00DF.byteLength="+"\u00DF".getBytes("utf-8").length);
            System.out.println("\u6771.byteLength="+"\u6771".getBytes("utf-8").length);
            System.out.println("\uD801.byteLength="+"\uD801".getBytes("utf-8").length);
            System.out.println("\uDC00.byteLength="+"\uDC00".getBytes("utf-8").length);
            System.out.println("\uD801\uDC00.byteLength="+"\uD801\uDC00".getBytes("utf-8").length);

        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
	      System.out.println(t.find("\u0041"));

		System.out.println(t.find("\u00DF"));
		System.out.println(t.find("\u6771"));
		System.out.println(t.find("\uD801\uDC00"));
	      System.out.println(t.find("\uaaaa"));

	}
	public static void main(String args[]){
		strings();
		System.out.println("----------");
		texts();	
	}
}
