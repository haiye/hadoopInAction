package cn.edu.ruc.cloudcomputing.book.chapter07;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;

public class E02TextAndStrTest {
    public static void printString() {

        // \uD801\uDC00使用两个代码单元表示一个字符𐐀，它是unicode表中的辅助字符，故需要两个代码单元
        // 此时单独的\uD801 和 单独的\uDC00表示的字符都不能被识别
        String str = "\u0041\u00DF\u6771\uD801\uDC00";
        System.out.println("str=" + str);
        System.out.println("str.length=" + str.length());
        System.out.println("index of \u0041:" + str.indexOf("\u0041"));
        System.out.println("index of \u00DF:" + str.indexOf("\u00DF"));
        System.out.println("index of \u6771:" + str.indexOf("\u6771"));
        System.out.println("index of \uD801:" + str.indexOf("\uD801")); // 此时单独的\uD801表示的字符都不能被识别
        System.out.println("index of \uDC00:" + str.indexOf("\uDC00"));// 此时单独的\uDC00表示的字符都不能被识别
        System.out.println("index of \uD801\uDC00:" + str.indexOf("\uD801\uDC00"));
    }

    public static void printText() {
        Text text = new Text("\u0041\u00DF\u6771\uD801\uDC00");
        System.out.println("text=" + text);
        System.out.println("text。bytes.length=" + text.getLength());
        try {
            System.out.println("\u0041.byteLength=" + "\u0041".getBytes("utf-8").length);
            System.out.println("\u00DF.byteLength=" + "\u00DF".getBytes("utf-8").length);
            System.out.println("\u6771.byteLength=" + "\u6771".getBytes("utf-8").length);
            System.out.println("\uD801.byteLength=" + "\uD801".getBytes("utf-8").length);
            System.out.println("\uDC00.byteLength=" + "\uDC00".getBytes("utf-8").length);
            System.out.println("\uD801\uDC00.byteLength=" + "\uD801\uDC00".getBytes("utf-8").length);

        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("byte index of \u0041:" + text.find("\u0041"));
        System.out.println("byte index of \u00DF:" + text.find("\u00DF"));
        System.out.println("byte index of \u6771:" + text.find("\u6771"));
        System.out.println("byte index of \uD801\uDC00:" + text.find("\uD801\uDC00"));

    }

    public static void main(String args[]) {
        printString();
        System.out.println("----------");
        printText();

        String a = "\uD801\uDC00 is a special word";// \uD801\uDC00使用两个代码单元表示一个字符𐐀，它是unicode表中的辅助字符，故需要两个代码单元
        char aChar0 = a.charAt(0);// 此时单独的\uD801表示的字符都不能被识别
        char aChar1 = a.charAt(1);// 此时单独的\uDC00表示的字符都不能被识别
        char aChar2 = a.charAt(2);

        System.out.println("aChar=" + aChar0 + "; aChar1=" + aChar1 + "; aChar2=" + aChar2 + ";");// 输出：
                                                                                                  // aChar=?;
                                                                                                  // aChar1=?;
                                                                                                  // aChar2=
                                                                                                  // ;

    }
}
