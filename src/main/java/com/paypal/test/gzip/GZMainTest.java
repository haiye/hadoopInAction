package com.paypal.test.gzip;

import java.util.ArrayList;
import java.util.Arrays;

public class GZMainTest {

    /**
     * @param args
     */
    public static void main(String[] args) {

        System.out.println("int.max=" + Integer.MAX_VALUE);
        String fileName = null;

        System.out.println("args.length=" + args.length + " args=" + Arrays.toString(args));
        Integer[] offsetBeginArray = null;
        Integer[] offsetEndArray = null;

        boolean debug = false;
        if (args.length < 2) {
            System.out.println("illegal input args");
            return;
        } else if (args.length % 2 != 0) {
            System.out
                    .println("args number should be even, like: filename beginIndex endIndex debginIndex endIndex true/false");
        } else {
            fileName = args[0];
            debug = Boolean.parseBoolean(args[args.length - 1]);

            ArrayList<Integer> offsetBegin = new ArrayList<Integer>();
            ArrayList<Integer> offsetEnd = new ArrayList<Integer>();

            for (int count = 1; count < args.length - 1;) {
                offsetBegin.add(Integer.parseInt(args[count]));
                offsetEnd.add(Integer.parseInt(args[count + 1]));
                count = count + 2;
            }

            offsetBeginArray = offsetBegin.toArray(new Integer[0]);
            offsetEndArray = offsetEnd.toArray(new Integer[0]);
        }

        System.out.println("fileName=" + fileName + " offsetBeginArray=" + Arrays.toString(offsetBeginArray)
                + " offsetEndArray=" + Arrays.toString(offsetEndArray) + " debug=" + debug);

        // GZReader gzReaqder= new GZReader(fileName, offsetBeginArray,
        // offsetEndArray, debug);
        //
        // gzReaqder.readLineByBufferedReaderLineSkipTest();
        // gzReaqder.readLineByInputStreamReadTest();
        // gzReaqder.readLineByBufferedReaderReadTest();

    }

}
