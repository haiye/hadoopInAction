package com.paypal.test.gzip;

import java.util.Arrays;

public class Main {

    private static enum Index {
        UUID, CHECKPOINT, TIME, ACCOUNT_NUMBER, ACTIVITY_ID, RESOURCE, CORR_ID
    }

    public static void main(String[] args) {
        byte[] COLUMN_FAMILY = "abcdLO world".getBytes();
        System.out.println("aa=" + Arrays.toString(COLUMN_FAMILY));

        System.out.println("Index.UUID=" + Index.UUID);
        System.out.println("Index.CHECKPOINT=" + Index.CHECKPOINT);
        System.out.println("Index.TIME=" + Index.TIME);
        System.out.println("Index.ACCOUNT_NUMBER=" + Index.ACCOUNT_NUMBER);
        System.out.println("Index.ACTIVITY_ID=" + Index.ACTIVITY_ID);
        System.out.println("Index.RESOURCE=" + Index.RESOURCE);
        System.out.println("Index.CORR_ID=" + Index.CORR_ID);

        String a = "be4e4(uuid)|BREFundingCheckpoint|2016-08-29 16:50:13|1246893748634025219|20271959507(activity_id)|IDI|a6ce6af4675ef";
        String[] arrayStr = a.split("\\|");

        for (int index = 0; index < arrayStr.length; index++)
            System.out.println("index=" + index + " value=" + arrayStr[index] + " length=" + arrayStr.length);

    }

}
