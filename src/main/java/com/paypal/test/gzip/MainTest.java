package com.paypal.test.gzip;

public class MainTest {

    /**
     * @param args
     */
    public static void main(String[] args) {

        int offsetBegin = 5677;
        int offsetEnd = 11281;
        byte[] bByteArr = new byte[offsetEnd - offsetBegin + 1];

        // int off = offsetBegin;
        // ;
        // int len= offsetEnd-offsetBegin;;
        // byte[] b= bByteArr;
        // System.out.println("aa="+(off | len | (off + len)));
        //
        // System.out.println("aaa="+(b.length - (off + len)));
        //
        // if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        // throw new IndexOutOfBoundsException();
        // } else if (len == 0) {
        // return ;
        // }

        String a = "be4e418774404b5db263acfbec4eb42e|BREFundingCheckpoint|2016-08-29 16:50:13|1246893748634025219|202719595079735429324960403802452441060|IDI|a6ce6af4675ef";
        String b = a.replaceFirst("\\|IDI\\|", "\\|");
        System.out.println("a=" + a + "\nb=" + b);

    }

}
