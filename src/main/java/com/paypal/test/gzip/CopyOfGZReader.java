package com.paypal.test.gzip;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class CopyOfGZReader {

    // public String getContext(String fileName, long offset, long maxLineNum)
    // throws IOException {
    // org.apache.hadoop.mapreduce.lib.input.LineRecordReader a;
    //
    // Path file = new Path(fileName);
    //
    // Configuration conf = new Configuration();
    //
    // FileSystem fs = file.getFileSystem(conf);
    //
    // FSDataInputStream fileIn = fs.open(file);
    //
    // Seekable filePosition;
    // long start = 0;
    // CompressionCodec codec = new
    // CompressionCodecFactory(conf).getCodec(file);
    // SplitLineReader in;
    // if (null != codec) {
    //
    // boolean isCompressedInput = true;
    // Decompressor decompressor = CodecPool.getDecompressor(codec);
    // if (codec instanceof SplittableCompressionCodec) {
    // final SplitCompressionInputStream cIn = ((SplittableCompressionCodec)
    // codec).createInputStream(fileIn,
    // decompressor, 0, Long.MAX_VALUE,
    // SplittableCompressionCodec.READ_MODE.BYBLOCK);
    // in = new CompressedSplitLineReader(cIn, conf, null);
    // // in.readLine("putput_file", Long.MAX_VALUE, );
    //
    // start = cIn.getAdjustedStart();
    // long end = cIn.getAdjustedEnd();
    // filePosition = cIn;
    // } else {
    // in = new SplitLineReader(codec.createInputStream(fileIn, decompressor),
    // conf, null);
    // filePosition = fileIn;
    // }
    // }else {
    // fileIn.seek(start);
    // in = new SplitLineReader(fileIn, conf, null);
    // filePosition = fileIn;
    // }
    //
    // if (start != 0) {
    // start += in.readLine(new Text(), 0, Integer.MAX_VALUE);
    // }
    // // CompressionCodec codec =
    // return null;
    // }
    //
    // public List<String> readLines(Path location, Configuration conf) throws
    // Exception {
    // FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
    // CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    // FileStatus[] items = fileSystem.listStatus(location);
    // if (items == null)
    // return new ArrayList<String>();
    // List<String> results = new ArrayList<String>();
    // for (FileStatus item : items) {
    //
    // // ignoring files like _SUCCESS
    // if (item.getPath().getName().startsWith("_")) {
    // continue;
    // }
    //
    // CompressionCodec codec = factory.getCodec(item.getPath());
    // InputStream stream = null;
    //
    // // check if we have a compression codec we need to use
    // if (codec != null) {
    // stream = codec.createInputStream(fileSystem.open(item.getPath()));
    // } else {
    // stream = fileSystem.open(item.getPath());
    // }
    //
    // StringWriter writer = new StringWriter();
    // IOUtils.copy(stream, writer, "UTF-8");
    // String raw = writer.toString();
    // String[] resulting = raw.split("\n");
    // for (String str : raw.split("\n")) {
    // results.add(str);
    // }
    // }
    // return results;
    // }

    String filePath;
    String[] offsetBeginIndexArray;
    String[] offsetEndIndexArray;
    boolean debugLog;

    public CopyOfGZReader() {

    }

    public CopyOfGZReader(String filePath, String[] offsetBeginIndexArray, String[] offsetEndIndexArray,
            boolean debugLog) {
        this.filePath = filePath;
        this.offsetBeginIndexArray = offsetBeginIndexArray;
        this.offsetEndIndexArray = offsetEndIndexArray;
        this.debugLog = debugLog;
    }

    public BufferedReader openBufferReaderStream(String filePath) {
        InputStream stream = null;
        try {
            stream = openInputStream(filePath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        InputStreamReader isr = new InputStreamReader(stream);
        BufferedReader br = new BufferedReader(isr);
        return br;
    }

    public InputStream openInputStream(String filePath) throws IOException {
        Configuration conf = new Configuration();
        Path file_path = new Path(filePath);

        // ignoring files like _SUCCESS
        if (file_path.getName().startsWith("_")) {
            return null;
        }

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(file_path);
        FileSystem fileSystem = FileSystem.get(file_path.toUri(), conf);
        InputStream stream = null;

        // check if we have a compression codec we need to use
        if (codec != null) {
            System.out.println("this is a compression file");
            stream = codec.createInputStream(fileSystem.open(file_path));
        } else {
            System.out.println("this is a normal file");
            stream = fileSystem.open(file_path);
        }
        return stream;
    }

    public void closeStream(Closeable br) {
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readLineTestByBufferedReaderLineSkipTest() {
        ArrayList<Long> timeBeginArray = new ArrayList<Long>();
        ArrayList<Long> timeEndArray = new ArrayList<Long>();
        long time_begin_total = System.currentTimeMillis();

        if (this.offsetBeginIndexArray.length != this.offsetEndIndexArray.length) {
            System.out.println("offsetBeginIndexArray.length != offsetEndIndexArray.length");
            return;
        }

        BufferedReader br = openBufferReaderStream(filePath);

        for (int index = 0; index < offsetBeginIndexArray.length; index++) {
            timeBeginArray.add(System.currentTimeMillis());
            String lineContent = readLineByBufferedReaderLineSkip(br, Integer.parseInt(offsetBeginIndexArray[index]));
            timeEndArray.add(System.currentTimeMillis());
            if (debugLog) {
                System.out.println("readLineByBufferedReaderSkipTest: filePath=" + filePath + " offset "
                        + offsetBeginIndexArray[index] + " lineContent=" + lineContent);
            }
        }

        closeStream(br);
        long time_end_total = System.currentTimeMillis();
        timeBeginArray.add(time_begin_total);
        timeEndArray.add(time_end_total);
        printCostTime(timeBeginArray, timeEndArray, offsetBeginIndexArray, offsetEndIndexArray);

    }

    public String readLineByBufferedReaderLineSkip(BufferedReader br, int offsetBegin) {
        try {
            br.skip(offsetBegin);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String content = null;
        try {
            content = br.readLine();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return content;
    }

    public void readLineByBufferedReaderReadTest() {
        ArrayList<Long> timeBeginArray = new ArrayList<Long>();
        ArrayList<Long> timeEndArray = new ArrayList<Long>();
        long time_begin_total = System.currentTimeMillis();

        if (offsetBeginIndexArray.length != offsetEndIndexArray.length) {
            System.out.println("offsetBeginIndexArray.length != offsetEndIndexArray.length");
            return;
        }
        BufferedReader br = openBufferReaderStream(filePath);

        for (int index = 0; index < offsetBeginIndexArray.length; index++) {
            timeBeginArray.add(System.currentTimeMillis());
            String lineContent = readLineByBufferedReaderRead(br, Integer.parseInt(offsetBeginIndexArray[index]),
                    Integer.parseInt(offsetEndIndexArray[index]));
            timeEndArray.add(System.currentTimeMillis());
            if (debugLog) {
                System.out.println("readLineByBufferedReaderSkipTest: filePath=" + filePath + " offset "
                        + offsetBeginIndexArray[index] + " lineContent=" + lineContent);
            }
        }

        closeStream(br);

        timeBeginArray.add(time_begin_total);
        timeEndArray.add(System.currentTimeMillis());

        printCostTime(timeBeginArray, timeEndArray, offsetBeginIndexArray, offsetEndIndexArray);
    }

    public String readLineByBufferedReaderRead(BufferedReader br, int offsetBegin, int offsetEnd) {
        char[] b = null;
        try {
            br.read(b, offsetBegin, offsetEnd);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        StringBuffer sbf = new StringBuffer();
        sbf.append(b);
        return sbf.toString();
    }

    public void readLineByInputStreamReadTest() {
        ArrayList<Long> timeBeginArray = new ArrayList<Long>();
        ArrayList<Long> timeEndArray = new ArrayList<Long>();
        long time_begin_total = System.currentTimeMillis();

        if (offsetBeginIndexArray.length != offsetEndIndexArray.length) {
            System.out.println("offsetBeginIndexArray.length != offsetEndIndexArray.length");
            return;
        }
        InputStream inputStream = null;
        try {
            inputStream = openInputStream(filePath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        for (int index = 0; index < offsetBeginIndexArray.length; index++) {
            timeBeginArray.add(System.currentTimeMillis());
            String lineContent = readLineByInputStreamRead(inputStream, Integer.parseInt(offsetBeginIndexArray[index]),
                    Integer.parseInt(offsetEndIndexArray[index]));
            timeEndArray.add(System.currentTimeMillis());
            if (debugLog) {
                System.out.println("readLineByBufferedReaderSkipTest: filePath=" + filePath + " offset "
                        + offsetBeginIndexArray[index] + " lineContent=" + lineContent);
            }
        }

        closeStream(inputStream);

        timeBeginArray.add(time_begin_total);
        timeEndArray.add(System.currentTimeMillis());

        printCostTime(timeBeginArray, timeEndArray, offsetBeginIndexArray, offsetEndIndexArray);
    }

    public String readLineByInputStreamRead(InputStream stream, int offsetBegin, int offsetEnd) {

        byte[] b = null;
        try {
            stream.read(b, offsetBegin, offsetEnd);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        StringBuffer sbf = new StringBuffer();
        sbf.append(b);
        return sbf.toString();
    }

    public void printCostTime(ArrayList<Long> timeBeginArray, ArrayList<Long> timeEndArray,
            String[] offsetBeginIndexArray, String[] offsetEndIndexArray) {
        if ((timeBeginArray.size() == timeEndArray.size())
                && (timeEndArray.size() == (offsetBeginIndexArray.length + 1))) {
            System.out.println("totalTimeCost: "
                    + (timeEndArray.get(timeEndArray.size() - 1) - timeBeginArray.get(timeBeginArray.size() - 1)));

            for (int index = 0; index < offsetBeginIndexArray.length; index++) {
                System.out.println("perLine: offsetBegin=" + offsetBeginIndexArray[index] + " timeCost="
                        + (timeEndArray.get(index) - timeBeginArray.get(index)));
            }
        } else {
            System.out
                    .println("timeEndArray.size != timeBeginArray.size or timeEndArray.size()!=offsetBeginIndexArray.length+1");
        }
    }

    public void readLineByBufferedReaderSeek(String filePath, String[] offsetBeginIndexArray, boolean debug)
            throws IOException {

        Configuration conf = new Configuration();
        Path file_path = new Path(filePath);

        // ignoring files like _SUCCESS
        if (file_path.getName().startsWith("_")) {
            return;
        }

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(file_path);
        FileSystem fileSystem = FileSystem.get(file_path.toUri(), conf);
        InputStream stream = null;

        // // check if we have a compression codec we need to use
        // if (codec != null) {
        // System.out.println("this is a compression file");
        // stream = codec.createInputStream(fileSystem.open(file_path));
        // } else {
        // System.out.println("this is a normal file");
        // stream = fileSystem.open(file_path);
        // }

        FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
        BufferedReader br = null;
        for (int i = 0; i < offsetBeginIndexArray.length; i++) {
            long time_begin = System.currentTimeMillis();

            int len = Integer.parseInt(offsetBeginIndexArray[i]);
            System.out.println("offset: " + len);

            // try{
            // inputStream.seek(len);
            // BufferedReader reader = new BufferedReader(new
            // InputStreamReader(inputStream));
            // String rawPayload = reader.readLine();
            // System.out.println("rawPayload="+rawPayload);
            // }catch(Exception e){
            // System.out.println("error happened1: "+e);
            // }
            codec.createInputStream(inputStream);
            try {
                inputStream = new FSDataInputStream(null);
                inputStream.seek(len);
                br = new BufferedReader(new InputStreamReader(inputStream));
                String content = br.readLine();
                System.out.println("content=" + content);
            } catch (Exception e) {
                System.out.println("error happened2: " + e);
            }
            br.close();

            long time_end = System.currentTimeMillis();

            if (debug) {
                System.out.println("readLineByBufferedReaderSkipTest: filePath=" + filePath + " offset "
                        + offsetBeginIndexArray[i] + " timecost=" + (time_end - time_begin));
            }
            System.out.println("readLineByBufferedReaderSkipTest: filePath=" + filePath + " offset "
                    + offsetBeginIndexArray[i] + " timecost=" + (time_end - time_begin));
        }
        inputStream.close();
    }

    /**
     * @param filePath
     *            The start offset into the compressed stream. May be changed
     * @param start
     *            The start offset into the compressed stream. May be changed
     * @param start
     *            The start offset into the compressed stream. May be changed
     * @param start
     *            The start offset into the compressed stream. May be changed
     * 
     * @return start The start offset into the compressed stream. May be changed
     * */
    public String readLines2(String filePath, long offset, int lineMaxNum) throws Exception {

        long time_begin = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Path file_path = new Path(filePath);

        // ignoring files like _SUCCESS
        if (file_path.getName().startsWith("_")) {
            return null;
        }

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(file_path);
        FileSystem fileSystem = FileSystem.get(file_path.toUri(), conf);
        InputStream stream = null;

        // check if we have a compression codec we need to use
        if (codec != null) {
            System.out.println("this is a compression file");
            stream = codec.createInputStream(fileSystem.open(file_path));
        } else {
            System.out.println("this is a normal file");
            stream = fileSystem.open(file_path);
        }

        InputStreamReader isr = new InputStreamReader(stream);
        BufferedReader br = new BufferedReader(isr);

        br.skip(offset);

        StringBuffer sbf = new StringBuffer();
        for (int i = 0; i < lineMaxNum; i++) {
            String tmp = br.readLine();
            // System.out.println("new line=" + tmp);
            sbf.append(tmp).append("\n");
        }

        // System.out.println("result=" + sbf.toString());
        long time_end = System.currentTimeMillis();
        System.out.println("timeCost=" + (time_end - time_begin) + " content=" + sbf);
        br.close();
        return sbf.toString();
    }

    /**
     * @param filePath
     *            The start offset into the compressed stream. May be changed
     * @param start
     *            The start offset into the compressed stream. May be changed
     * @param start
     *            The start offset into the compressed stream. May be changed
     * @param start
     *            The start offset into the compressed stream. May be changed
     * 
     * @return start The start offset into the compressed stream. May be changed
     * */
    public String readLines3(String filePath, long offset, long len) throws Exception {

        long time_begin = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Path file_path = new Path(filePath);

        // ignoring files like _SUCCESS
        if (file_path.getName().startsWith("_")) {
            return null;
        }

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(file_path);
        FileSystem fileSystem = FileSystem.get(file_path.toUri(), conf);
        InputStream stream = null;

        // check if we have a compression codec we need to use
        if (codec != null) {
            System.out.println("this is a compression file");
            stream = codec.createInputStream(fileSystem.open(file_path));
        } else {
            System.out.println("this is a normal file");
            stream = fileSystem.open(file_path);
        }
        byte[] b = null;
        stream.read(b, (int) offset, (int) len);
        stream.close();
        StringBuffer sbf = new StringBuffer();

        sbf.append(b).append("\n");

        long time_end = System.currentTimeMillis();
        System.out.println("timeCost=" + (time_end - time_begin) + " content=" + sbf);

        return sbf.toString();
    }

    /**
     * @param filePath
     *            The start offset into the compressed stream. May be changed
     * @param start
     *            The start offset into the compressed stream. May be changed
     * @param start
     *            The start offset into the compressed stream. May be changed
     * @param start
     *            The start offset into the compressed stream. May be changed
     * 
     * @return start The start offset into the compressed stream. May be changed
     * */
    public String readLines4(String filePath, long offset, long len) throws Exception {

        long time_begin = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Path file_path = new Path(filePath);

        // ignoring files like _SUCCESS
        if (file_path.getName().startsWith("_")) {
            return null;
        }

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(file_path);
        FileSystem fileSystem = FileSystem.get(file_path.toUri(), conf);
        InputStream stream = null;

        // check if we have a compression codec we need to use
        if (codec != null) {
            System.out.println("this is a compression file");
            stream = codec.createInputStream(fileSystem.open(file_path));
        } else {
            System.out.println("this is a normal file");
            stream = fileSystem.open(file_path);
        }

        InputStreamReader isr = new InputStreamReader(stream);
        BufferedReader br = new BufferedReader(isr);
        char[] b = null;
        br.read(b, (int) offset, (int) len);

        StringBuffer sbf = new StringBuffer();

        sbf.append(b).append("\n");

        long time_end = System.currentTimeMillis();
        System.out.println("timeCost=" + (time_end - time_begin) + " content=" + sbf);
        br.close();
        return sbf.toString();
    }
}
