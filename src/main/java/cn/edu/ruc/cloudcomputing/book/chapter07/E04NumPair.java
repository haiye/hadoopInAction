package cn.edu.ruc.cloudcomputing.book.chapter07;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class E04NumPair implements WritableComparable<E04NumPair> {
    private LongWritable line;
    private LongWritable location;

    public E04NumPair() {
        set(new LongWritable(0), new LongWritable(0));
    }

    public void set(LongWritable first, LongWritable second) {
        this.line = first;
        this.location = second;
    }

    public E04NumPair(LongWritable first, LongWritable second) {
        set(first, second);
    }

    public E04NumPair(int first, int second) {
        set(new LongWritable(first), new LongWritable(second));
    }

    public LongWritable getLine() {
        return line;
    }

    public LongWritable getLocation() {
        return location;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        line.readFields(in);
        location.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        line.write(out);
        location.write(out);
    }

    public boolean equals(E04NumPair o) {
        if ((this.line == o.line) && (this.location == o.location))
            return true;
        return false;
    }

    @Override
    public int hashCode() {
        return line.hashCode() * 13 + location.hashCode();
    }

    @Override
    public int compareTo(E04NumPair o) {
        if ((this.line == o.line) && (this.location == o.location))
            return 0;
        return -1;
    }
}
