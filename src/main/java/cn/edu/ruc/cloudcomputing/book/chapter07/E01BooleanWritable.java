package cn.edu.ruc.cloudcomputing.book.chapter07;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class E01BooleanWritable implements WritableComparable {
    private boolean value;

    public E01BooleanWritable() {
    };

    public E01BooleanWritable(boolean value) {
        set(value);
    }

    public void set(boolean value) {
        this.value = value;
    }

    public boolean get() {
        return value;
    }

    public void readFields(DataInput in) throws IOException {
        value = in.readBoolean();
    }

    public void write(DataOutput out) throws IOException {
        out.writeBoolean(value);
    }

    public boolean equals(Object o) {
        if (!(o instanceof E01BooleanWritable)) {
            return false;
        }
        E01BooleanWritable other = (E01BooleanWritable) o;
        return this.value == other.value;
    }

    public int hashCode() {
        return value ? 0 : 1;
    }

    public int compareTo(Object o) {
        boolean a = this.value;
        boolean b = ((E01BooleanWritable) o).value;
        return ((a == b) ? 0 : (a == false) ? -1 : 1);
    }

    public String toString() {
        return Boolean.toString(get());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(E01BooleanWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            boolean a = (readInt(b1, s1) == 1) ? true : false;
            boolean b = (readInt(b2, s2) == 1) ? true : false;
            return ((a == b) ? 0 : (a == false) ? -1 : 1);
        }
    }

    static {
        WritableComparator.define(E01BooleanWritable.class, new Comparator());
    }
}
