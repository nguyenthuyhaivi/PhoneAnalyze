package model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by nguyenthuyhaivi on 10/15/17.
 */
public class KeyPair implements WritableComparable<KeyPair> {

    private Text first;
    private int second;

    public KeyPair() {
        first  = new Text();
    }

    public KeyPair(Text first, int second) {
        this.first = first;
        this.second = second;
    }


    public Text getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        KeyPair keyPair = (KeyPair) o;

        return new EqualsBuilder()
                .append(second, keyPair.second)
                .append(first, keyPair.first)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(first)
                .append(second)
                .toHashCode();
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    @Override
    public int compareTo(KeyPair ip) {
        int cmp = first.compareTo(ip.first);
        if (cmp != 0) {
            return cmp;
        }

        return Integer.compare(second, ip.second);

    }
}