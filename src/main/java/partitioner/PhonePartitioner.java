package partitioner;

import model.CustomerWritable;
import model.KeyPair;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by nguyenthuyhaivi on 10/15/17.
 */
public class PhonePartitioner extends Partitioner<KeyPair, CustomerWritable> {

    @Override
    public int getPartition(KeyPair keyPair, CustomerWritable customerWritable, int i) {
        try {
            long phone = Long.parseLong(keyPair.getFirst().toString());
            if (phone < 0) {
                throw new IllegalArgumentException("First part should be in in format of possitive number: " + phone);
            }
            return (int) (Math.abs(phone * 127) % i);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("First part of KeyPair should be Number String: " + keyPair.getFirst().toString(), e);
        }

    }
}