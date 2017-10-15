package comparator;

import model.KeyPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by nguyenthuyhaivi on 10/15/17.
 */
public class KeyComparator extends WritableComparator {
    protected KeyComparator() {
        super(KeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (!(a instanceof KeyPair) || !(b instanceof KeyPair)) {
            throw new IllegalArgumentException(
                    String.format("KeyPair type is expected, found %s and %s instead", a.getClass(), b.getClass()));
        }
        KeyPair ip1 = (KeyPair) a;
        KeyPair ip2 = (KeyPair) b;

        int cmp = ip1.getFirst().compareTo(ip2.getFirst());
        if (cmp != 0) {
            return cmp;
        }
        return -Integer.compare(ip1.getSecond(), ip2.getSecond());
    }

}
