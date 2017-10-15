package partitioner;

import model.CustomerWritable;
import model.KeyPair;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by nguyenthuyhaivi on 10/16/17.
 */
public class PhonePartitionerTest {
    private PhonePartitioner tester;

    @Before
    public void before() {
        tester = new PhonePartitioner();
    }

    @Test
    public void testGetPartition() {
        KeyPair keyPair = new KeyPair(new Text("123445"), 2);
        assertThat(tester.getPartition(keyPair, new CustomerWritable(), 5)).isEqualTo(0);

        keyPair = new KeyPair(new Text("3424242"), 2);
        assertThat(tester.getPartition(keyPair, new CustomerWritable(), 5)).isEqualTo(4);

        keyPair = new KeyPair(new Text(String.valueOf(Long.MAX_VALUE)), 2);
        assertThat(tester.getPartition(keyPair, new CustomerWritable(), 5)).isEqualTo(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPartitionWithNegativeNumberFormat(){
        KeyPair keyPair = new KeyPair(new Text(String.valueOf(Long.MIN_VALUE)), 2);
        tester.getPartition(keyPair, new CustomerWritable(), 5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPartitionWithInvalidNumberFormat(){
        KeyPair keyPair = new KeyPair(new Text("dfsfsfdsf"), 2);
        tester.getPartition(keyPair, new CustomerWritable(), 5);
    }

}
