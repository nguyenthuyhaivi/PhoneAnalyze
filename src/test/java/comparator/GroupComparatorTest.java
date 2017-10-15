package comparator;

import model.CustomerWritable;
import model.KeyPair;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by nguyenthuyhaivi on 10/16/17.
 */
public class GroupComparatorTest {
    private GroupComparator tester;

    @Before
    public void before(){
        tester = new GroupComparator();
    }

    @Test
    public void testCompare(){
        assertThat(tester.compare(createKeyPair("2", 1), createKeyPair("1", 1))).isGreaterThan(0);
        assertThat(tester.compare(createKeyPair("1", 1), createKeyPair("1", 1))).isEqualTo(0);
        assertThat(tester.compare(createKeyPair("0", 1), createKeyPair("1", 1))).isLessThan(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongInstanceTypeForFirstParam(){
        tester.compare(new CustomerWritable(), createKeyPair("1", 1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongInstanceTypeForSecondParam(){
        tester.compare(createKeyPair("1", 1), new CustomerWritable());
    }


    private KeyPair createKeyPair(String first, int second){
        return new KeyPair(new Text(first), second);
    }
}
