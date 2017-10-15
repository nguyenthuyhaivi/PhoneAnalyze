package comparator;

import model.CustomerWritable;
import model.KeyPair;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

/**
 * Created by nguyenthuyhaivi on 10/16/17.
 */
public class KeyComparatorTest {
    private KeyComparator tester;

    @Before
    public void before(){
        tester = new KeyComparator();
    }

    @Test
    public void testCompareByFirstPart(){
        KeyPair value1 = createKeyPair("2", 1);
        KeyPair value2 = createKeyPair("1", 1);
        assertThat(tester.compare(value1, value2)).isGreaterThan(0);

        value1 = createKeyPair("1", 1);
        assertThat(tester.compare(value1, value2)).isEqualTo(0);

        value1 = createKeyPair("0", 1);
        assertThat(tester.compare(value1, value2)).isLessThan(0);
    }

    @Test
    public void testCompareBySecondPartWithReverseOrder(){
        KeyPair value1 = createKeyPair("1", 2);
        KeyPair value2 = createKeyPair("1", 1);

        assertThat(tester.compare(value1, value2)).isLessThan(0);

        value1 = createKeyPair("1", 1);
        assertThat(tester.compare(value1, value2)).isEqualTo(0);

        value1 = createKeyPair("1", 0);
        assertThat(tester.compare(value1, value2)).isGreaterThan(0);
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
