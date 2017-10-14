import model.CustomerWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.Date;

import static org.mockito.Mockito.*;

/**
 * Created by nguyenthuyhaivi on 10/14/17.
 */
@RunWith(MockitoJUnitRunner.class)
public class CustomerMapperTest {
    private CustomerMapper tester;

    @Mock
    private Mapper.Context context;

    @Before
    public void before() throws IOException, InterruptedException {
        tester = new CustomerMapper();
        doNothing().when(context).write(anyObject(), anyBoolean());
    }

    @Test
    public void testValidInputFullValue() throws IOException, InterruptedException {
        CustomerWritable expectedResult = new CustomerWritable();
        expectedResult.setPhone(new Text("0987000001"));
        expectedResult.setActivationDate(new DateWritable(Date.valueOf("2016-03-01")));
        expectedResult.setDeactivationDate(new DateWritable(Date.valueOf("2016-05-01")));

        tester.map(new LongWritable(1), new Text("0987000001,2016-03-01,2016-05-01"), context);

        verify(context, times(1)).write(expectedResult.getPhone(), expectedResult);
    }

    @Test
    public void testValidInputMissingDeactivationDate() throws IOException, InterruptedException {
        CustomerWritable expectedResult = new CustomerWritable();
        expectedResult.setPhone(new Text("0987000001"));
        expectedResult.setActivationDate(new DateWritable(Date.valueOf("2016-03-01")));

        tester.map(new LongWritable(1), new Text("0987000001, 2016-03-01,"), context);

        verify(context, times(1)).write(expectedResult.getPhone(), expectedResult);
    }

    @Test
    public void testInvalidInput() throws IOException, InterruptedException {
        tester.map(new LongWritable(1), new Text("0987000001,,"), context);
        verify(context, times(0)).write(anyObject(), anyObject());

        tester.map(new LongWritable(1), new Text("0987000001"), context);
        verify(context, times(0)).write(anyObject(), anyObject());

        tester.map(new LongWritable(1), new Text("0987000001,2016-03-01,2016-03-01,2016-03-01"), context);
        verify(context, times(0)).write(anyObject(), anyObject());

        tester.map(new LongWritable(1), new Text("0987000001,ThisIsAnInvalidDate,"), context);
        verify(context, times(0)).write(anyObject(), anyObject());

        tester.map(new LongWritable(1), new Text("0987000001,2016-03-01,ThisIsAnInvalidDate"), context);
        verify(context, times(0)).write(anyObject(), anyObject());
    }
}
