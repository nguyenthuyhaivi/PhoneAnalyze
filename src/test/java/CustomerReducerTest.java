import com.google.common.collect.Lists;
import model.CustomerWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.sql.Date;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Created by nguyenthuyhaivi on 10/14/17.
 */
@RunWith(MockitoJUnitRunner.class)
public class CustomerReducerTest {
    private CustomerReducer tester;

    @Mock
    private Reducer.Context context;

    @Before
    public void before() throws IOException, InterruptedException {
        tester = new CustomerReducer();
        doNothing().when(context).write(anyObject(), anyBoolean());
    }

    @Test
    public void testSingleValue() throws IOException, InterruptedException {
        CustomerWritable value = createTestData("0987000001", "2016-05-01", null);
        List<CustomerWritable> values = Lists.newArrayList(value);

        tester.reduce(value.getPhone(), values, context);

        verify(context, times(1)).write(value.getPhone(), new Text(value.getActivationDate().toString()));
    }

    @Test
    public void testMultipleValues() throws IOException, InterruptedException {
        Text key = new Text("0987000001");

        List<CustomerWritable> values = Lists.newArrayList();
        values.add(createTestData(key.toString(), "2016-03-01", "2016-05-01"));
        values.add(createTestData(key.toString(), "2016-02-01", "2016-03-01"));
        values.add(createTestData(key.toString(), "2016-12-01", null));
        values.add(createTestData(key.toString(), "2016-09-01", "2016-12-01"));
        values.add(createTestData(key.toString(), "2016-06-01", "2016-09-01"));

        tester.reduce(key, values, context);

        verify(context, times(1)).write(key, new Text("2016-06-01"));
    }

    @Test
    public void testNoCurrentOwner() throws IOException, InterruptedException {
        CustomerWritable value = createTestData("0987000001", "2016-05-01", "2016-12-1");
        List<CustomerWritable> values = Lists.newArrayList(value);

        tester.reduce(value.getPhone(), values, context);

        verify(context, times(0)).write(anyObject(), anyObject());
    }

    @Test
    public void testCustomOutputHeader() throws IOException, InterruptedException {
        tester.run(context);

        verify(context, times(1)).write(new Text("PHONE_NUMBER"), new Text("REAL_ACTIVATION_DATE"));
    }

    private CustomerWritable createTestData(String key, String activationDate, String deactivationDate) {
        CustomerWritable value = new CustomerWritable();
        value.setPhone(new Text(key));
        value.setActivationDate(new DateWritable(Date.valueOf(activationDate)));
        if (!StringUtils.isEmpty(deactivationDate)) {
            value.setDeactivationDate(new DateWritable(Date.valueOf(deactivationDate)));
        }
        return value;
    }


}
