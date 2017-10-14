import model.CustomerWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * Created by nguyenthuyhaivi on 10/14/17.
 */
public class CustomerReducer extends Reducer<Text, CustomerWritable, Text, Text> {
    private static final Comparator<CustomerWritable> COMPARATOR = Comparator.comparing(CustomerWritable::getActivationDate);

    @Override
    protected void reduce(Text key, Iterable<CustomerWritable> values, Context context) throws IOException, InterruptedException {
        //TODO: consider secondary sort for this
        Iterable<CustomerWritable> sortedValues = sort(values, COMPARATOR.reversed());
        DateWritable actualActivationDate = findActualActivationDate(sortedValues);
        if (actualActivationDate != null){
            context.write(key, new Text(actualActivationDate.toString()));
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        context.write(new Text("PHONE_NUMBER"), new Text("REAL_ACTIVATION_DATE"));

        super.run(context);
    }

    private Iterable<CustomerWritable> sort(Iterable<CustomerWritable> values, Comparator<CustomerWritable> comparator){
        return StreamSupport.stream(values.spliterator(), false)
                .map(value -> new CustomerWritable(value))
                .sorted(comparator)
                .collect(Collectors.toList());
    }

    private DateWritable findActualActivationDate(Iterable<CustomerWritable> records) {
        Iterator<CustomerWritable> iterator = records.iterator();
        CustomerWritable currentOwner = iterator.next();

        if (currentOwner.getDeactivationDate().getDays() != 0) {
            return null;
        }

        while (iterator.hasNext()){
            CustomerWritable oldRecord = iterator.next();
            if (currentOwner.getActivationDate().compareTo(oldRecord.getDeactivationDate()) != 0){
                return currentOwner.getActivationDate();
            }
            currentOwner.setActivationDate(oldRecord.getActivationDate());

        }
        return currentOwner.getActivationDate();
    }
}
