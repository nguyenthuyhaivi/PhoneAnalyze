import model.CustomerWritable;
import model.KeyPair;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by nguyenthuyhaivi on 10/14/17.
 */
public class CustomerReducer extends Reducer<KeyPair, CustomerWritable, Text, Text> {

    @Override
    protected void reduce(KeyPair key, Iterable<CustomerWritable> values, Context context) throws IOException, InterruptedException {
        DateWritable actualActivationDate = findActualActivationDate(values);
        if (actualActivationDate != null){
            context.write(key.getFirst(), new Text(actualActivationDate.toString()));
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        context.write(new Text("PHONE_NUMBER"), new Text("REAL_ACTIVATION_DATE"));

        super.run(context);
    }

    private DateWritable findActualActivationDate(Iterable<CustomerWritable> records) {
        Iterator<CustomerWritable> iterator = records.iterator();
        CustomerWritable currentOwner = new CustomerWritable(iterator.next());

        if (currentOwner.getDeactivationDate().getDays() != 0) {
            return null;
        }

        while (iterator.hasNext()){
            CustomerWritable oldRecord = new CustomerWritable(iterator.next());
            if (currentOwner.getActivationDate().compareTo(oldRecord.getDeactivationDate()) != 0){
                return currentOwner.getActivationDate();
            }
            currentOwner.setActivationDate(oldRecord.getActivationDate());

        }
        return currentOwner.getActivationDate();
    }
}
