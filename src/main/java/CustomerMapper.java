import model.CustomerWritable;
import model.KeyPair;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;

/**
 * Created by nguyenthuyhaivi on 10/14/17.
 */
public class CustomerMapper extends Mapper<LongWritable, Text, KeyPair, CustomerWritable> {
    private final static Logger LOGGER = LoggerFactory.getLogger(CustomerMapper.class);
    private final static String DELIMITER = ",";


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] record = line.split(DELIMITER);
        if (isInvalidRecord(record)) {
            return;
        }

        CustomerWritable item = new CustomerWritable();
        item.setPhone(new Text(StringUtils.trim(record[0])));
        try {
            item.setActivationDate(new DateWritable(parseDate(record[1])));
            if (record.length > 2) {
                item.setDeactivationDate(new DateWritable(parseDate(record[2])));
            }
            KeyPair keyPair = new KeyPair(item.getPhone(), item.getActivationDate().getDays());
            context.write(keyPair, item);
        } catch (IllegalArgumentException e) {
            LOGGER.error("Invalid record");
            return;
        }

    }

    private boolean isInvalidRecord(String[] record) {
        return record.length < 2 || record.length > 3;
    }

    private Date parseDate(String str) {
        return Date.valueOf(StringUtils.trim(str));
    }
}
