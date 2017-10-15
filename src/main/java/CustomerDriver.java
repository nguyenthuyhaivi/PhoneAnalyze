import comparator.GroupComparator;
import comparator.KeyComparator;
import model.CustomerWritable;
import model.KeyPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import partitioner.PhonePartitioner;

/**
 * Created by nguyenthuyhaivi on 10/14/17.
 */
public class CustomerDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerDriver.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Analyzing started");
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = new Job(conf);
        job.setJarByClass(CustomerDriver.class);
        job.setJobName("Phone Customer Analyzer");

        job.setMapperClass(CustomerMapper.class);
        job.setReducerClass(CustomerReducer.class);

        job.setPartitionerClass(PhonePartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setMapOutputKeyClass(KeyPair.class);
        job.setMapOutputValueClass(CustomerWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        LOGGER.info("Analyzing ended");
    }
}
