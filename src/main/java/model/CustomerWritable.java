package model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by nguyenthuyhaivi on 10/14/17.
 */
public class CustomerWritable implements WritableComparable<CustomerWritable> {
    private Text phone;
    private DateWritable activationDate;
    private DateWritable deactivationDate;

    public CustomerWritable() {
        this.phone = new Text();
        this.activationDate = new DateWritable();
        this.deactivationDate = new DateWritable();
    }

    public CustomerWritable(CustomerWritable o) {
        this.phone = new Text(o.getPhone());
        this.activationDate = new DateWritable(o.getActivationDate());
        this.deactivationDate = new DateWritable(o.getDeactivationDate());
    }

    @Override
    public int compareTo(CustomerWritable o) {
        int phoneEquality = phone.compareTo(o.getPhone());
        int activationDateEquality = activationDate.compareTo(o.getDeactivationDate());
        int deactivationDateEquality = deactivationDate.compareTo(o.getDeactivationDate());
        return phoneEquality | activationDateEquality | deactivationDateEquality;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        phone.write(dataOutput);
        activationDate.write(dataOutput);
        deactivationDate.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phone.readFields(dataInput);
        activationDate.readFields(dataInput);
        deactivationDate.readFields(dataInput);
    }


    public Text getPhone() {
        return phone;
    }

    public void setPhone(Text phone) {
        this.phone = phone;
    }

    public DateWritable getActivationDate() {
        return activationDate;
    }

    public void setActivationDate(DateWritable activationDate) {
        this.activationDate = activationDate;
    }

    public DateWritable getDeactivationDate() {
        return deactivationDate;
    }

    public void setDeactivationDate(DateWritable deactivationDate) {
        this.deactivationDate = deactivationDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        CustomerWritable that = (CustomerWritable) o;

        return new EqualsBuilder()
                .append(phone, that.phone)
                .append(activationDate, that.activationDate)
                .append(deactivationDate, that.deactivationDate)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(phone)
                .append(activationDate)
                .append(deactivationDate)
                .toHashCode();
    }
}
