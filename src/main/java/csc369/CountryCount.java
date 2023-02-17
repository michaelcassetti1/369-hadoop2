package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class CountryCount
    implements Writable, WritableComparable<CountryCount> {
    
    private final Text country = new Text();
    private final IntWritable count = new IntWritable();
    
    public CountryCount() {
    }
    
    public CountryCount(Text country, int count) {
        this.country.set(country);
        this.count.set(count);
    }
    
    @Override
    public void write(DataOutput out) throws IOException{
        country.write(out);
        count.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        country.readFields(in);
        count.readFields(in);
    }
    
    @Override
    public int compareTo(CountryCount pair) {
        if (country.compareTo(pair.getCountry()) == 0) {
            return count.compareTo(pair.count) * -1; // descending order hack
        }
        return country.compareTo(pair.getCountry());
    }
    
    public Text getCountry() {
        return country;
    }
    
    public IntWritable getCount() {
        return count;
    }
    
}
