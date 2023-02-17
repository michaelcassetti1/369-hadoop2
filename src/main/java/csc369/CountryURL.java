package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class CountryURL
    implements Writable, WritableComparable<CountryURL> {
    
    private final Text country = new Text();
    private final Text url = new Text();


    public CountryURL() {
    }
    
    public CountryURL(Text country, Text url) {
        this.country.set(country);
        this.url.set(url);
    }
    
    @Override
    public void write(DataOutput out) throws IOException{
        country.write(out);
        url.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        country.readFields(in);
        url.readFields(in);
    }
    
    @Override
    public int compareTo(CountryURL pair) {
        if (url.compareTo(pair.getURL()) == 0) {
            return country.compareTo(pair.getCountry()); 
        }
        return url.compareTo(pair.getURL());
    }
    
    public Text getCountry() {
        return country;
    }
    
    public Text getURL() {
        return url;
    }
    
}
