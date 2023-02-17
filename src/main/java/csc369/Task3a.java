package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task3a {

    public static final Class OUTPUT_KEY_CLASS = CountryURL.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, CountryURL, Text> {
          @Override
          public void map(LongWritable key, Text value, Context
                          context) throws IOException, InterruptedException {
                String[] sa = value.toString().split("\t");
                String [] country_and_url = sa[0].split("-----"); ///delimter from part 2

                Text country = new Text(country_and_url[0]);
                Text url = new Text(country_and_url[1]);

                context.write(new CountryURL(new Text(country), url), new Text(country));
          }
    }
    
    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(CountryURL.class, true);
        }


        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
                            CountryURL pair = (CountryURL) wc1;
                            CountryURL pair2 = (CountryURL) wc2;
            return pair.compareTo(pair2);
        }
    }
    // used to group (year,month,day) data by (year,month)
    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(CountryURL.class, true);
        }
        
        @Override
        public int compare(WritableComparable wc1,
                        WritableComparable wc2) {
                            CountryURL pair = (CountryURL) wc1;
                            CountryURL pair2 = (CountryURL) wc2;
            return pair.getURL().compareTo(pair2.getURL());
        }
    }



    
    public static class ReducerImpl extends Reducer<CountryURL, Text, Text, Text> {
            @Override
            protected void reduce(CountryURL key,
                                  Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


                    String country_list = "";
                    for (Text country : values){
                        country_list += (country + ", ");
                    }
                    context.write(key.getURL(), new Text(country_list));

            }
    }
}


