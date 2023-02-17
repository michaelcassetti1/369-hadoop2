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

public class Task2c {

    public static final Class OUTPUT_KEY_CLASS = CountryCount.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, CountryCount, Text> {
          @Override
          public void map(LongWritable key, Text value, Context
                          context) throws IOException, InterruptedException {
                String[] sa = value.toString().split("\t");
                String [] country_and_url = sa[0].split("-----"); ///delimter from part 2

                Text country = new Text(country_and_url[0]);
                Text url = new Text(country_and_url[1]);

                context.write(new CountryCount(new Text(country), Integer.parseInt(sa[1])), new Text(url));

          }
    }
    
    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(CountryCount.class, true);
        }
        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
                            CountryCount pair = (CountryCount) wc1;
                            CountryCount pair2 = (CountryCount) wc2;
            return pair.compareTo(pair2);
        }
    }
    
    public static class ReducerImpl extends Reducer<CountryCount, Text, Text, Text> {
            @Override
            protected void reduce(CountryCount key,
                                  Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

                    Iterator<Text> itr = values.iterator();
                    while (itr.hasNext()){
                        Text value = new Text(itr.next().toString() + "\t" + key.getCount().toString());
                        context.write(new Text(key.getCountry()), value);
                    }

            }
    }
}


