package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TreeMap;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1c {

    public static final Class OUTPUT_KEY_CLASS = LongWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
	    Text hostname = new Text();
            hostname.set(sa[0]);
            LongWritable count = new LongWritable(Long.parseLong(sa[1]) * -1); // make negative so we can sort later on
	    context.write(count, hostname);
        }
    }
    
    public static class ReducerImpl extends Reducer<LongWritable, Text, Text, LongWritable> {
	
        @Override
	protected void reduce(LongWritable count, Iterable<Text> hostnames,
			      Context context) throws IOException, InterruptedException {
            for (Text hostname : hostnames) {
                context.write(hostname, new LongWritable(count.get() * -1)); // we are sorted in descending order, undo negative
            }

        }
    }

}

