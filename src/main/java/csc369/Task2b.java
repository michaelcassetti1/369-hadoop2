package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2b {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
        String [] logs = sa[1].split(" ");
	    String country = new String(sa[0] + "-----"); // use this a my delimitter
        String country_url = country + " " + logs[6];
        context.write(new Text(country_url), one);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text country_url, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {

            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(country_url, result);
       }
    }

}
