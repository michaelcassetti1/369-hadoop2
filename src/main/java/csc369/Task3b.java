package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3b {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split("\t");
	    Text url = new Text(sa[0]);
        Text countries = new Text(sa[1]);
	    context.write(url, countries);
        }
    }

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {
    
        @Override
	protected void reduce(Text url, Iterable<Text> values,
			      Context context) throws IOException, InterruptedException {

            Iterator<Text> itr = values.iterator();
            while (itr.hasNext()){
                context.write(url, new Text(itr.next().toString()));
            }

       }
    }

}
