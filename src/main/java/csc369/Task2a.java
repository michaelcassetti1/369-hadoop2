package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2a {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;




	public static class URLMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final IntWritable one = new IntWritable(1);
	
	
			@Override
		protected void map(LongWritable key, Text value,
				   Context context) throws IOException, InterruptedException {
			String[] sa = value.toString().split(" ", 2);
			Text hostname = new Text();
			hostname.set(sa[0]);    
			String log = "A\t" + sa[1];
			context.write(new Text(hostname), new Text(log));
			}
		}

    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
	    String text[] = value.toString().split(",");
	    if (text.length == 2) {
		String hostname = text[0];
		String country = text[1];
		String countryB = "B\t"+ country;
		context.write(new Text(hostname), new Text(countryB));
	    }
	}
    }

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> countries = new ArrayList();
			ArrayList<String> logs = new ArrayList();

			for (Text value : values) {
				String[] sa = value.toString().split("\t");
				if (sa[0].contains("B")) {
					countries.add(sa[1]);
				}
				else{
					logs.add(sa[1]);
				}
			}
			for (String country : countries){
				for (String log : logs){
					context.write(new Text(country), new Text(key.toString() + " " + log));
				}
			}


        }
	}

}
