package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("UserMessages".equalsIgnoreCase(otherArgs[0])) {

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, UserMessages.UserMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, UserMessages.MessageMapper.class ); 

	    job.setReducerClass(UserMessages.JoinReducer.class);

	    job.setOutputKeyClass(UserMessages.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(UserMessages.OUTPUT_VALUE_CLASS);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));} 

	else if ("Task1a".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					TextInputFormat.class, Task1a.HostnameMapper.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, Task1a.CountryMapper.class ); 
		job.setReducerClass(Task1a.JoinReducer.class);
		job.setOutputKeyClass(Task1a.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Task1a.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));}

	else if ("Task1b".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Task1b.ReducerImpl.class);
		job.setMapperClass(Task1b.MapperImpl.class);
		job.setOutputKeyClass(Task1b.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Task1b.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));} 

	else if ("Task1c".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Task1c.ReducerImpl.class);
		job.setMapperClass(Task1c.MapperImpl.class);
		job.setOutputKeyClass(Task1c.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Task1c.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));} 
	

	else if ("Task2a".equalsIgnoreCase(otherArgs[0])) {

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					TextInputFormat.class, Task2a.URLMapper.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, Task2a.CountryMapper.class ); 
		job.setReducerClass(Task2a.JoinReducer.class);
		job.setOutputKeyClass(Task2a.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Task2a.OUTPUT_VALUE_CLASS);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));}

	else if ("Task2b".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Task2b.ReducerImpl.class);
		job.setMapperClass(Task2b.MapperImpl.class);
		job.setOutputKeyClass(Task2b.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Task2b.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));}

	else if ("Task2c".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Task2c.ReducerImpl.class);
		job.setMapperClass(Task2c.MapperImpl.class);
			
			job.setSortComparatorClass(Task2c.SortComparator.class);
			
		job.setOutputKeyClass(Task2c.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Task2c.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}

	else if ("Task3a".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(Task3a.ReducerImpl.class);
	    job.setMapperClass(Task3a.MapperImpl.class);
            
            job.setGroupingComparatorClass(Task3a.GroupingComparator.class);
            job.setSortComparatorClass(Task3a.SortComparator.class);
            
	    job.setOutputKeyClass(Task3a.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(Task3a.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	}

	else if ("Task3b".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Task3b.ReducerImpl.class);
		job.setMapperClass(Task3b.MapperImpl.class);
		job.setOutputKeyClass(Task3b.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Task3b.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));} 

	else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
