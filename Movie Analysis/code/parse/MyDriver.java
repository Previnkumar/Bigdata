package com.movieanalysis.parse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MyDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		//Read input and output paths
		if(otherArgs.length<2)
		{
			System.err.println("Usage: Parse data <in> [<in>...] <out>");
			System.exit(0);
		}
		

		Job job = new Job(conf,"Parser");
		job.setMapperClass(MyMapper.class);
		job.setJarByClass(MyDriver.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		for(int i=0;i<otherArgs.length-1;++i)
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[i])); //Adding input paths
		}

		//creating parsed output files at specified paths
		MultipleOutputs.addNamedOutput(job, "movies", TextOutputFormat.class , NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ratings", TextOutputFormat.class , NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "users", TextOutputFormat.class , NullWritable.class, Text.class);

		//creating new file for storing unmatched/unqualified records
		MultipleOutputs.addNamedOutput(job, "badrecords", TextOutputFormat.class , NullWritable.class, Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
