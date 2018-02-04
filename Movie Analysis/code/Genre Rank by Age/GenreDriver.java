package com.movieanalysis.genres_Age;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class GenreDriver {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	
		//get input and output paths
		if (otherArgs.length < 2)
		{
			System.err.println("Usage: Average rating <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Average rating of Genre by age");

		job.setJarByClass(GenreDriver.class);
		job.setMapperClass(GenreMapper.class);
		job.setReducerClass(GenreReducer.class);
		
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(JoinWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		for(int i = 0;i < otherArgs.length-1 ; ++i)
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
