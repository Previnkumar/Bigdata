package com.movies.genres_profession;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.movies.genres_Age.JoinWritable;

public class MyMapper extends Mapper <LongWritable, Text, IntWritable ,JoinWritable>{

/*
	movieid             	int                 	                    
	title               	string              	                    
	genre1              	string              	                    
	genre2              	string              	                    
	genre3              	string              	                    
	genre4              	string              	                    
	userid              	int                 	                    
	gender              	string              	                    
	age                 	int                 	                    
	occupation          	int                 	                    
	zipcode             	int                 	                    
	rating              	int                 	                    
	timestamp           	int 

*/
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String tokens[] = value.toString().split("\\001");		
		int occupation = Integer.parseInt(tokens[9]);
		if(occupation>=0 && occupation<=20) //valid occupation fields
		{
			context.write(new IntWritable(occupation),new JoinWritable(tokens[2],tokens[3],tokens[4],tokens[5],tokens[11]) );
		}
	}
	
}
