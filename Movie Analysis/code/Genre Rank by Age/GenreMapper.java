package com.movieanalysis.genres_Age;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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

public class GenreMapper extends Mapper<LongWritable, Text ,IntWritable,JoinWritable> {

	String tokens[];
	int age = 0;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		tokens = value.toString().split("\\001");		
		age = Integer.parseInt(tokens[8]);
		
		if(age>=18 && age<=35)
		{
			age=18;
		}
		else if(age>=36 && age<=50)
		{
			age=36;
		}
		else if(age>50)
		{
			age=51;
		}
		
		if(age==18 || age==36 || age==51)
		{
			context.write(new IntWritable(age),new JoinWritable(tokens[2],tokens[3],tokens[4],tokens[5],tokens[11]) );
		}
	}
}
