package com.movieanalysis.parse;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MyMapper extends Mapper <LongWritable, Text, NullWritable, Text>{

	String fileName;
	String tokens[],Genres[];
	StringBuffer createString = null;
	MultipleOutputs <NullWritable,Text> mos = null;
	
	@Override
	protected void setup(Context context)
	{
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		fileName = fileSplit.getPath().getName();
		mos = new MultipleOutputs <NullWritable, Text> (context);
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		mos.close();
	}
	
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{

			tokens = value.toString().split("::");
			if(fileName.startsWith("movie"))   
			{			
//				2::Jumanji (1995)::Adventure|Children's				=>input
//		 		MovieID::Title ::Genres								
				
				if(tokens.length == 3)
				{
					createString = new StringBuffer(tokens[0]);
					createString.append("|"+tokens[1]);
					
					Genres = tokens[2].split("\\|");
				
					for(int i=0;i<Genres.length;i++)
					{
						createString.append("|"+ Genres[i] );
					}
			
					for(int i=3 ; i>=Genres.length;i--)
					{
						createString.append("|" + "-" );
					}
					
					mos.write("movies", NullWritable.get(), new Text(createString.toString()),"movies/part");				
				}
				else
				{
					mos.write("badrecords", NullWritable.get(), new Text(fileName + "\t" + value));
				}
			}
			else if(fileName.startsWith("rating"))
			{
				
				// 1::914::3::978301968				
				//UserID::MovieID::Rating::Timestamp
				
				tokens = value.toString().split("::");
				createString = new StringBuffer(tokens[0]);
				if(tokens.length == 4)
				{				
					for(int i=1;i<tokens.length;i++)
					{
							createString.append("|" + tokens[i]);
					}
					mos.write("ratings", NullWritable.get(), new Text(createString.toString()),"ratings/part");
				}	
				else
				{
					mos.write("badrecords", NullWritable.get(), new Text(fileName + "\t" + value));
				}
				
			}
			else if(fileName.startsWith("user"))
			{
				//1::F::1::10::48067
				//UserID::Gender::Age::Occupation::Zip-code
				
				tokens = value.toString().split("::");
				if(tokens.length == 5)
				{
					createString = new StringBuffer(tokens[0]);
					for(int i=1;i<tokens.length;i++)
					{
						createString.append("|" + tokens[i]);
					}
					
					mos.write("users", NullWritable.get(), new Text(createString.toString()),"users/part");
				}
				else
				{
					mos.write("badrecords", NullWritable.get(), new Text(fileName + "\t" + value));
				}	
			}						
	}

}
