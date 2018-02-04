package com.movieanalysis.genres_Age;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JoinWritable implements Writable{

	private Text genre1;
	private Text genre2;
	private Text genre3;
	private Text genre4;
	private Text ratings;
	

	public JoinWritable()
	{
		set(new Text(), new Text(),new Text(),new Text(),new Text());
	}

	public JoinWritable(String genre1, String genre2,String genre3, String genre4, String ratings)
	{
		set(new Text(genre1), new Text(genre2), new Text(genre3), new Text(genre4), new Text(ratings));
	}

	public JoinWritable(Text genre1, Text genre2, Text genre3, Text genre4, Text ratings)
	{
		set(genre1, genre2, genre3, genre4, ratings);
	}

	public void set(Text genre1, Text genre2, Text genre3, Text genre4, Text ratings)
	{
		this.genre1 = genre1;
		this.genre2 = genre2;
		this.genre3 = genre3;
		this.genre4 = genre4;
		this.ratings = ratings;
	}

	public Text getGenre1()
	{
		return genre1;
	}

	public Text getGenre2()
	{
		return genre2;
	}
	public Text getGenre3()
	{
		return genre3;
	}
	public Text getGenre4()
	{
		return genre4;
	}
	public Text getRating()
	{
		return ratings;
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		genre1.write(out);
		genre2.write(out);
		genre3.write(out);
		genre4.write(out);
		ratings.write(out);
	}

	public void readFields(DataInput in) throws IOException
	{
		genre1.readFields(in);
		genre2.readFields(in);
		genre3.readFields(in);
		genre4.readFields(in);
		ratings.readFields(in);
	}

	@Override
	public int hashCode()
	{
		return genre1.hashCode() * 163 + genre2.hashCode() + genre3.hashCode() + genre4.hashCode() + ratings.hashCode();
	}

	@Override
	public String toString()
	{
		return genre1 + "\t" + genre2 + "\t" + genre3 +"\t" + genre4 +"\t"+ratings;
	}

}

