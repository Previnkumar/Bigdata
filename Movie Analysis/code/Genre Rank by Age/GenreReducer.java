package com.movieanalysis.genres_Age;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenreReducer extends Reducer<IntWritable,JoinWritable, Text, Text>{

	//age , 4 genres + rating
	int Action=0,Adventure=0,Animation=0,Children=0,Comedy=0,Crime=0,Documentary=0,Drama=0,Fantasy=0,FilmNoir=0;
	int Horror=0, Musical=0, Mystery=0, Romance=0, SciFi=0, Thriller=0, War=0, Western=0;
	
	int ActionRating=0,AdventureRating=0,AnimationRating=0,ChildrenRating=0,ComedyRating=0,CrimeRating=0,DocumentaryRating=0,DramaRating=0,FantasyRating=0,FilmNoirRating=0;
	int HorrorRating=0, MusicalRating=0, MysteryRating=0, RomanceRating=0, SciFiRating=0, ThrillerRating=0, WarRating=0, WesternRating=0;
	
	int ratings = 0;
	String ageGroup = null;
	DecimalFormat df = new DecimalFormat("#.##");
	public void reduce(IntWritable key, Iterable<JoinWritable> values, Context context) throws IOException, InterruptedException
	{
		 Action=0;Adventure=0;Animation=0;Children=0;Comedy=0;Crime=0;Documentary=0;Drama=0;Fantasy=0;FilmNoir=0;
		 Horror=0; Musical=0; Mystery=0; Romance=0; SciFi=0; Thriller=0; War=0; Western=0;
		 ratings = 0;
		 ActionRating=0;AdventureRating=0;AnimationRating=0;ChildrenRating=0;ComedyRating=0;CrimeRating=0;DocumentaryRating=0;DramaRating=0;FantasyRating=0;FilmNoirRating=0;
		 HorrorRating=0; MusicalRating=0; MysteryRating=0; RomanceRating=0; SciFiRating=0; ThrillerRating=0; WarRating=0; WesternRating=0;
		 ageGroup = null;
		 
		for(JoinWritable val: values)
		{
			ratings = Integer.parseInt(val.getRating().toString());
			if(val.getGenre1().toString()!="-")
			{
				Inc(val.getGenre1().toString(),ratings);
			}
			if(val.getGenre2().toString()!="-")
			{
				Inc(val.getGenre2().toString(),ratings);
			}
			if(val.getGenre3().toString()!="-")
			{
				Inc(val.getGenre3().toString(),ratings);
			}
			if(val.getGenre4().toString()!="-")
			{
				Inc(val.getGenre4().toString(),ratings);
			}			
		
		}
		
		if(key.get()==18)
		{							
			ageGroup = "18-35";
		}
		else if(key.get()==36)
		{
			ageGroup = "36-50";
		}
		else if(key.get()==51)
		{
			ageGroup = "50+";				
		}

		//write average rating for each genre with age group
		if(!ageGroup.equals(null)) {
			context.write(new Text("Age Group: "+ ageGroup+"\t"), new Text(new Text("Average Rating: Action: "+ df.format((float)ActionRating/Action) +" Adventure "+ df.format((float)AdventureRating/Adventure)
			+" Animation "+df.format((float)AnimationRating/Animation) +" Children's " +df.format((float) ChildrenRating/Children) + " Comedy "+df.format((float)ComedyRating/Comedy) +" Crime "+df.format((float)CrimeRating/Crime)
			+" Documentary "+df.format((float) DocumentaryRating/Documentary) +" Drama "+df.format((float)DramaRating/Drama) +" Fantasy "+df.format((float)FantasyRating/Fantasy) +" Film-Noir" + df.format((float)FilmNoirRating/FilmNoir)
			+" Horror " + df.format((float)HorrorRating/Horror) + " Musical "+df.format((float)MusicalRating/Musical) +" Mystery "+df.format((float)MysteryRating/Mystery) +" Romance "+df.format((float)RomanceRating/Romance) 
			+ " Sci-Fi "+df.format((float)SciFiRating/SciFi) + " Thriller "+df.format((float)ThrillerRating/Thriller) +" War " + df.format((float)WarRating/War) +" Western "+df.format((float)WesternRating/Western))));
		}
	}
	
	public void Inc(String genre,int ratings)
	{
		if(genre.equals("Action"))
		{
			Action++; ActionRating += ratings;				
		}
		else if(genre.equals("Adventure"))
		{
			Adventure++; AdventureRating += ratings;
		}
		else if(genre.equals("Animation"))
		{
			Animation++; AnimationRating += ratings;
		}
		else if(genre.equals("Children's"))
		{
			Children++; ChildrenRating += ratings;
		}
		else if(genre.equals("Comedy"))
		{
			Comedy++; ComedyRating += ratings;
		}		
		else if(genre.equals("Crime"))
		{
			Crime++; CrimeRating += ratings;
		}
		else if(genre.equals("Documentary"))
		{
			Documentary++; DocumentaryRating += ratings;
		}		
		else if(genre.equals("Drama"))
		{
			Drama++; DramaRating += ratings;
		}
		else if(genre.equals("Fantasy"))
		{
			Fantasy++; FantasyRating += ratings;
		}
		else if(genre.equals("Film-Noir"))
		{
			FilmNoir++; FilmNoirRating += ratings;
		}
		else if(genre.equals("Horror"))
		{
			Horror++; HorrorRating += ratings;
		}		
		else if(genre.equals("Musical"))
		{
			Musical++; MusicalRating += ratings;
		}
		else if(genre.equals("Mystery"))
		{
			Mystery++; MysteryRating += ratings;
		}		
		else if(genre.equals("Romance"))
		{
			Romance++; RomanceRating += ratings;
		}
		else if(genre.equals("Sci-Fi"))
		{
			SciFi++; SciFiRating += ratings;
		}
		else if(genre.equals("Thriller"))
		{
			Thriller++; ThrillerRating += ratings;
		}
		else if(genre.equals("War"))
		{
			War++; WarRating += ratings;
		}
		else if(genre.equals("Western"))
		{
			Western++; WesternRating += ratings;
		}

		
	}
}
