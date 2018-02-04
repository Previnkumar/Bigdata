package com.movies.genres_profession;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.movies.genres_Age.JoinWritable;

public class MyReducer extends Reducer<IntWritable, JoinWritable, Text,Text>{

	int Action=0,Adventure=0,Animation=0,Children=0,Comedy=0,Crime=0,Documentary=0,Drama=0,Fantasy=0,FilmNoir=0;
	int Horror=0, Musical=0, Mystery=0, Romance=0, SciFi=0, Thriller=0, War=0, Western=0;
	
	int ActionRating=0,AdventureRating=0,AnimationRating=0,ChildrenRating=0,ComedyRating=0,CrimeRating=0,DocumentaryRating=0,DramaRating=0,FantasyRating=0,FilmNoirRating=0;
	int HorrorRating=0, MusicalRating=0, MysteryRating=0, RomanceRating=0, SciFiRating=0, ThrillerRating=0, WarRating=0, WesternRating=0;
	
	int ratings = 0;
	String occ = null;
	int occupation = 0;
	
	DecimalFormat df = new DecimalFormat("#.##");


	public void reduce(IntWritable key, Iterable<JoinWritable> values, Context context) throws IOException, InterruptedException
	{
		 Action=0;Adventure=0;Animation=0;Children=0;Comedy=0;Crime=0;Documentary=0;Drama=0;Fantasy=0;FilmNoir=0;
		 Horror=0; Musical=0; Mystery=0; Romance=0; SciFi=0; Thriller=0; War=0; Western=0;
		 ratings = 0;
		 ActionRating=0;AdventureRating=0;AnimationRating=0;ChildrenRating=0;ComedyRating=0;CrimeRating=0;DocumentaryRating=0;DramaRating=0;FantasyRating=0;FilmNoirRating=0;
		 HorrorRating=0; MusicalRating=0; MysteryRating=0; RomanceRating=0; SciFiRating=0; ThrillerRating=0; WarRating=0; WesternRating=0;
		 occupation = key.get();
		
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
		

		if(occupation == 0)
		{
			occ = "other or not specified";
		}
		else if (occupation == 1)
		{
			occ = "academic/eductor";
		}
		else if (occupation == 2)
		{
			occ = "artist";
		}
		else if (occupation == 3)
		{
			occ = "clerical/admin";
		}
		else if (occupation == 4)
		{
			occ = "college/grad student";
		}
		else if (occupation == 5)
		{
			occ = "customer service";
		}
		else if (occupation == 6)
		{
			occ = "doctor/health care";
		}
		else if (occupation == 7)
		{
			occ = "executive/managerial";
		}
		else if (occupation == 8)
		{
			occ = "farmer";
		}
		else if (occupation == 9)
		{
			occ = "homemaker";
		}
		else if (occupation == 10)
		{
			occ = "k-12 student";
		}
		else if (occupation == 11)
		{
			occ = "lawyer";
		}
		else if (occupation == 12)
		{
			occ = "programmer";
		}
		else if (occupation == 13)
		{
			occ = "retired";
		}
		else if (occupation == 14)
		{
			occ = "sales/marketing";
		}
		else if (occupation == 15)
		{
			occ = "scientist";
		}
		else if (occupation == 16)
		{
			occ = "self-employed";
		}
		else if (occupation == 17)
		{
			occ = "technician/engineer";
		}
		else if (occupation == 18)
		{
			occ = "tradesman/craftsman";
		}
		else if (occupation == 19)
		{
			occ = "unemployed";
		}
		else if (occupation == 20)
		{
			occ = "writer";
		}
		
		//write all genre average rating for each occupation
		
		context.write(new Text(occ+": "), new Text(new Text("Average Rating: Action: "+ df.format((float)ActionRating/Action) +" Adventure "+ df.format((float)AdventureRating/Adventure)
				+" Animation "+df.format((float)AnimationRating/Animation) +" Children's " +df.format((float) ChildrenRating/Children) + " Comedy "+df.format((float)ComedyRating/Comedy) +" Crime "+df.format((float)CrimeRating/Crime)
				+" Documentary "+df.format((float) DocumentaryRating/Documentary) +" Drama "+df.format((float)DramaRating/Drama) +" Fantasy "+df.format((float)FantasyRating/Fantasy) +" Film-Noir" + df.format((float)FilmNoirRating/FilmNoir)
				+" Horror " + df.format((float)HorrorRating/Horror) + " Musical "+df.format((float)MusicalRating/Musical) +" Mystery "+df.format((float)MysteryRating/Mystery) +" Romance "+df.format((float)RomanceRating/Romance) 
				+ " Sci-Fi "+df.format((float)SciFiRating/SciFi) + " Thriller "+df.format((float)ThrillerRating/Thriller) +" War " + df.format((float)WarRating/War) +" Western "+df.format((float)WesternRating/Western))));
		
		
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
