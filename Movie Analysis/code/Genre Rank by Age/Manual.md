### Pre-Requisite:
1)Hadoop Installation (Guide available at Bigdata/InstallationGuide)

2)Add necessary libraries to classpath (Guide available at Bigdata/Movie Analysis/AddLibraries)

3)Start the cluster (mentioned in Bigdata/InstallationGuide)

### Procedure:
Task: To know the genre ranking by rating for each age category in the rawdata

Build the java files to jar and execute it in terminal as

yarn jar GenreRank.jar com.movieanalysis.genres_Age.MyDriver /user/hive/dbname/combined_moviedata /GenreRank

where

parser.jar => name of jar created

com.movieanalysis.genres_Age.MyDriver => Fully qualified class name

user/hive/dbname/combined_moviedata => input file path(contains the parsed and combined raw data)

GenreRank => output directory

After execution, the output file will be created in the specified output directory
