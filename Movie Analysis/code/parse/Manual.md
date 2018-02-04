### Pre-Requisite : 
1) Hadoop Installation (Guide available at Bigdata/InstallationGuide)
2) Add necessary libraries to classpath (Guide available at Bigdata/Movie Analysis/AddLibraries)
3) Start the cluster (mentioned in Bigdata/InstallationGuide)

### Procedure

To parse the datasets(available at Bigata/Movie Analysis/rawdata),

Build MyMapper.java and MyDriver.java to jar file And execute it in terminal as 

```yarn jar parser.jar com.movieanalysis.parse.MyDriver /rawdata /parsed_data```

where

parser.jar => name of jar created

org.movieanalysis.parse.MyDriver  => Fully qualified class name

rawdata => input file path(contains the files movies,users,ratings)

parsed_data => output directory


After execution the expected parsed files will be created at the specified output directory
