### Pre-Requisite:

1)Hadoop Installation (Guide available at Bigdata/InstallationGuide)

2)Start the cluster (mentioned in Bigdata/InstallationGuide)

3)Start Hive (mentioned in BigData/hiveGuide)

### Procedure:
With hive, querying the data is too simple.

To load the parsed rawdata(done by parse job) to hive, execute the following commands in hive
The location mentioned in the location where the parse job output is generated

1) Create table named movies and load the parsed data into it

```
create external table movies
(
movieid int,
title string,
genre1 string,
genre2 string,
genre3 string,
genre4 string
)
row format delimited fields terminated by '|'
location '/movieparsed/movies'
```
2) Create table named ratings and load the parsed data into it

```
create external table ratings
(
userid int,
movieid int,
rating int,
timestamp int
)
row format delimited fields terminated by '|'
location '/movieparsed/ratings'
```

3) Create table named users and load the parsed data into it

```
create external table users
(
userid int,
gender string,
age int,
occupation int,
zipcode int
)
row format delimited fields terminated by '|'
location '/movieparsed/users'
```

4) As the tables had created, we need to combine the tables into single table So that we can query easily

```
create table combined_moviedata as 
select movies.movieid , movies.title, movies.genre1, movies.genre2, movies.genre3, movies.genre4, 
users.userid, users.gender, users.age, users.occupation,users.zipcode, 
ratings.rating, ratings.timestamp 
from 
movies join ratings on movies.movieid = ratings.movieid join users on users.userid = ratings.userid;
```

Now the combined table can be viewed by
```
select * from combined_moviedata;
```
