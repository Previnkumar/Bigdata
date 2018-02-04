 ### Pre-Requisite:
1)Hadoop Installation (Guide available at Bigdata/InstallationGuide)

2)Start the cluster (mentioned in Bigdata/InstallationGuide)

3)Start Hive (mentioned in BigData/hiveGuide)

### Procedure:

To extract top 20 rated movies with a condition of minimum number of users(say 40),

the query is 
```
create table top20ratedmovies as select title,sum(ratings) as ratings
from combined_moviedata 
group by title 
having count(userid)>40 
order by ratings desc limit 20;
```
