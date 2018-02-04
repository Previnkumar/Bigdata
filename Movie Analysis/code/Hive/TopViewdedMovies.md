### Pre-Requisite

1)Hadoop Installation (Guide available at Bigdata/InstallationGuide)

2)Start the cluster (mentioned in Bigdata/InstallationGuide)

3)Start the Hive (mentioned in Bigdata/HiveGuide)

### Procedure

The top 10 most viewed movies in overall rawdata given can be predicted by the following hive query

```
create table topviewed as with t AS 
( 
  select title,count(userid) as users from combined_moviedata
  group by title
  order by users desc limit 10
)
select * FROM t ORDER BY title;
```

To see the output of data extracted by query

```
select * from topviewed;
```
