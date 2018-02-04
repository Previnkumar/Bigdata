### Why Hive

Hive is a data-warehouse on the top of Hadoop. 

Using Hive we can run ad-hoc queries for the analysis on data. 

Hive saves us from writing complex Map-Reduce jobs, rather than that we can submit merely SQL queries.


### Software Download Link
Hive - http://archive.cloudera.com/cdh5/cdh/5/hive-0.13.1-cdh5.3.2.tar.gz

### Installation

Hive installation is merely one step

```tar xzf hive-0.13.1-cdh5.3.2.tar.gz```

### Start Hive mode

In terminal, type the following and hit enter

```bin/hive```

### Metastore config

Hive, by default, comes with derby - a single threaded database.

For a single node cluster, it wont be a issue, But when several users access
it will be a burden and needs changed. 

So, Changing derby to mysql is as follows

#### Step-1: Install MySQL

```sudo apt-get install mysql-server```

#### Step-2: Copy MySQL connector to lib directory

Download MySQL connector (mysql-connector-java-5.1.35-bin.jar) and 
copy it into the $HIVE_HOME/lib directory

Note: $HIVE_HOME refers hive installation directory

#### Step-3: Edit / Create configuration file hive-site.xml

Add following entries in the hive-site.xml (present in $HIVE_HOME/conf)
```
<property>
<name>javax.jdo.option.ConnectionURL</name>
<value>jdbc:mysql://localhost/hcatalog?createDatabaseIfNotExist=true</value>
</property>

<property>
<name>javax.jdo.option.ConnectionUserName</name>
<value>your_username</value>
</property>

<property>
<name>javax.jdo.option.ConnectionPassword</name>
<value>your_password</value>
</property>

<property>
<name>javax.jdo.option.ConnectionDriverName</name>
<value>com.mysql.jdbc.Driver</value>
</property>
```

Now start hive terminal, it will be connected to MySQL. 
Now you can open multiple hive connections, which was not possible with Derby database.


