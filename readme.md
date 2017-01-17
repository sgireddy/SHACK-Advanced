<h1>Let's build a SOLID SHACK and scale it</h1>

This is an attempt to promote SOLID SHACK architecture. Let's build a SOLID SHACK and scale it to enterprise level.

<h4>SOLID refers to design principles https://en.wikipedia.org/wiki/SOLID_(object-oriented_design) </h4>
<h4>SHACK (Scala/Spark, H-Hadoop, A-All things Apache, C-Cassandra, K-Kafka)</h4>
<br />
<b>This is Part III of the series, please check 
    <a href='https://github.com/sgireddy/SHACKSparkBasics'> Part I </a> for implementation plan and our fictitious scenario "optimizing promo efficiency for ISellInstoreAndOnline.com". </b>
    <a href='https://github.com/sgireddy/SHACKSparkKafkaCassandra'>Part II for System Setup and Spark-Kafka-Cassandra Integration</a>    
<h4> Module Structure </h4>     
    1. Deep dive into Spark transformations
    2. RDD vs. DStream
    2. Configure Hadoop in Pseudo Distributed Mode on CentOS 7
    3. State Management
     
<br />

<h4>Next Steps</h4>
Connect the dots and provide Batch (daily) & Speed (hourly) layers for our Promo Efficiency Project <br />
Spark 2.0 and Structured Streaming <br />
Containerizing With Docker, Vagrant <br />
Resource Management & Scheduling with MesOS <br />

<h4>Deep dive into Spark transformations</h4>

In <a href='https://github.com/sgireddy/SHACKSparkBasics'> Part I </a> 
we translated a file with JSON strings into RDD[Activity] in just a few lines of efficient code. 
Thanks to Spark those few lines of code is good enough to distribute the work and achieve extreme performance, redundancy and fault tolerance.

Let's see how we could use RDD[Activity] to perform our analytics. 
Here is our original code: (I explicitly specified data types RDD[String] & RDD[Activity] for demonstration even though scala can infer it)

          val sourceFile = tmpFile
          val input: RDD[String] = sc.textFile(sourceFile) 
          val inputRDD: RDD[Activity] = for {
            line <- input
            activity <- tryParse[Activity](line)
          } yield activity 

For our scenario all historical data is captured at daily level (time truncated) and intra-day operations are hourly. Lets truncate timestamp to its nearest hour:

          def getTimeInMillisByHour(timeInMillis: Long) = {
            val cal = Calendar.getInstance()
            cal.setTimeInMillis(timeInMillis)
            val offset = ((cal.get(Calendar.MINUTE) * 60) + cal.get(Calendar.SECOND) ) * 1000
            (timeInMillis - offset) - ((timeInMillis - offset) % 9999)
          }
        
          val inputRDD: RDD[Activity] = for {
            line <- input
            activity <- tryParse[Activity](line)
          } yield activity.copy(timeStamp = getTimeInMillisByHour(activity.timeStamp))
                
Having captured our data at product, referrer, timestamp by hour, lets add a key and cache RDD (included signature for our understanding). 
(Having captured the data in our favorite format, we could checkpoint here in a streaming context and persist to hdfs) 

        val keyedByProduct: RDD[((Int, String, Long), Activity)] = inputRDD.keyBy( a => (a.productId, a.referrer, a.timeStamp)).cache()

Let's capture Hourly activity, i.e. total number of visits, number of times the product added to cart and number of times we secured an order: <br />
 
              val visitsByCode = keyedByProduct.mapValues(v => v.actionCode match {
                  case 0 => (1, 0, 0)
                  case 1 => (1, 1, 0)
                  case 2 => (1, 0, 1)
                  case _ => (0, 0, 0)
               })
              
               val reduced = visitsByCode.reduceByKey((s: (Int, Int, Int), v: (Int, Int, Int)) => ((s._1+ v._1, s._2 + v._2, s._3 + v._3))).sortByKey().collect()
                
                //Optionally Map to model ProductActivityByReferrer 
               val productActivityByReferer = reduced.map { r =>
                  ProductActivityByReferrer(r._1._1, r._1._2, r._1._3, r._2._1.toLong, r._2._2.toLong, r._2._3.toLong)
               }
                
               productActivityByReferer.filter(p => p.productId == 1001 | p.productId == 1002).foreach(println)


Here is the output for first two products (1001 & 1002)

            ProductActivityByReferrer(1001,bing,1484614793673,40,9,25)
            ProductActivityByReferrer(1001,facebook,1484614793673,41,7,27)
            ProductActivityByReferrer(1001,google,1484614793673,49,13,31)
            ProductActivityByReferrer(1001,site,1484614793673,295,91,126)
            ProductActivityByReferrer(1001,yahoo,1484614793673,54,12,38)
            ProductActivityByReferrer(1002,bing,1484614793673,50,16,18)
            ProductActivityByReferrer(1002,facebook,1484614793673,48,16,18)
            ProductActivityByReferrer(1002,google,1484614793673,39,16,12)
            ProductActivityByReferrer(1002,site,1484614793673,260,92,83)
            ProductActivityByReferrer(1002,yahoo,1484614793673,41,11,15)

Let's the same using Spark Data Frames:

             val inputDF = inputRDD.toDF()
              inputDF.registerTempTable("activities")
              val activityByProductReferrer = sqlContext.sql(
                """
                      select productId as product_id, referrer, timeStamp,
                      sum(case when actionCode >= 0 then 1 else 0 end) as viewCount,
                      sum(case when actionCode = 1 then 1 else 0 end) as cartCount,
                      sum(case when actionCode = 2 then 1 else 0 end) as purchaseCount
                      from activities
                      where productId in (1001, 1002)
                      group by productId, referrer, timeStamp
                      order by product_id, referrer
                      """
              )
            
              val result = activityByProductReferrer.map{
                r => ProductActivityByReferrer(r.getInt(0), r.getString(1), r.getLong(2), r.getLong(3), r.getLong(4), r.getLong(5))
              }
            
              result.foreach(println)
Here is the output:

            ProductActivityByReferrer(1002,facebook,1484614793673,48,16,18)
            ProductActivityByReferrer(1002,yahoo,1484614793673,41,11,15)
            ProductActivityByReferrer(1002,bing,1484614793673,50,16,18)
            ProductActivityByReferrer(1001,bing,1484614793673,40,9,25)
            ProductActivityByReferrer(1002,site,1484614793673,260,92,83)
            ProductActivityByReferrer(1001,google,1484614793673,49,13,31)
            ProductActivityByReferrer(1002,google,1484614793673,39,16,12)
            ProductActivityByReferrer(1001,facebook,1484614793673,41,7,27)
            ProductActivityByReferrer(1001,yahoo,1484614793673,54,12,38)
            ProductActivityByReferrer(1001,site,1484614793673,295,91,126)

RDDs outperform Data Frames in most cases & provides us type safety, data frames bring us intuitive SQL syntax. 

<h4>RDD vs. DStream</h4>

<b>What is RDD</b> <br />
RDD is an abstraction where its defined by (ref: kafka-exactly-once github) 
1. A method to divide the work into partitions (getPartition)
2. A method to do the work for a given partition (compute)
3. A list of parent RDDs

It make sense given RDD is a distributed data set where data is stored across nodes and we perform transformations & reductions. 

<a>What is DStream</a> <br />

In a simple sense a Discretised Stream is a continuous sequence of RDDs. Remember StreamingContext, 
it takes SparkContext and duration as parameters for instantiation. DStream is a micro batch of RDDs bound to this duration. 

DStream[T] contains 
        
        DStream[T]
            generatedRDDs: HashMap[Time, RDD[T])
            rememberDuration: Duration

We can perform regular transformations (with some limitations) on DStream, where transformation on rdd[T] will result in DStream[T],
DStream also comes with special transformations window, reduceByKeyAndWindow and special transformations that are responsible for maintaining state
updateStateByKey, mapWithState. DStream also comes with another method foreachRDD where we could perform actions on RDD (like save, print) but can't generate a new RDD (i.e. returns Unit) <br />

Here is the signature for the function transform, it takes a function where the function expects an RDD[T] as input and transforms into RDD[U], 
final transformation itself returns DStream[U]

        def transform[U]((RDD[T])=>RDD[U]):Dstream[U]

Here is the signature for foreeachRDD, it expects a higher order function to do some stuff but returns a Unit.
        
        def foreachRDD((RDD[T]=>Unit):Unit
        

<h4>Spark Streaming & State Management</h4>

Spark uses window operations to maintain state, we need to define Window Size & Slide Interval. 
Lets says each batch operation takes 4s, and we want to keep the data for 12s, then window size would be 12s (must multiples side interval)
and slide interval defines how fast the window could move... a slide interval of 4s means that the window moves after each batch but it keeps two batches of data.

NOte: Both window size and slide interval must be multiples of batch interval.

        //NOTE pseudo code
        Dstream.window(Duration(x), Duration(y)) where x is window size and y is slide interval
        our standard RDD reduce function becomes
        DStream.reduceByWindow((a,b) => a op b), windowinterval, slideinterval)

UpdateStateByKey

        //Note psedo code, will walk through real code later in this section....
        updateStateByKey[T]((Seq[newvalues], currentState:T)=>Option(newState:T))

MapWithState (update state and then optionally map to new type)
        
        //Note: pseudo code, will walk through real code later in this section....
        mapWithState(StateType, MappedType](spec: StateSpec)
        val spec = StateSpec.function(K, Option[V], currentState[T])=>Option[MappedType]
                            .timeout(Seconds(10))


Will walk through the code in a few hours... Please checkout the packages consumer & jobs

<h4>Hadoop Configuration in pseudo distributed mode </h4>
1. Download, extract and move hadoop binaries to /opt <br />

         wget http://www.trieuvan.com/apache/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
         tar -xvzf hadoop-2.7.3.tar.gz
         sudo mv hadoop-2.7.3 /opt/hadoop
2. Hadoop uses ssh public key authentication to communicate with nodes in cluster mode. 
Given we are working in pseudo distributed mode, we need to setup ssh public key authentication. <br />
Generate ssh keys with ssh-keygen & move public key to authorized keys for your user account (or hadoop service account)

        ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
        cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
Verify, with "ssh localhost", if it prompts for a password, then you made a mistake <br />
3. Hadoop Configuration, customize hadoop configuration files at $HADOOP_HOME/etc/hadoop (please use following configuration for reference only)
        
        #hadoop-env.sh
        export JAVA_HOME=/usr/java/latest #${JAVA_HOME}
        export HADOOP_PREFIX=/home/sri/hadoop-2.7.3
        
        #mapred-site.xml
        <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
        </property>        
        
        #yarn-site.xml
        <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
        </property>        
        
        #core-site.xml
        <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
        </property>        
        
        #hdfs-site.xml
        <property>
        <name>dfs.namenode.name.dir</name>
        <value>/home/sri/hdfs/namenode</value>
        </property>
        <property>
        <name>dfs.datanode.data.dir</name>
        <value>/home/sri/hdfs/datanode</value>
        </property>
        <property>
        <name>dfs.replication</name>
        <value>1</value>
        </property>
            
4. (Optional) Add hadoop-conf.sh under /etc/profile.d or directly update /etc/profile to add hadoop commands to PATH 
        
            export HADOOP_PREFIX=/opt/hadoop
            PATH=$PATH:$HADOOP_PREFIX/bin:$HADOOP_PREFIX/sbin
            
5. Run "source /etc/profile" command or reboot to load latest configuration
6. Format namenode (this is most important, if you see binding issues, this may be the root cause)
    
        hadoop namenode -format
7. (Optional) Update /etc/hosts if you are using docker or VM with port forwarding (this might cause binding issues as well)
        
        127.0.0.1   <yourhostname>
8. Start Hadoop
        
        start-dfs.sh
9. Start YARN cluster manager
    
        start-yarn.sh
        
10. Verify if hadoop is running 

        HDFS NameNode http://localhost:50070/ 
        Cluster http://localhost:8088/cluster
        Data Nodes http://localhost:50075 
               
11. If you see any issues check $HADOOP_PREFIX/logs
        
<b>References:</b> <br/>
1. Spark Streaming Programming Guide <br />
2. <a href='https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html'>Hadoop Configuration</a>
3. Lambda Architecture by Ahmed Alkilani <a href='https://app.pluralsight.com/library/courses/spark-kafka-cassandra-applying-lambda-architecture'> link </a> <br />

