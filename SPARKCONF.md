##Spark Configuration

oni-ml main component uses Spark and Spark SQL to analyze network events and thus produce a list of least probable events
or most suspicious. 

Before running oni-ml users need to set up some configurations required by this component and follow the next recommendations.  

### Yarn tuning

oni-ml Spark application is conceived to [run on a _Yarn_ cluster](http://spark.apache.org/docs/latest/running-on-yarn.html) and depending
on the amount of data users are planning to analyze it might result very important to tune _Yarn_ cluster before running oni-ml
or not. 

For small data sets, perhaps a couple of hundreds of gigabytes of CSV data, default _Yarn_ configurations should be enough
but if users try to analyze hundreds of gigabytes of data in parquet format it's probable that it don't work; _Yarn_ most likely will start killing
 containers or will terminate the application with Out Of Memory errors.

This document is not intended to explain in detail how to tune _Yarn_ but instead it provides some articles we used for this task:

- [Cloudera Tuning Yarn](https://www.cloudera.com/documentation/enterprise/5-6-x/topics/cdh_ig_yarn_tuning.html)
- [Tune Hadoop Cluster to get Maximum Performance](http://crazyadmins.com/tag/tuning-yarn-to-get-maximum-performance)
- [Best Practices for YARN Resource Management](https://www.mapr.com/blog/best-practices-yarn-resource-management)

Users need to keep in mind that to get a Spark application running, especially for big data sets, it might take more than
one try before getting any results.

### Spark Properties

When running _Spark_ on _Yarn_ users can set up a set of properties in order to get the best performance and consume resources in a more
effective way. Since not all clusters are the same and not all users are planning to have the same capacity of computation, we have created
variables that users need to configure before running oni-ml.

After installing oni-setup users will find the [duxbay.conf](https://github.com/Open-Network-Insight/oni-setup/blob/dev/duxbay.conf) 
file under /etc folder. This file contains all the required configuration tu run oni-ml, as explained in [INSTALL.md](https://github.com/Open-Network-Insight/oni-ml/blob/dev/INSTALL.md). In this
file exist a section for _Spark_ properties, below is the explanation for each of those variables:

            SPK_EXEC=''                 ---> Maximumn number of executors
            SPK_EXEC_MEM=''             ---> Memory per executor in MB i.e. 30475m
            SPK_DRIVER_MEM=''           ---> Driver memory in MB i.e. 39485m
            SPK_DRIVER_MAX_RESULTS=''   ---> Maximumn driver results in MB or GB i.e. 8g
            SPK_EXEC_CORES=''           ---> Cores per executor i.e. 4
            SPK_DRIVER_MEM_OVERHEAD=''  ---> Driver memory overhead in MB i.e. 3047. Note that there is no "m" at the end.
            SPAK_EXEC_MEM_OVERHEAD=''   ---> Executor memory overhead in MB i.e. 3047. Note that there is no "m" at the end.

The following code is an extract of ml_ops.sh that shows how the application is executed and  how 
the variables are assigned to _Spark_ properties:

            spark-submit --class "org.opennetworkinsight.SuspiciousConnects" \
              --master yarn-client \
              --driver-memory ${SPK_DRIVER_MEM} \
              --conf spark.driver.maxResultSize=${SPK_DRIVER_MAX_RESULTS} \
              --conf spark.driver.maxPermSize=512m \
              --conf spark.driver.cores=1 \
              --conf spark.dynamicAllocation.enabled=true \
              --conf spark.dynamicAllocation.minExecutors=1 \
              --conf spark.dynamicAllocation.maxExecutors=${SPK_EXEC} \
              --conf spark.executor.cores=${SPK_EXEC_CORES} \
              --conf spark.executor.memory=${SPK_EXEC_MEM} \
              --conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=512M -XX:PermSize=512M" \
              --conf spark.shuffle.io.preferDirectBufs=false    \
              --conf spark.kryoserializer.buffer.max=512m \
              --conf spark.shuffle.service.enabled=true \
              --conf spark.yarn.am.waitTime=1000000 \
              --conf spark.yarn.driver.memoryOverhead=${SPK_DRIVER_MEM_OVERHEAD} \
              --conf spark.yarn.executor.memoryOverhead=${SPAK_EXEC_MEM_OVERHEAD} target/scala-2.10/oni-ml-assembly-1.1.jar
              
Besides the variables in duxbay.conf, users can modify the rest of the properties in ml_ops.sh based on their needs.
 
 #### Setting Spark properties
 
 After _Yarn_ cluster has been tuned the next step is to set Spark properties assigning the right values to duxbay.conf _Spark_
 variables. 
  
 #####Number of Executors, Executor Memory, Executor Cores and Executor Memory Overhead
 
  The first thing users need to know is how to set the number of executors and the memory per executor as well as the number of cores.
  To get that number, users should know the available total memory per node after _Yarn_ tuning, this total memory is determined by _yarn.nodemanager.resource.memory-mb_ 
  property and the total number of available cores is given by _yarn.nodemanager.resource.cpu-vcores_.
 
 Depending on the total physical memory available for _Yarn_ containers and given the number of files and the density 
 of the data in each file, the memory per executor can determine the starting point to set the total amount of executors, take 
 the next scenario as an example: 
 
 >To analyze a data set of netflow records, 770 GB parquet format which translates into 12200 files, 70 MB each file average
 we have set 30475 MB of memory for each executor. With a 9 worker nodes cluster, 152 GM memory available for each node we have set the maximum number
 of executors to 43, where 43/9 = 4.7 ~ 5. The reason to allocate only 43 executors is because _Yarn_ is going to create up to 5 in the first 8 nodes and 3 in the 9th node
 and that way we reserve enough resources for the driver; the 9th node will allocate only ~ 90 GB memory for executors 
 and leave the rest available for the driver and other services running.
 
 In the previous example, the reason to start with 30 GB memory per executor is because given the 152 GB available, 5 executors 
 per node for 8 nodes and 3 executors for the node running the driver, will potentially leave less unused resources. See the next example:
 
 >Having the same 152 GB physical memory available as the previous example, if we set executors with 50 GB memory we could only 
 set Spark to run with 25 executors. 3 executors the first 8 nodes and 1 executor the 9th node. The reason to do not set 26 executors
, 3 executors the first 8 nodes and 2 in the 9th executor is because that would try to allocate exactly 100 GB leaving 
limited resources to the diver and the rest of the running services.

With the previous example, the unused resources increase to more than 60 GB while in the first example we just leave around 15 GB unused.

Perhaps the next question is _why not to set more executors with less memory and have more parallel tasks?_ The answer
to that question will depend again on the density of the data, the number of files in HDFS and the amount of records per file. 
Setting a small memory can drive to **Out Of Memory Errors** but users can experiment until they get the right settings for their
data set.

Continuing with executor cores, users can determine the number of cores per executor having the total of executors and the
available vcpus per node. For example:
 
 >Taking the first example where we set 43 executors 30475 MB memory each, having a total of 48 vcpus per node and a total of 9 nodes
 we can set up to 10 cores per executor because 9\*48 = 432/43 ~ 10 cores per executor.
 
 Although it sounds like a good idea to allocate all the available cores, we have seen cases where many cores 
 per executor will cause Spark to assign more task to every executor and that can potentially cause **OOM** errors. In the 
 example above we have tested with up to 6 cores per executor and that worked well.
 
 Lastly, for overhead memory we recommend to use something between 8% and 10% of executor memory.
 
 Following the first example, the values for the _Spark_ variables in duxbay.conf would look like this:
 
            SPK_EXEC='43'
            SPK_EXEC_MEM='30475m'
            SPK_EXEC_CORES='6'
            SPAK_EXEC_MEM_OVERHEAD='3047'
 
#####Driver Memory, Driver Maximum Results and Driver Memory Overhead
 
 During our tests, we have set driver memory the same 
 memory as the executors. If there is a large number of executors with small memory, lest's say less than 5 GB, we then recommend
 allocating a different amount of memory for the driver.
 
 In oni-ml driver will load and _.collect_ results from oni-lda-c (ML section) which can grow up to gigabytes of data, _.orderBy_ and 
 finally _.saveAsTextFile_. Given those activities for word creation, it's recommended to assign a 
 considerable amount of memory for the driver. 
 
 The same way, driver maximum results should be enough for the serialized results.
 
 Depending on users data volume these two properties can be small as tens of gigabytes for driver and a couple of gigabytes for 
 driver maximum results or get to 50 GB and 8 GB respectively. See the next example:
 
 >Following the example in the section above, with 9 nodes, 43 executors, 30 GB memory each executor, we have set driver memory
 to 30GB and 8GB driver maximum results. That's because the data we have processed returned results from ML up to 2.1 GB.
 
 Memory overhead for driver can be set to something between 8% and 10% of driver memory.
 
 This is how _Spark_ variables look like for driver properties:
 
        SPK_DRIVER_MEM='30475m'
        SPK_DRIVER_MAX_RESULTS='8g'
        SPK_DRIVER_MEM_OVERHEAD='3047'
 ![DriverMemory](https://raw.githubusercontent.com/Open-Network-Insight/oni-docs/master/images/Driver%20Memory.png)
 
 _Representation of memory allocation in driver node._
 
 
 For more information about Spark properties click [here](http://spark.apache.org/docs/latest/configuration.html).
 
 ###Known Spark error messages running oni-ml
 
 ####Out Of Memory Error
 
This issue includes _java.lang.OutOfMemoryError: Java heap space_ and _java.lang.OutOfMemoryError : GC overhead limit exceeded_.
When users get OOME can be for many different issues but we have identified a couple of important points or reasons for this
error in oni-ml. 

The main reason for this error in oni-ml can be when oni-lda-c (ML part of oni-ml) returns many results for _word probabilities per topic_.
These results are being broadcast so each executor needs more memory.

Another possible reason for this error is driver is running out of memory, try increasing driver memory.

####Container killed by Yarn for exceeding memory limits. _X.Y_ GB of _X_ GB physical memory used. Consider boosting spark.yarn.executor memoryOverhead

This issue is caused by certain operations, mainly during the join of _document probabilities per topic_ with the rest of 
the data - scoring stage. If users receive this error they should try with increasing memory overhead up to 10% of executor memory
or increase executors memory.

####org.apache.spark.serializer.KryoSerializer 

KryoSerializer can cause issues if property _spark.kryoserializer.buffer.max_ is not enough for the data being serialized.
Try increasing memory up to 2 GB but keeping in mind the total of the available memory.

 