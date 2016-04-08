#Run Unit Test#

Checkout NeverwinterDP

```
git clone https://github.com/Nventdata/NeverwinterDP
cd NeverwinterDP
```

You may want to work with the latest code, switch to the dev/master branch
````
git checkout dev/master 
````

Pull for the latest code

````
git pull origin dev/master 
````

Generate the eclipse configuration

```
gradle eclipse
```

You need to install the eclipse and import all projects in NeverwinterDP

To run the unit test, go to each project, open any class with UnitTest suffix, choose Run menu and select Run As => Junit Test

From the terminal, you can also go to any NeverwinterDP project and run all the unit test in the project

````
gradle clean test
````

To run a single with gradle

````
gradle clean test -Dtest.single=TestClassName
````

##Important Unit Test###

Here are the important Unit Test

###scribengin/storage###

1. HdfsSSMUnitTest is the unit test for SSM(Segment storage management). A storage framework that allow to concurrently read/write to the same location in the different segment, once a segment is complete or full, the storage manager will merge and optimize the storage. The SSM has 2 parts, one is the registry that keep track of the segments, reader, writer... which can be reused for different sequence storage system such HDFS, S3. The second part is the segment storage, we currently implement for HDFS only
2. HDFSStorageUnitTest, the unit test for the sink/source api for the hdfs implementation
3. KafkaStorageUnitTest, the unit test for the sink/source api for the kafka implementation
4. ESSinkUnitTest, the unit test for sink api for the elasticsearch implementation. Note that there is no source api implementation for the elasticsearch
5. S3SinkSourceIntegrationTest, the test for the sink/source implementation for the S3. Note that this is the interation test, not the unit test since the AWS account is needed to setup correctly in the home directory in order the test can be run.

###scribengin/core###

1. **KafkaTrackingUnitTest**, the most useful unit test since it include almost all the components. The test launch a local scribengin which consist of the embedded zookeeper, kafka, elasticsearch and a local vm jvm engine, the test then launch the tracking dataflow that run on top of the local scribengin.
2. HDFSTrackingUnitTest, the test is similar to the KafkaTrackingUnitTest except that it use HDFS sink instead of kafka sink.
3. HDFSTaggingUnitTest, this test is used to test the hdfs partition. The test is similar to the HDFSTrackingUnitTest test. The main different is in the validation strategy, in the HDFSTrackingUnitTest, the validator run continuously and clean the data, the segments, that have been read and validated. In the HDFSTaggingUnitTest, the validator create a tag, randomly by the position or date time, read the data that tag in the previous cycle, validate and then drop.
4. KafkaWithSimulationIntegrationTest, this test is similar to the KafkaTrackingUnitTest and it simulate 4 failures scenario during the test, random kill the worker, master, kill all master worker, start/resume the dataflow.

###scribengin/dataflow/analytics###

AnalyticsUnitTest, This test launch the the analytics dataflow that describe in 
[Analytics](code-organization-howto.md#analytics)


###client/webui###

WebuiIntegrationTest, this test basically launch the analytics test, launch the http service for the webui, run different rest api test such kill the worker, master.... wait for long time so the user can open a web browser and access localhost:8080/index.html to manually test the web ui. 

#Run The Tracking And The Example Test On The Real Cluster#

