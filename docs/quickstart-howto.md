Scribengin Quickstart
=====================

###Docker Setup

#####Prerequisites
* Install Ansible
* Install Docker
* Install Gradle
* Make sure the user you are running as has write permissions for /etc/hosts
  * Setup scripts will update your /etc/hosts file, but will not remove any entries that are already there
* Setup your SSH config
  * 
     ```
     echo -e "Host *\n  StrictHostKeyChecking no" >> ~/.ssh/config
     ``` 
* If you want to work with S3, set up your credentials file in this format
     
     ```
     user@machine $ cat ~/.aws/credentials
     [default]
     aws_access_key_id=XXXXX
     aws_secret_access_key=YYYYYY
     ``` 
 
 

#####Setup neverwinterdp_home

- Checkout Scribengin into neverwinterdp_home

```
git clone https://github.com/Nventdata/NeverwinterDP
cd NeverwinterDP

#You may want to work with the latest code:
#switch to the dev/master branch
git checkout dev/master 

#Pull for the latest code
git pull origin dev/master 

gradle clean build install -x test
gradle release -x test
```

- Set NEVERWINTERDP_HOME environment variable (optional) or you can give --neverwinterdp-home option value in the commandline arguments.

```
export NEVERWINTERDP_HOME=/your/path/to/NeverwinterDP
```

#####Launching Scribengin cluster in Docker
```
#Checkout neverwinterdp-deployments project 
git clone git clone https://<bitbucket_user>@bitbucket.org/nventdata/neverwinterdp-deployments.git
```

#####Build docker image with scribengin in one step
```
#If you're running on a Mac!
#Run the following command, then follow the instructions 
#on the screen for which environment variables to export
boot2docker up

#Build images, launch containers, run ansible
./neverwinterdp-deployments/docker/scribengin/docker.sh  cluster --launch

#If you decided not to set NEVERWINTERDP_HOME, then you can pass it in manually here
./neverwinterdp-deployments/docker/scribengin/docker.sh  cluster --launch--neverwinterdp-home=/your/path/to/NeverwinterDP

#If you wish to DESTROY your cluster (clean images and containers)
./neverwinterdp-deployments/docker/scribengin/docker.sh cluster --clean-containers --clean-image
```

###Checking the status of your cluster
```
#Run the setup script for clusterCommander once
$> sudo ./neverwinterdp-deployments/tools/cluster/setup.sh

#Then run
$> ./neverwinterdp-deployments/tools/cluster/clusterCommander.py status
Role           Hostname         ProcessIdentifier          ProcessID    HomeDir                             Status
-------------  ---------------  -------------------------  -----------  ----------------------------------  --------
elasticsearch  elasticsearch-1
                                com.neverwinterdp.es.Main  216          /opt/elasticsearch/                 Running
generic        monitoring-1
                                /opt/kibana/bin/kibana     205          /opt/kibana                         Running
hadoop-master  hadoop-master
                                NameNode                   470          /opt/hadoop                         Running
                                SecondaryNameNode          565          /opt/hadoop                         Running
                                SecondaryNameNode          565          /opt/hadoop                         Running
                                ResourceManager            640          /opt/hadoop                         Running
hadoop-worker  hadoop-worker-3
                                DataNode                   450          /opt/hadoop                         Running
                                NodeManager                553          /opt/hadoop                         Running
                                vm-master-1                2576         /opt/neverwinterdp/scribengin/bin/  Running
hadoop-worker  hadoop-worker-2
                                DataNode                   437          /opt/hadoop                         Running
                                NodeManager                540          /opt/hadoop                         Running
                                vm-scribengin-master-2     2362         /opt/neverwinterdp/scribengin/bin/  Running
hadoop-worker  hadoop-worker-1
                                DataNode                   437          /opt/hadoop                         Running
                                NodeManager                540          /opt/hadoop                         Running
                                vm-scribengin-master-1     2362         /opt/neverwinterdp/scribengin/bin/  Running
kafka          kafka-3
                                Kafka                      316          /opt/kafka                          Running
kafka          kafka-2
                                Kafka                      316          /opt/kafka                          Running
kafka          kafka-1
                                Kafka                      316          /opt/kafka                          Running
zookeeper      zookeeper-1
                                QuorumPeerMain             285          /opt/zookeeper                      Running
```

###Navigate to Kibana to view real time metrics
```
Point your browser to http://monitoring-1:5601
You can change the interval at which Kibana refreshes itself in the top panel, or manually refresh the page
```


###Launching a dataflow from a preconfigured test
```
#There are lots of tests under the integration-tests folder apart from these ones listed below
#The kafka test is a simple, quick test 
./neverwinterdp-deployments/tests/scribengin/integration-tests/Scribengin_Integration_Kafka.sh

#The Log Sample test is a more complicated, longer running test
./neverwinterdp-deployments/tests/scribengin/integration-tests/Log_Sample_Chain.sh
```

###Launching a dataflow manually

In order to launch the dataflow, we expect that you read the document how to build the cluster with the docker script or digitalocean script. Basically you will need those server install and run:

1. Zookeeper
2. Kafka
3. Hadoop
4. Elasticsearch
5. Run vm-master and scribengin on top of hadoop yarn as an yarn application

You need to build the dataflow log-sample by check out the NeverwinterDP code:

```
#Build and install
cd NeverwinterDP
gradle clean build install -x test

#Build the release
cd NeverwinterDP/release
gradle clean release
```

You will find the release in NeverwinterDP/release/build/release/neverwinterdp

You will need to run the following command in order to generate the sample data, launch the dataflow, run the validator:

```
#The location of the scribengin shell script
SHELL=./scribengin/bin/shell.sh


#Upload the log-sample application from local to remote dfs location
$SHELL vm upload-app --local $APP_DIR --dfs /applications/log-sample

#Submit and run the log generator. The log generator is run in a separate vm 
$SHELL vm submit \
   --dfs-app-home /applications/log-sample \
   --registry-connect zookeeper-1:2181 --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
   --name vm-log-generator-1  --role vm-log-generator --vm-application  com.neverwinterdp.dataflow.logsample.vm.VMToKafkaLogMessageGeneratorApp \
   --prop:report-path=/applications/log-sample/reports --prop:num-of-message=5000 --prop:message-size=512

#Submit and launch the sample log splitter dataflow
$SHELL dataflow submit \
  --dfs-app-home /applications/log-sample \
  --dataflow-config $APP_DIR/conf/splitter/kafka-log-splitter-dataflow.json \
  --dataflow-id log-splitter-dataflow-1 --max-run-time 180000

#This command is wait and detect when the vm log generator terminated. This command is not important
$SHELL vm wait-for-vm-status --vm-id vm-log-generator-1 --vm-status TERMINATED --max-wait-time 45000

#This command dump the registry that the log generator report
$SHELL registry dump --path /applications/log-sample

#This command wait and detect when the dataflow is finished
$SHELL dataflow wait-for-status --dataflow-id log-splitter-dataflow-1 --status TERMINATED
#Print the dataflow info
$SHELL dataflow info --dataflow-id log-splitter-dataflow-1 --show-all


#Submit and run the log validator vm
$SHELL vm submit  \
  --dfs-app-home /applications/log-sample \
  --registry-connect zookeeper-1:2181  --registry-db-domain /NeverwinterDP --registry-implementation com.neverwinterdp.registry.zk.RegistryImpl \
  --name vm-log-validator-1 --role log-validator  --vm-application com.neverwinterdp.dataflow.logsample.vm.VMLogMessageValidatorApp \
  --prop:report-path=/applications/log-sample/reports \
  --prop:num-of-message-per-partition=5000 \
  --prop:wait-for-termination=300000 \
  --prop:validate-kafka=log4j.info,log4j.warn,log4j.error

$SHELL vm wait-for-vm-status --vm-id vm-log-validator-1 --vm-status TERMINATED --max-wait-time 60000

$SHELL vm info
#This command dump the registry that the log generator validator report
$SHELL registry dump --path /applications/log-sample
```




###Access the Scribengin Command Line
```
#When the cluster is first launched, there will not  be much running
#so the following commands will show limited information.
#Once you launch a dataflow, you will be able to see more interesting output

#Get the full list of commands the shell can give you
./NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh

#Dump the registry
./NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh registry dump

#Get info on running containers
./NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh vm info

#Get info on running dataflows
./NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh scribengin info
```


