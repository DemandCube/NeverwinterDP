Scribengin Quickstart
=====================

###Docker Setup

#####Prerequisites
* Install Ansible
* Install Docker
* Install Gradle
* Make sure the user you're running as has write permissions for /etc/hosts
  * Setup scripts will update your /etc/hosts file, but will not remove any entries that are already there

#####Setup neverwinterdp_home

- Checkout Scribengin into neverwinterdp_home

```
git clone https://github.com/Nventdata/NeverwinterDP
cd NeverwinterDP
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
#Build images, launch containers, run ansible
./neverwinterdp-deployments/docker/scribengin/docker.sh  cluster --launch --neverwinterdp-home=/your/path/to/NeverwinterDP
```

###Checking the status of your cluster
```
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



###Access the Scribengin Command Line
```
#When the cluster is first launched, there won't be much running
#so the following commands will show limited information.
#Once you launch a dataflow, you'll be able to see more interesting output

#Get the full list of commands the shell can give you
./NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh

#Dump the registry
./NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh registry dump

#Get info on running containers
./NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh vm info

#Get info on running dataflows
./NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh scribengin info
```


###Launching a dataflow from a preconfigured test
```
#There are lots of tests under the integration-tests folder apart from these ones listed below
#The kafka test is a simple, quick test 
./neverwinterdp-deployments/tests/scribengin/integration-tests/Scribengin_Integration_Kafka.sh

#The Log Sample test is a more complicated, longer running test
./neverwinterdp-deployments/tests/scribengin/integration-tests/Scribengin_Integration_Log_Sample.sh
```

###Launching a dataflow manually
```
```


###Navigate to Kibana to view real time metrics
```
Point your browser to http://monitoring-1:5601
```
