Scribengin Quickstart
=====================

##General Steps To Setup##

1. You need to check out NeverwinterDP code and build
    * Check out NeverwinterDP code from https://github.com/Nventdata/NeverwinterDP
    * Build with the gradle command
    * Build the scribengin release
2. Setup the scribengin cluster using docker, digitalocean vm or aws vm
    * Install  java and other requirement on the vm
    * Update the /etc/hosts so the vm know about each other
    * Run zookeeper on one vm
    * Run hadoop master and hadoop workers cluster
    * Optionally run kafka, elasticsearch...

##Check Out And Build NeverwinterDP Code##

Checkout The NeverwinterDP code repository

```
git clone https://github.com/Nventdata/NeverwinterDP
cd NeverwinterDP
```
You may want to work with the latest code, switch to the dev/master branch

```
git checkout dev/master 
```

Pull for the latest code

```
git pull origin dev/master 
```

Build the NeverwinterDP code

```
gradle clean build install -x test
```

Create the release

```
cd release
gradle release -x test
```
You will find the release in NeverwinterDP/release/build/release/neverwinterdp

You need tot set NEVERWINTERDP_HOME environment variable (optional) in order the other cluster script can build and deploy the scribengin automatically

```
export NEVERWINTERDP_HOME=/your/path/to/NeverwinterDP
```

In case you do not setup the NEVERWINTERDP_HOME variable, you need to give --neverwinterdp-home /path/to/NeverwinterDP option when run the cluster setup script.



##Docker Setup##

###Prerequisites###

1. Install Ansible
2. Install Docker
3. Install Gradle
4. Make sure the user you are running as has write permissions for /etc/hosts
    * Setup scripts will update your /etc/hosts file, but will not remove any entries that are already there
    * Setup your SSH config

    ```
       echo -e "Host *\n  StrictHostKeyChecking no" >> ~/.ssh/config
    ``` 
    
     * If you want to work with S3, set up your credentials file in this format    

     `````
     user@machine $ cat ~/.aws/credentials
     [default]
     aws_access_key_id=XXXXX
     aws_secret_access_key=YYYYYY
     ````` 
 

###Launching Scribengin cluster in Docker Using neverwinterdp-deployments###

Neverwinterdp team develop various script to build, manage, monitor  the cluster automatically, to checkout neverwinterdp-deployments project 

```
 git clone git clone https://<bitbucket_user>@bitbucket.org/nventdata/neverwinterdp-deployments.git
```

#####Build docker image with scribengin in one step####
If you're running on a Mac, run the following command, then follow the instructions on the screen for which environment variables to export

``````
boot2docker up
``````

Build images, launch containers, run ansible

``````
 ./neverwinterdp-deployments/docker/scribengin/docker.sh  cluster --launch

 #If you decided not to set NEVERWINTERDP_HOME, then you can pass it in manually here
 ./neverwinterdp-deployments/docker/scribengin/docker.sh  cluster --launch--neverwinterdp-home=/your/path/to/NeverwinterDP

``````

If you wish to DESTROY your cluster (clean images and containers)

``````
./neverwinterdp-deployments/docker/scribengin/docker.sh cluster --clean-containers --clean-image

``````

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

###Navigate to Kibana to view real time metrics###

```
  Point your browser to http://monitoring-1:5601
  You can change the interval at which Kibana refreshes itself in the top panel, or manually refresh the page
```


###Launching a dataflow from a preconfigured test###

```
  #There are lots of tests under the integration-tests folder apart from these ones listed below
  #The kafka test is a simple, quick test 
  ./neverwinterdp-deployments/tests/scribengin/integration-tests/Scribengin_Integration_Kafka.sh
    
  #The Log Sample test is a more complicated, longer running test
  ./neverwinterdp-deployments/tests/scribengin/integration-tests/Log_Sample_Chain.sh
```



###Launching Scribengin cluster manually###

In order to launch the dataflow, we expect that you read the document how to build the cluster with the docker script or digitalocean script. Basically you will need those server install and run:

1. Zookeeper
2. Kafka
3. Hadoop4. 
4. Elasticsearch
5. Build the NeverwinterDP release or download a NeverwinterDP release
6. Run vm-master and scribengin on top of hadoop yarn as an yarn application

Suppose that you already have zookeeper, hadoop, kafka and elasticsearch setup correctly

To launch the vm-master and scribengin master

````
  #Suppose you configure your cluster with zookeeper server with the name zookeeper-1 and hadoop master with 
  #the name hadoop-master. If not you have to edit the shell.sh script according to your server name
  #APP_OPT="$APP_OPT -Dshell.zk-connect=zookeeper-1:2181 -Dshell.hadoop-master=hadoop-master"

    
  #From release/neverwinterdp directory
  cd  path/release/neverwinterdp
    
  #To run the vm-master on top of hadoop yarn
  ./scribengin/bin/shell.sh vm start
  
  #To run the scribengin-master
  ./scribengin/bin/shell.sh scribengin start
  
  #To check the scribengin status
  ./scribengin/bin/shell.sh scribengin info
  
  
````

To launch the log-sample splitter dataflow

````
  #Suppose you configure your cluster with zookeeper server with the name zookeeper-1 and hadoop master with 
  #the name hadoop-master. If not you have to edit the run-splitter.sh script according to your server name
  #APP_OPT="$APP_OPT -Dshell.zk-connect=zookeeper-1:2181 -Dshell.hadoop-master=hadoop-master"

  #From release/neverwinterdp directory
  cd  path/release/neverwinterdp
    
  #To run the vm-master on top of hadoop yarn
  ./dataflow/log-sample/bin/run-splitter.sh
  
````

See dataflow-development-howto.md for more information about the log sample dataflow and more detail instructions