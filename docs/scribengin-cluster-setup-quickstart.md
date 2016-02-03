Scribengin Quickstart
=====================

#Contents#
1. [Overview](#general-steps-to-setup)
2. [Build NeverwinterDP](#build-neverwinterdp)
3. [Setup a cluster automatically in Docker](#docker-setup) 
4. [Launching Scribengin manually](#launching-scribengin-manually)

---

#General Steps To Setup#

1. You need to check out NeverwinterDP code and build
    * Check out NeverwinterDP from https://github.com/Nventdata/NeverwinterDP
    * Build Scribengin with gradle
2. Setup the scribengin cluster using Docker, Digital Ocean, or any VM provider
    * Install java and other requirement on the VMs
    * Update /etc/hosts so the VMs know about each other
    * Run Zookeeper, Hadoop, and YARN
    * Optionally run Kafka, Elasticsearch...

---

#Build NeverwinterDP#

Checkout NeverwinterDP 

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

Build and release the NeverwinterDP code

```
gradle clean build install release -x test
```

You will find the release, binaries, and shell scripts in ```NeverwinterDP/release/build/release/neverwinterdp```

You need to set the NEVERWINTERDP_HOME environment variable (optional) in order the other cluster script can build and deploy Scribengin automatically

```
export NEVERWINTERDP_HOME=/your/path/to/NeverwinterDP
```



---

#Docker Setup#
This will require access to Nvent's private repos.  Continue on to [Launching Scribengin cluster manually](#launching-scribengin-cluster-manually) if you do not have access. 

The following steps will deploy all the necessary components to run Scribengin locally by using Docker.

###Prerequisites###

1. [Install Ansible](http://docs.ansible.com/ansible/intro_installation.html)
2. [Install and configure Docker](https://docs.docker.com/engine/installation/)
3. [Install Gradle](https://docs.gradle.org/current/userguide/installation.html)
4. Install Java 7 
5. Install Python 2.7
6. Make sure the user you are running as has write permissions for /etc/hosts
    * Setup scripts will update your /etc/hosts file, but will not remove any entries that are already there
7. Setup your SSH config

    ```
       echo -e "Host *\n  StrictHostKeyChecking no" >> ~/.ssh/config
    ``` 
    
8. If you want to work with S3, set up your credentials file in this format    

     `````
     user@machine $ cat ~/.aws/credentials
     [default]
     aws_access_key_id=XXXXX
     aws_secret_access_key=YYYYYY
     ````` 

###Launching Scribengin cluster in Docker Using neverwinterdp-deployments###
1. Clone deployments and tools repo
        
        git clone git clone https://<bitbucket_user>@bitbucket.org/nventdata/neverwinterdp-deployments.git

2. Set up for neverwinter tools
        
        #Run the setup script for tools (only necessary ONCE)
        sudo ./neverwinterdp-deployments/tools/cluster/setup.sh

3. Build docker image with scribengin in one step
        
        #Build images, launch containers, run ansible
        ./neverwinterdp-deployments/docker/scribengin/docker.sh  cluster --launch

        #If you decided not to set NEVERWINTERDP_HOME, then you can pass it in manually here
        ./neverwinterdp-deployments/docker/scribengin/docker.sh  cluster --launch --neverwinterdp-home=/your/path/to/NeverwinterDP

4. If you wish to DESTROY your cluster (clean images and containers)
        
        ./neverwinterdp-deployments/docker/scribengin/docker.sh cluster --clean-containers --clean-image

5. Checking the status of your cluster
        

        $> ./tools/statusCommander/statusCommander.py
        Role           Hostname         IP           ProcessIdentifier       ProcessID    Status
        -------------  ---------------  -----------  ----------------------  -----------  --------
        monitoring     monitoring-1     172.17.0.11
                                                     /opt/kibana/bin/kibana  1910         Running
        hadoop_master  hadoop-master    172.17.0.2
                                                     SecondaryNameNode       1261         Running
                                                     ResourceManager         1376         Running
                                                     NameNode                1179         Running
        zookeeper      zookeeper-1      172.17.0.6
                                                     QuorumPeerMain          622          Running
        kafka          kafka-3          172.17.0.9
                                                     Kafka                   686          Running
                       kafka-2          172.17.0.8
                                                     Kafka                   684          Running
                       kafka-1          172.17.0.7
                                                     Kafka                   684          Running
        hadoop_worker  hadoop-worker-3  172.17.0.5
                                                     DataNode                1179         Running
                                                     NodeManager             1261         Running
                       hadoop-worker-2  172.17.0.4
                                                     DataNode                1177         Running
                                                     NodeManager             1259         Running
                                                     vm-master-1             3898         Running
                       hadoop-worker-1  172.17.0.3
                                                     DataNode                1179         Running
                                                     NodeManager             1261         Running
        elasticsearch  elasticsearch-1  172.17.0.10
                                                     Main                    442          Running


###Navigate to Kibana to view real time metrics###

```
Point your browser to http://monitoring-1:5601
You can change the interval at which Kibana refreshes itself in the top panel, or manually refresh the page
```

###SSH onto a cluster node###
```
#neverwinterdp user has sudo permissions
ssh neverwinterdp@[node-name]
```

###Launching a dataflow from a preconfigured test###

```
#The kafka test is a simple, quick test 
./tests/scribengin/tracking/integration/kafka-run-test.sh
    
#The kafka stability test is a more complicated, longer running test
./tests/scribengin/tracking/stability/stability-kafka-test.sh
```

###Getting status of a running dataflow
```
./scribengin/bin/shell.sh plugin com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingMonitor \
   --dataflow-id [DATAFLOW NAME] \
   --report-path /applications/tracking-sample/reports \
   --max-runtime 0 \
   --print-period 15000 \
   --show-history-workers
```

---

#Launching Scribengin manually#

###Prerequisite services to configure and launch###

1. Zookeeper
2. Hadoop4
3. YARN
4. Elasticsearch
5. Kafka


###To launch###

1.  Follow [instructions to build and release Scribengin](#check-out-and-build-neverwinterdp-code)
2.  Launch the VM Master in YARN
        
        #After building, you'll need to edit the file:
        #NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh
            #-Dshell.zk-connect    - [hostname]:[port] of your Zookeeper server
            #-Dshell.hadoop-master - [hostname] of your master Hadoop node
        APP_OPT="$APP_OPT -Dshell.zk-connect=zookeeper-1:2181 -Dshell.hadoop-master=hadoop-master"

          
        #From release/neverwinterdp directory
        cd  path/release/neverwinterdp
          
        #To run the vm-master on top of hadoop yarn
        ./scribengin/bin/shell.sh vm start
        
        
        #To check the scribengin status
        ./scribengin/bin/shell.sh vm info

3.  Upload your compiled dataflow to HDFS
        
        ./scribengin/bin/shell.sh vm upload-app --local $APP_DIR --dfs $DFS_APP_HOME

4.  Deploy your dataflow by submitting the app to YARN
        
        ./scribengin/bin/shell.sh dataflow submit \
            --dfs-app-home $DFS_APP_HOME \
            --dataflow-config $DATAFLOW_DESCRIPTOR_FILE \
            --dataflow-id [DATAFLOW NAME] \
            --dataflow-max-runtime $DATAFLOW_MAX_RUNTIME  \
            --dataflow-num-of-worker $DATAFLOW_NUM_OF_WORKER \
            --dataflow-num-of-executor-per-worker $DATAFLOW_NUM_OF_EXECUTOR_PER_WORKER \
            --dataflow-worker-enable-gc  \
            --wait-for-running-timeout 180000









  
