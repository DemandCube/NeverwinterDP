Scribengin Quickstart
=====================

#Contents#
1. [Overview](#general-steps-to-setup)
2. [Build NeverwinterDP](#build-neverwinterdp)
3. [Automation Prerequisites](#automation-prerequisites)
3. [Setup a cluster automatically in Docker](#docker-setup) 
4. [Setup a cluster automatically in Digital Ocean](#digital-ocean-setup) 
5. [Setup a cluster automatically somewhere else](#arbitrary-cluster-setup) 
4. [Launching Scribengin manually in an already setup YARN cluster ](#manually-launching)
5. [Launching a Datafow from a preconfigured test](#launching-a-dataflow-from-a-preconfigured-test)
5. [Monitoring Scribengin](#monitoring-scribengin)

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
3. Launch the VM Master (Scribengin's YARN Application Master)
4. Submit your dataflow

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
#Automation Prerequisites#

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


#Docker Setup#
This will require access to Nvent's private repos.  Continue on to [Launching Scribengin cluster manually](#manually-launching) if you do not have access. 

The following steps will deploy all the necessary components to run Scribengin locally by using Docker.

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


#Digital Ocean Setup#
This will require access to Nvent's private repos.  Continue on to [Launching Scribengin cluster manually](#manually-launching) if you do not have access. 

The following steps will deploy all the necessary components to run Scribengin in the cloud via Digital Ocean.  You'll also need a Digital Ocean account and a Digital Ocean token (see step 3)

1. Clone deployments and tools repo
        
        git clone git clone https://<bitbucket_user>@bitbucket.org/nventdata/neverwinterdp-deployments.git

2. Set up for neverwinter tools
        
        #Run the setup script for tools (only necessary ONCE)
        sudo ./neverwinterdp-deployments/tools/cluster/setup.sh

3. Set up your Digital Ocean token

        #To get a token visit - 
        #  https://cloud.digitalocean.com/settings/applications#access-tokens
        echo "TOKENGOESHERE" > ~/.digitaloceantoken


4. Run the Digital Ocean automation
        
        cd ./neverwinterdp-deployments/tools/

        ./cluster/clusterCommander.py \
          digitalocean \
          --launch --neverwinterdp-home $NEVERWINTERDP_HOME \
          --ansible-inventory \
          --create-containers $ROOT/ansible/profile/stability.yml \
          --subdomain $SUBDOMAIN --region nyc3

5. Install Scribengin and necessary cluster services
        
        ./serviceCommander/serviceCommander.py \ 
          --cluster --install --configure --profile-type stability

#Arbitrary Cluster Setup
[Follow the steps in this guide for information on how to use Nvent's private automation to launch in any arbitrary cluster](arbitrary-cluster-guide.md)

#Manually Launching#

These steps will be necessary if you do not have access to Nvent's private automation repo's

###Prerequisites

1. Set up at least one Hadoop4 node
2. Set up YARN on Hadoop
3. Set up at least one Zookeeper node
4. Set up at least one ElasticSearch node
5. Set up any machines for sources/sinks (i.e. Kafka, etc)
6. Create a user on all the machines (These tweaks make things run smoothly without interruption)
  - username: neverwinterdp
  - ssh keys set up in ~/.ssh
  - has passwordless sudo
7. Set up all node's /etc/hosts file.  See [Machine Naming Conventions Below](#machine-naming-conventions)

        ##SCRIBENGIN CLUSTER START##
        10.0.0.1 elasticsearch-1 
        10.0.0.2 hadoop-master
        10.0.0.3 hadoop-worker-1
        10.0.0.4 hadoop-worker-2
        10.0.0.5 hadoop-worker-3
        10.0.0.6 zookeeper-1 
        ##SCRIBENGIN CLUSTER END##


###Machine Naming conventions
  
  We strongly suggest aptly naming the nodes in your cluster.

  - hadoop-master
    - Only one node of this is required
    - Runs Hadoop and YARN master processes
    - Needs to run
      - SecondaryNameNode
      - ResourceManager
      - NameNode
    - Since there is only ONE master, no sequential naming
  - hadoop-worker-*
    - Running Hadoop and YARN slave processes
    - Named sequentially, i.e.
      - hadoop-worker-1
      - hadoop-worker-2
      - etc...
    - Needs to run
      - DataNode
      - NodeManager
  - elasticsearch-*
    - Running elasticsearch
    - Named sequentially
    - Handles receiving logs and metric information
  - zookeeper-*
    - Runs zookeeper quorum
    - Named sequentially

###Launching Scribengin

1. Make sure you have the JAVA_HOME environment variable correctly set
2. [Build NeverwinterDP](#build-neverwinterdp)
3.  Launch the VM Master in YARN
        
        #After building, if you didn't edit your /etc/hosts file, you'll need to edit the file:
        #NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh
            #-Dshell.zk-connect    - [hostname]:[port] of your Zookeeper server
            #-Dshell.hadoop-master - [hostname] of your master Hadoop node
        APP_OPT="$APP_OPT -Dshell.zk-connect=zookeeper-1:2181 -Dshell.hadoop-master=hadoop-master"

          
        #From release/neverwinterdp directory
        cd  NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/
          
        #To run the vm-master on top of hadoop yarn
        ./shell.sh vm start
        
        
        #To check the scribengin status
        ./shell.sh vm info

4.  [Use the Scribengin API to upload your Dataflow.](dataflowSubmission.md)


#Launching a dataflow from a preconfigured test#

This test can be launched manually from the public NeverwinterDP repo
```
./NeverwinterDP/release/build/release/neverwinterdp/dataflow/tracking-sample/bin/run-tracking.sh
```

These tests are in Nvent's private automation repo

```
#The kafka test is a simple, quick test 
./neverwinterdp-deployments/tests/scribengin/tracking/integration/kafka-run-test.sh
    
#The kafka stability test is a more complicated, longer running test
./neverwinterdp-deployments/tests/scribengin/tracking/stability/stability-kafka-test.sh
```


#Monitoring Scribengin

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


###Getting status of a running dataflow
```
#After you launch a dataflow on the command line, it will give you a command
# you can run to monitor the dataflow.  It will look similar to this.
./scribengin/bin/shell.sh plugin com.neverwinterdp.scribengin.dataflow.tool.tracking.TrackingMonitor \
   --dataflow-id [DATAFLOW NAME] \
   --report-path /applications/tracking-sample/reports \
   --max-runtime 0 \
   --print-period 15000 \
   --show-history-workers
```










  
