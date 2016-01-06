Aribtrary Cluster Guide
=======================

So you're too cool for school and you want to set up a cluster in a currently non-automated environment (i.e. in-house cluster, AWS, etc).  Great! Here's how to do it!

You'll need access to Nvent's private repo's to set this up.


#Required Machines

You'll need to set up machines in the cluster

- hadoop-master
  - Will run all hadoop master processes
  - Only one node of this is required
- hadoop-worker-*
  - You will need as many worker machines as you want nodes in your YARN cluster
  - We suggest at least 3 workers
- elasticsearch-*
  - Handles receiving logs and metric information
  - We suggest at least 1 elasticsearch node
- zookeeper-*
  - Runs zookeeper quorum
  - We suggest at least 1 Zookeeper node
- kafka-*
  - Runs Kafka cluster
  - If you plan on running dataflows using Kafka (you will), you'll need a Kafka cluster
  - We suggest 3-5 kafka nodes
- monitoring-*
  - This is where kibana and ganglia will live
  - Only one node is required

#User set up

1. Create a user named neverwinterdp on ALL machines.  It requires passwordless sudo.

        useradd -m -d /home/neverwinterdp -s /bin/bash -c "neverwinterdp user" -p $(openssl passwd -1 neverwinterdp)  neverwinterdp 
        echo "neverwinterdp ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers 
        echo "root ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers 
        chown -R neverwinterdp:neverwinterdp /opt  
        mkdir -p /home/neverwinterdp/.ssh 
        chown -R neverwinterdp:neverwinterdp /home/neverwinterdp/.ssh
        chmod 700 ~/.ssh
        

2. You can now upload your own key pair, or create a new one to all the machine's ~/.ssh directory

#Setting up the hosts file

Setting up /etc/hosts will make your life easier.

###Machine Naming conventions
  
  Aptly name the nodes in your cluster.  
  - hadoop-master
    - Does not need to have a number after the name
    - Should only be named "hadoop-master"
  - All other nodes
    - Must have a sequential, numeric ID
    - I.E. - kafka-1, kafka-2, kafka-3...

###The Hostfile

The hostfile on all machines in the cluster must also have this entry.  

If you plan on running tests/launching dataflows from your local machine, also include this entry in your own /etc/hosts file

        #Insert an entry like this into all the cluster machine's /etc/hosts file
        #Insert onto your local dev box if you wish to run anything from your local machine as well
        ##SCRIBENGIN CLUSTER START##
        10.0.0.1 hadoop-master
        10.0.0.2 elasticsearch-1 
        10.0.0.3 hadoop-worker-1
        10.0.0.4 hadoop-worker-2
        10.0.0.5 hadoop-worker-3
        10.0.0.6 zookeeper-1 
        10.0.0.7 monitoring-1
        hosting.server.kafka1.com kafka-1
        hosting.server.kafka2.com kafka-2
        hosting.server.kafka3.com kafka3
        ##SCRIBENGIN CLUSTER END##





#Setting Up Your Inventory File

Our automation is based on ansible inventory files.  Your file should look something like the following.  Make sure id's are set correctly, as well as the path to the ssh key you'll be using to access these machines.

```
[monitoring]
monitoring-1 ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.1 id=1

[kafka]
kafka-1 ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.2 id=1
kafka-2 ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.3 id=2
kafka-3 ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.4 id=3

[hadoop_master]
hadoop-master ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.5 id=1

[hadoop_worker]
hadoop-worker-1 ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.6 id=1
hadoop-worker-2 ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.7 id=2
hadoop-worker-3 ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.8 id=3

[zookeeper]
zookeeper-1 ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.9 id=1

[elasticsearch]
elasticsearch-1 ansible_ssh_user=neverwinterdp ansible_ssh_private_key_file=~/.ssh/id_rsa ansible_host=10.0.0.10 id=1
```


#Launching
The following steps will deploy all the necessary components to run Scribengin to the machines specified in your inventory file.

1. Clone deployments and tools repo
        
        git clone git clone https://<bitbucket_user>@bitbucket.org/nventdata/neverwinterdp-deployments.git

2. Set up for neverwinter tools
        
        #Run the setup script for tools (only necessary ONCE)
        sudo ./neverwinterdp-deployments/tools/cluster/setup.sh


3. Install Scribengin and necessary cluster services
        
        ./neverwinterdp-deployments/tools/serviceCommander/serviceCommander.py \ 
          --cluster --install --configure --clean --start \
          --profile-type stability \
          --inventory-file [path to your inventory]

4. Now you're ready to launch your dataflow!  Check the [quickstart guide](scribengin-cluster-setup-quickstart.md#launching-scribengin) for instructions on that

