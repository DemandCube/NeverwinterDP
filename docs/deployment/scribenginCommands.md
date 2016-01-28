Scribengin Shell Commands
=========================
#Contents#
1. [Intro](#intro)
2. [Dataflow Commands](#dataflow-commands)
3. [Registry Commands](#registry-commands)
4. [VM Commands](#vm-commands)

#Intro

##Configure your Zookeeper connection
Configure this environment variable to point to your Zookeeper node(s)
```
export SCRIBENGIN_REGISTRY_CONNECT=zookeeper-1:2181

#If you have more than one zookeeper node in your cluster, pass them in as a comma separated list
export SCRIBENGIN_REGISTRY_CONNECT=zookeeper-1:2181,zookeeper-2:2181,zookeeper-3:2181
```


##Find the shell script
To access the Scribengin shell, find shell.sh
```
NeverwinterDP/release/build/release/neverwinterdp/scribengin/bin/shell.sh
```

##To get a simple list of available commands
```
./shell.sh 
```

#Dataflow Commands

##Stop your dataflow

Stopped dataflows can be resumed.

```
./shell.sh dataflow stop --dataflow-id [dataflowID]
```

##Resume your dataflow
```
./shell.sh dataflow resume --dataflow-id [dataflowID]
```

##Get info on your dataflow
```
./shell.sh dataflow info --dataflow-id [dataflowID]

#To continuously print out information about your dataflow
./shell.sh dataflow monitor --dataflow-id [dataflowID] --dump-period 5000
```

##To wait until your dataflow is at a certain status
```
./shell.sh dataflow wait-for-status --dataflow-id [dataflowID] --status TERMINATED  --timeout 60000 

./shell.sh dataflow wait-for-status --dataflow-id [dataflowID] --status RUNNING --timeout 60000 
```

#Registry Commands

##Output the full contents of the registry
```
./shell.sh registry dump --max-print-data-length 120
```

##Get info about a specific registry node
```
./shell.sh registry info --path /scribengin/dataflows
```

#VM Commands

##Start the VMMaster

Refer to the [Quickstart guide](scribengin-cluster-setup-quickstart.md) for full instructions on how to start the VM Master (Scribengin's YARN Application Master)

```
./shell.sh vm start
```

##Shutdown the VMMaster
```
./shell.sh vm shutdown
```

##Get info on history and running VMs
```
./shell.sh vm info
```




