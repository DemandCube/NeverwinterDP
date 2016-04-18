#!/bin/bash
set -e

function h1() {
  echo ""
  echo "###########################################################################################################"
  echo "$@"
  echo "###########################################################################################################"
}

function has_opt() {
  OPT_NAME=$1
  shift
  #Par the parameters
  for i in "$@"; do
    if [[ $i == $OPT_NAME ]] ; then
      echo "true"
      return
    fi
  done
  echo "false"
}

function get_opt() {
  OPT_NAME=$1
  DEFAULT_VALUE=$2
  shift
  
  #Par the parameters
  for i in "$@"; do
    index=$(($index+1))
    if [[ $i == $OPT_NAME* ]] ; then
      value="${i#*=}"
      echo "$value"
      return
    fi
  done
  echo $DEFAULT_VALUE
}

function login_ssh_port() {
  docker port $1 22 | sed 's/.*://'
}

function ip_route() {
  if [[ $OSTYPE == *"darwin"* ]] ; then
    h1 "Updating route for OSX"
    if [ -f /usr/local/bin/boot2docker ]; then
       HOST_IP=$(boot2docker ip)
    else
       HOST_IP=$(docker-machine ip default)
    fi
    sudo route -n add 172.17.0.0/16 $HOST_IP
  fi
}

function container_update_hosts() {
  h1 "Update /etc/hosts file on containers"
  HOSTS=$'## scribengin server ##\n'
  HOSTS+=$'127.0.0.1 localhost\n\n'
  images=( $(docker ps -a | grep scribengin | awk '{print $NF}') )
  for image in "${images[@]}" ; do
    #extract the container name
    container_name=$(docker inspect -f {{.Config.Hostname}} $image)
    container_domain=$(docker inspect -f {{.Config.Domainname}} $image)
    #extract the container ip
    container_ip=$(docker inspect -f "{{ .NetworkSettings.IPAddress }}" $image)
    #extract the container running state
    container_running=$(docker inspect -f {{.State.Running}} $image)
    HOSTS+="$container_ip $container_name"
    HOSTS+=$'\n'
    #echo "container id = $image, container name = $container_name, container ip = $container_ip, container running = $container_running"
  done

  echo ""
  echo "Insert Content:"
  echo ""
  echo "-----------------------------------------------------------------------------------------------"
  echo "$HOSTS"
  echo "-----------------------------------------------------------------------------------------------"
  for image in "${images[@]}" ; do
    #extract the container name
    container_name=$(docker inspect -f {{.Config.Hostname}} $image)
    
    #Update ssh key while we're at it
    echo "ssh-keygen -R $container_name"
    ssh-keygen -R $container_name
    
    echo "Update /etc/hosts for $container_name"
    ssh -o StrictHostKeyChecking=no -p $(login_ssh_port $image) root@$HOST_IP "echo '$HOSTS'  > /etc/hosts"
  done
}

function host_machine_update_hosts() {
  h1 "Updating host machine's /etc/hosts file"
  #Updating /etc/hosts file on host machine
  h1 "Updating /etc/hosts file of host machine"
  
  startString="##SCRIBENGIN CLUSTER START##"
  endString="##SCRIBENGIN CLUSTER END##"
  
  #Build entry to add to /etc/hosts by reading info from Docker
  hostString="$startString\n"
  images=( $(docker ps -a | grep scribengin | awk '{print $NF}') )
  for image in "${images[@]}" ; do
    hostname=$(docker inspect -f '{{ .Config.Hostname }}' $image)
    hostString="$hostString$(docker inspect -f "{{ .NetworkSettings.IPAddress }}" $hostname)    $hostname\n"
  done
  hostString="$hostString$endString\n"
  
  if [ ! -f /etc/hosts ] ; then
    echo -e "\nERROR!!!\nYou don't have write permissions for /etc/hosts!!!\nManually add this to your /etc/hosts file:"
    echo -e "$hostString"
    return
  fi
  
  #Strip out old entry
  out=`sed "/$startString/,/$endString/d" /etc/hosts`
  #write new hosts file
  echo -e "$out\n$hostString" > /etc/hosts
  echo -e "$hostString"
  
  ip_route
}


function clean_image() {
  OS_TYPE=$(get_opt --os-type 'centos' $@)
  h1 "Clean the images"
  images=( $(docker images -a | grep -i $OS_TYPE-scribengin |  awk '{print $1 ":" $2}') )
  for image in "${images[@]}" ; do
    docker rmi -f $image
  done
}

function build_images() {
  h1 "Building Images"
  
  OS_TYPE=$(get_opt --os-type 'centos' $@)
  
  #Direcotry this script is in
  DOCKERSCRIBEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
  DOCKERSCRIBEDIR=$DOCKERSCRIBEDIR/dockerfile/$OS_TYPE
  if [ ! -d $DOCKERSCRIBEDIR/tmp ]; then
    mkdir $DOCKERSCRIBEDIR/tmp
  fi
  
  cp ~/.ssh/id_rsa      $DOCKERSCRIBEDIR/tmp/id_rsa
  cp ~/.ssh/id_rsa.pub  $DOCKERSCRIBEDIR/tmp/id_rsa.pub
  cp ~/.ssh/id_rsa.pub $DOCKERSCRIBEDIR/tmp/authorized_keys
  
  if [ ! -d $DOCKERSCRIBEDIR/tmp ]; then
    mkdir $DOCKERSCRIBEDIR/tmp
  fi
  cp ~/.ssh/id_rsa.pub $DOCKERSCRIBEDIR/tmp/authorized_keys

  #Build the systemd image first
  #docker build --rm -t local/c7-systemd $DOCKERSCRIBEDIR/systemd/

  #Build the scribengin image next
  docker build -t $OS_TYPE:scribengin $DOCKERSCRIBEDIR


  #Install common dependencies  
  docker run -d -p 22 --privileged -h scribengincommon --name scribengincommon  $OS_TYPE:scribengin
  SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
  TMP_INVENTORY=/tmp/commoninventory
  echo $(docker inspect -f "{{ .NetworkSettings.IPAddress }}" scribengincommon) > $TMP_INVENTORY
  ip_route
  $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py -e "common" --install -i $TMP_INVENTORY
  containerID=$(docker ps -a | grep scribengincommon | awk '{print $1}')
  docker commit $containerID $OS_TYPE:scribengin
  docker rm -f $containerID
  rm /tmp/commoninventory
  
  rm -rf $DOCKERSCRIBEDIR/tmp
  
  launch_intermediate_containers $@
}

function clean_containers() {
  h1 "Cleaning Containers"
  containers=( $(docker ps -a | grep scribengin | awk '{print $NF}') )
  for (( i=0; i<${#containers[@]}; i=$i+1 )); do
    #Removes trailing hyphen and digits
    image_name=${containers[i]}
    docker rm -f $image_name
  done
}

function launch_intermediate_containers() {
  OS_TYPE=$(get_opt --os-type 'centos' $@)
  
  h1 "Launching intermediate containers for final image configuration"
  
  #Launch 1 of each kind of container
  h1 "Launch intermediate hadoop-master container"
  docker run -d -p 22 -p 50070 -p 9000 -p 8030 -p 8032 -p 8088 --privileged -h hadoop-master --name hadoop-master  $OS_TYPE:scribengin
  
  h1 "Launch intermediate hadoop-worker container"
  docker run -d -p 22 --privileged -h hadoop-worker --name hadoop-worker $OS_TYPE:scribengin
  
  h1 "Launch intermediate zookeeper container"
  docker run -d -p 22 -p 2181 --privileged -h zookeeper --name zookeeper  $OS_TYPE:scribengin

  h1 "Launch intermediate kafka container"
  docker run -d -p 22 -p 9092 --privileged -h kafka --name kafka  $OS_TYPE:scribengin
  
  h1 "Launch intermediate elasticsearch container"
  docker run -d -p 22 -p 9300 --privileged -h elasticsearch --name elasticsearch  $OS_TYPE:scribengin
  
  h1 "Launch intermediate monitoring container"
  docker run -d -p 22 -p 3000 -p 5601:5601 --privileged -h monitoring --name monitoring $OS_TYPE:scribengin
  
  #Get those intermediate containers set up with ansible
  host_machine_update_hosts
  #container_update_hosts $@
  ansible_inventory $@ 
  
  deploy_all $@ 

  #Create base images for each container
  containers=( $(docker ps -a | grep scribengin | awk '{print $NF}') )
  for (( i=0; i<${#containers[@]}; i=$i+1 )); do
    #Removes trailing hyphen and digits
    image_name=${containers[i]}
    h1 "Creating base image for $image_name"
    docker commit $image_name $OS_TYPE-scribengin:$image_name
  done
  clean_containers $@
}

function launch_containers() {
  OS_TYPE=$(get_opt --os-type 'centos' $@)
  
  h1 "Launching Containers"
  #Checks to make sure images exist
  #If they don't exist, then create them
  scribeImageLine=$(docker images | grep scribengin)
  if [ "$scribeImageLine" == "" ] ; then
    clean_image $@
    build_images $@
  fi
  
  DISABLE_HADOOPMASTER=$(has_opt "--disable-hadoopmaster" $@ )
  NUM_KAFKA_BROKER=$(get_opt --kafka-server '3' $@)
  NUM_ZOOKEEPER_SERVER=$(get_opt --zk-server 1 $@)
  NUM_HADOOP_WORKER=$(get_opt --hadoop-worker 3 $@)
  
  NUM_ELASTICSEARCH_SERVER=$(get_opt --elasticsearch-server 1 $@)
  
  NUM_MONITORING=$(get_opt --monitoring-server 1 $@)
  
  if [ $DISABLE_HADOOPMASTER == "false" ] ; then
    h1 "Launch hadoop-master containers"
    docker run -d -p 22 -p 50070:50070 -p 9000:9000 -p 8030:8030 -p 8032:8032 -p 8088:8088 --privileged -h hadoop-master --name hadoop-master  $OS_TYPE-scribengin:hadoop-master
  fi
  
  
  h1 "Launch hadoop-worker containers"
  for (( i=1; i<="$NUM_HADOOP_WORKER"; i++ ))
  do
    NAME="hadoop-worker-"$i
    docker run -d -p 22 --privileged -h "$NAME" --name "$NAME" $OS_TYPE-scribengin:hadoop-worker
  done

  h1 "Launch zookeeper containers"
  for (( i=1; i<="$NUM_ZOOKEEPER_SERVER"; i++ ))
  do
    NAME="zookeeper-"$i
    PORT_NUM=`expr 2181 - 1 + $i`
    docker run -d -p 22 -p $PORT_NUM:2181 --privileged -h "$NAME" --name "$NAME"  $OS_TYPE-scribengin:zookeeper
  done  

  h1 "Launch kafka containers"
  for (( i=1; i<="$NUM_KAFKA_BROKER"; i++ ))
  do
    NAME="kafka-"$i
    docker run -d -p 22 -p 9092 --privileged -h "$NAME" --name "$NAME"  $OS_TYPE-scribengin:kafka
  done

  h1 "Launch elasticsearch containers"
  for (( i=1; i<="$NUM_ELASTICSEARCH_SERVER"; i++ ))
  do
    NAME="elasticsearch-"$i
    EXPOSE_PORT_9300=`expr 9300 - 1 + $i`
    EXPOSE_PORT_9200=`expr 9200 - 1 + $i`
    docker run -d -p 22 -p $EXPOSE_PORT_9300:9300 -p $EXPOSE_PORT_9200:9200 --privileged -h "$NAME" --name "$NAME"  $OS_TYPE-scribengin:elasticsearch
  done
  
  h1 "Launch monitoring containers"
  for (( i=1; i<="$NUM_MONITORING"; i++ ))
  do
    NAME="monitoring-"$i
    docker run -d -p 22 -p 3000:3000 -p 5601:5601 --privileged -h "$NAME" --name "$NAME"  $OS_TYPE-scribengin:monitoring
  done
  
  docker ps
}

function ansible_inventory(){
  h1 "Creating ansible inventory file"
  ANSIBLE_USER=$(get_opt    --ansible-user    'neverwinterdp' $@)
  ANSIBLE_SSH_KEY=$(get_opt --ansible-ssh-key '~/.ssh/id_rsa' $@)
  INVENTORY_FILE_LOCATION=$(get_opt --inventory-file-location '/tmp/scribengininventory_docker' $@)
  
  #list of container regex's - these must match the ansible group names
  regexList=("monitoring"
             "kafka"
             "hadoop-master"
             "hadoop-worker"
             "zookeeper"
             "elasticsearch")
  #File contents we'll write to $INVENTORY_FILE_LOCATION
  filecontents=""
  
  #Go through all docker images, see if it matches the regex, and add it to the inventory
  for regex in ${regexList[@]}; do
    #our ansible groups use _ instead of - character to help differentiate the groups from containers
    ansiblegroup=$(echo $regex | sed -e 's/-/_/g')
    #Write the ansible group header
    filecontents="$filecontents\n[$ansiblegroup]\n"
    id=0
    container_names=( $(docker ps -a | grep scribengin | awk '{print $NF}') )
    for container_name in "${container_names[@]}" ; do
      #If the hostname matches the header, then add it to the ansible group
      if [[ $container_name =~ $regex.* ]] ; then
        id=`expr $id + 1`
        IP=$(docker inspect -f "{{ .NetworkSettings.IPAddress }}" $container_name)
        filecontents="$filecontents$container_name ansible_ssh_user=$ANSIBLE_USER ansible_ssh_private_key_file=$ANSIBLE_SSH_KEY ansible_host=$IP id=$id\n"
      fi
    done
  done
  echo "Writing ansible inventory file to $INVENTORY_FILE_LOCATION"
  echo -e $filecontents > $INVENTORY_FILE_LOCATION
  echo "Writing ansible inventory file to ~/inventory"
  echo -e $filecontents > ~/inventory
  echo -e $filecontents
}

function deploy_all(){
  NEVERWINTERDP_HOME_OVERRIDE=$(get_opt --neverwinterdp-home '' $@)
  SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
  PROFILE_TYPE=$(get_opt --profile-type 'stability' $@)
  
  if [[ $NEVERWINTERDP_HOME_OVERRIDE == "" ]] ; then
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py --services "elasticsearch,zookeeper,kafka,hadoop,kibana" --install --configure --clean -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE
  else
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py --services "elasticsearch,zookeeper,kafka,hadoop,kibana" --install --configure --clean -i $INVENTORY_FILE_LOCATION --extra-vars "neverwinterdp_home_override=$NEVERWINTERDP_HOME_OVERRIDE" --profile-type $PROFILE_TYPE
  fi
}

function cluster(){
  SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
  
  CLEAN_IMAGE=$(has_opt "--clean-image" $@ )
  BUILD_IMAGE=$(has_opt "--build-image" $@ )
  CLEAN_CONTAINERS=$(has_opt "--clean-containers" $@ )
  RUN_CONTAINERS=$(has_opt "--run-containers" $@ )
  ANSIBLE_INVENTORY=$(has_opt "--ansible-inventory" $@ )
  DEPLOY=$(has_opt "--deploy" $@)
  DEPLOY_SCRIBENGIN=$(has_opt "--deploy-scribengin" $@)
  DEPLOY_TOOLS=$(has_opt "--deploy-tools" $@)
  CLEAN_CLUSTER=$(has_opt "--clean-cluster" $@ )
  STOP_CLUSTER=$(has_opt "--stop-cluster" $@ )
  FORCE_STOP_CLUSTER=$(has_opt "--force-stop-cluster" $@ )
  START=$(has_opt "--start" $@)
  LAUNCH=$(has_opt "--launch" $@ )
  #DEPLOY_KIBANA=$(has_opt "--deploy-kibana" $@)
  INVENTORY_FILE_LOCATION=$(get_opt --inventory-file-location '/tmp/scribengininventory' $@)
  NEVERWINTERDP_HOME_OVERRIDE=$(get_opt --neverwinterdp-home '' $@)
  PROFILE_TYPE=$(get_opt --profile-type 'stability' $@)
  SCRIBENGIN_PRE_START_SLEEP=$(get_opt --scribengin-pre-start-sleep '30' $@)

  
  if [ $CLEAN_CONTAINERS == "true" ] || [ $LAUNCH == "true" ] ; then
    clean_containers $@
  fi
  
  if [ $CLEAN_IMAGE == "true" ] || [ $LAUNCH == "true" ]  ; then
    clean_image $@
  fi
  
  if [ $BUILD_IMAGE == "true" ] || [ $LAUNCH == "true" ] ; then
    build_images $@
  fi
  
  
  if [ $RUN_CONTAINERS == "true" ] || [ $LAUNCH == "true" ] ; then
    launch_containers $@
    host_machine_update_hosts
    container_update_hosts $@
  fi
  
  if [ $ANSIBLE_INVENTORY == "true" ] || [ $LAUNCH == "true" ] ; then
    ansible_inventory $@
  fi
  
  if [ $DEPLOY == "true" ] ; then
    deploy_all $@
  fi
  
  if [ $DEPLOY_SCRIBENGIN == "true" ] || [ $LAUNCH == "true" ] ; then
    if [[ $NEVERWINTERDP_HOME_OVERRIDE == "" ]] ; then
      $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py -e "scribengin" --install -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE
    else
      $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py -e "scribengin" --install -i $INVENTORY_FILE_LOCATION --extra-vars "neverwinterdp_home_override=$NEVERWINTERDP_HOME_OVERRIDE" --profile-type $PROFILE_TYPE
    fi
  fi  
  
  if [ $DEPLOY_TOOLS == "true" ] || [ $LAUNCH == "true" ] ; then
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py -e "neverwinterdp_deployments" --install -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py -e "neverwinterdp_code" --install -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py -e "gradle" --install -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py -e "ansible" --install -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE
  fi
  
  if [ $STOP_CLUSTER == "true" ] ; then
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py --cluster --stop -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE
  fi
  
  if [ $FORCE_STOP_CLUSTER == "true" ] ; then
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py --cluster --force-stop -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE
  fi
  
  if [ $CLEAN_CLUSTER == "true" ] || [ $LAUNCH == "true" ] ; then
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py --cluster --clean -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE
  fi
  
  if [ $START == "true" ] || [ $LAUNCH == "true" ] ; then
    $SCRIPT_DIR/../../tools/serviceCommander/serviceCommander.py --cluster --configure --start -i $INVENTORY_FILE_LOCATION --profile-type $PROFILE_TYPE --scribengin-pre-start-sleep $SCRIBENGIN_PRE_START_SLEEP
  fi
}

function printUsage() {
  echo "Cluster command options: "
  echo "  Command \"image\" consists of the sub commands: "
  echo "      build                      : To build the ubuntu os image with the required components"
  echo "      clean                      : To remove the image"
  echo "    Examples:"
  echo "       ./docker.sh image build"
  echo "  Command \"container\" consists of the sub commands: "
  echo "      run                        : To run the containers(hadoop, zookeeper, kafka...)"
  echo "      clean                      : To remove and destroy all the running containers"
  echo "      update-hosts               : To update the /etc/hosts in all the running containers"
  echo "    Examples:"
  echo "       ./docker.sh container run"
  echo "  Command \"cluster\" consists of the following options: "
  echo "       Options: "
  echo "         --clean-image         : Cleans docker images"
  echo "         --build-image         : Builds the docker image for Scribengin"
  echo "         --clean-containers    : Cleans docker containers"
  echo "         --run-containers      : Runs docker containers"
  echo "         --ansible-inventory   : Creates ansible inventory file"
  echo "         --deploy              : Run ansible playbook"
  echo "         --deploy-scribengin   : Run ansible playbook to install Scribengin"
  echo "         --deploy-tools        : Run ansible playbook to install neverwinterdp-deployments"
  echo "         --start               : Starts services"
  echo "         --stop-cluster        : Stops cluster services"
  echo "         --force-stop-cluster  : Force stops cluster services"
  echo "         --neverwinterdp-home  : /Path/To/Neverwinterdp"
  echo "         --launch              : Cleans docker image and containers, Builds image and container, and starts"
  echo "         --kafka-server        : Number of kafka containers to start"
  echo "         --zk-server           : Number of zookeeper containers to start"
  echo "         --hadoop-worker       : Number of hadoop worker containers to start"
  echo "         --os-type             : Operating System distribution type [ubuntu,centos], default is ubuntu"
  echo "         --idle-kafka-brokers  : Number of ideal kafka containers initially"
  #echo "         --deploy-kibana       : Deploy kibana visualizations and dashboards"
  echo "    Examples:"
  echo "        ./docker.sh cluster [options]"
  echo "       ./docker.sh cluster --launch"
  echo "       ./docker.sh cluster --launch --neverwinterdp-home=/home/user/NeverwinterDP"
  echo "  Extra options for cluster --deploy: "
  echo "         --inventory-file-location : Path to ansible inventory file.                 Default='/tmp/scribengininventory'"
  echo "         --playbook-file-location  : Path to playbook to run.                        Default='SCRIPT_DIR/./../ansible/playbooks/scribenginCluster.yml'"
  echo "         --ansible-forks           : Number of number of parallel processes to use.  Default=10 "
  echo "         --neverwinterdp-home      : Path to NeverwinterDP home"
  echo "  Other commands:"
  echo "    ip-route                   : If you are running macos, use this command to route the 127.17.0.0 ip range to the boot2docker host. It allows to access the docker container directly from the MAC"
  echo "    ansible-inventory          : Creates ansible inventory file"
  
}


##########################################################
# Start script                                           #
##########################################################
HOST_IP="127.0.0.1"
OS=`uname`
if [[ "$OS" == 'Linux' ]]; then
   OS='linux'
elif [[ "$OS" == 'FreeBSD' ]]; then
   platform='freebsd'
elif [[ "$OS" == 'Darwin' ]]; then
  eval "$(docker-machine env default)"
  platform='macos'
  if [ -f /usr/local/bin/boot2docker ]; then
    HOST_IP=$(boot2docker ip)
  else
    HOST_IP=$(docker-machine ip default)
  fi
fi

BIN_DIR=`dirname "$0"`
BIN_DIR=`cd "$BIN_DIR"; pwd`

# get command
COMMAND=$1
shift
 
if [ "$COMMAND" = "image" ] ; then
  # get subcommand
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "build" ] ; then
    build_images $@
  elif [ "$SUB_COMMAND" = "clean" ] ; then
    clean_image $@
  else
    printUsage
  fi
elif [ "$COMMAND" = "container" ] ; then
  # get subcommand
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "clean" ] ; then
    clean_containers $@
  elif [ "$SUB_COMMAND" = "run" ] ; then
    launch_containers $@
    host_machine_update_hosts $@
    container_update_hosts $@
  elif [ "$SUB_COMMAND" = "update-hosts" ] ; then
    host_machine_update_hosts $@
    container_update_hosts $@
  else
    printUsage
  fi
elif [ "$COMMAND" = "deploy" ] ; then
  deploy_all $@
elif [ "$COMMAND" = "ansible-inventory" ] ; then
  ansible_inventory $@
elif [ "$COMMAND" = "cluster" ] ; then
  cluster $@
elif [ "$COMMAND" = "ip-route" ] ; then
  ip_route
elif [ "$COMMAND" = "update-hosts" ] ; then
  host_machine_update_hosts
else
  h1 "Docker Images"
  docker images
  h1 "Docker Running Containers"
  docker ps
  h1 "docker.sh usage"
  printUsage
fi
