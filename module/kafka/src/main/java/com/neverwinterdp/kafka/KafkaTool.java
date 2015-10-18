package com.neverwinterdp.kafka;

import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.asScalaMap;
import static scala.collection.JavaConversions.asScalaSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;

import kafka.admin.AdminUtils;
import kafka.admin.PreferredReplicaLeaderElectionCommand;
import kafka.admin.ReassignPartitionsCommand;
import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.common.TopicAndPartition;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.collection.Seq;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.Set;

public class KafkaTool  {
  private String name;
  private String zkConnects;
  
  public KafkaTool(String name, String zkConnects) {
    this.name = name;
    this.zkConnects = zkConnects;
  }

  public void createTopic(String topicName, int numOfReplication, int numPartitions) throws Exception {
    String[] args = { 
      "--create",
      "--topic", topicName,
      "--partition", String.valueOf(numPartitions),
      "--replication-factor", String.valueOf(numOfReplication),
      "--zookeeper", zkConnects
    };
    createTopic(args);
  }
  
  /**
   * Create a topic. This method will not create a topic that is currently scheduled for deletion.
   * For valid configs see https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools#Replicationtools-Howtousethetool?.3
   *
   * @See https://kafka.apache.org/documentation.html#topic-config 
   * for more valid configs
   * */
  public void createTopic(String[] args) throws Exception {
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;

    TopicCommandOptions options = new TopicCommandOptions(args);
    ZkClient client = 
        new ZkClient(zkConnects, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    if (topicExits(name)) {
      TopicCommand.deleteTopic(client, options);
    }

    TopicCommand.createTopic(client, options);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    client.close();
  }

  /**
   * This method works if cluster has "delete.topic.enable" = "true".
   * It can also be implemented by TopicCommand.deleteTopic which simply calls AdminUtils.delete 
   *
   * @param topicName
   * @throws Exception
   */
  public void deleteTopic(String topicName) throws Exception {
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkClient zkClient = new ZkClient(zkConnects, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    AdminUtils.deleteTopic(zkClient, topicName);
    zkClient.close();
  }

  /**
   * Returns true if topic path exists in zk.
   * Warns user if topic is scheduled for deletion.
   * 
   * @See http://search-hadoop.com/m/4TaT4VWNg8/v=plain
   * */
  public boolean topicExits(String topicName) throws Exception {
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkClient zkClient = new ZkClient(zkConnects, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    boolean exists = AdminUtils.topicExists(zkClient, topicName);
    if (exists && ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topicName))) {
      System.err.println("Topic "+topicName+" exists but is scheduled for deletion!");
    }
    zkClient.close();
    return exists;
  }
  
  // Move Leader to first broker in ISR
  // https://kafka.apache.org/documentation.html#basic_ops_leader_balancing
  // https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools#Replicationtools-2.PreferredReplicaLeaderElectionTool
  public void moveLeaderToPreferredReplica(String topic, int partition) {
    ZkClient client = new ZkClient(zkConnects, 10000, 10000, ZKStringSerializer$.MODULE$);

    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    // move leader to broker 1
    Set<TopicAndPartition> topicsAndPartitions = asScalaSet(Collections.singleton(topicAndPartition));
    PreferredReplicaLeaderElectionCommand commands = new PreferredReplicaLeaderElectionCommand(client, topicsAndPartitions);
    commands.moveLeaderToPreferredReplica();
    client.close();
  }

  /**
   * Re-assign topic/partition to remainingBrokers
   * Remaining brokers is a list of id's of the brokers where the topic/partition is to be moved to.
   * 
   *   Thus if remainingBrokers = [1,2] the topic will be moved to brokers 1 and 2 
   *   
   *   @see https://kafka.apache.org/documentation.html#basic_ops_cluster_expansion
   *   @see https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools#Replicationtools-6.ReassignPartitionsTool
   * */
  public boolean reassignPartition(String topic, int partition, List<Object> remainingBrokers) {
    ZkClient client = new ZkClient(zkConnects, 10000, 10000, ZKStringSerializer$.MODULE$);

    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

    Buffer<Object> seqs = asScalaBuffer(remainingBrokers);
    Map<TopicAndPartition, Seq<Object>> map = new HashMap<>();
    map.put(topicAndPartition, seqs);
    scala.collection.mutable.Map<TopicAndPartition, Seq<Object>> x = asScalaMap(map);
    ReassignPartitionsCommand command = new ReassignPartitionsCommand(client, x);

    return command.reassignPartitions();
  }
  
  public boolean reassignPartitionReplicas(String topic, int partition, Integer ... brokerId) {
    ZkClient client = new ZkClient(zkConnects, 10000, 10000, ZKStringSerializer$.MODULE$);
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

    Buffer<Object> seqs = asScalaBuffer(Arrays.asList((Object[])brokerId));
    Map<TopicAndPartition, Seq<Object>> map = new HashMap<>();
    map.put(topicAndPartition, seqs);
    ReassignPartitionsCommand command = new ReassignPartitionsCommand(client, asScalaMap(map));
    return command.reassignPartitions();
  }
}
