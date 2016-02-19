package com.neverwinterdp.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.annotation.PreDestroy;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import com.google.inject.Singleton;
import com.neverwinterdp.zookeeper.ZKClient;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

@Singleton
public class KafkaTool  {
  private String name;
  private String zkConnects;
  private ZKClient zkClient;
  
  public KafkaTool(String name, String zkConnects) throws Exception {
    this.name = name;
    this.zkConnects = zkConnects;
    zkClient = new ZKClient(zkConnects);
    zkClient.connect(15000);
  }
  
  @PreDestroy
  public void onDestroy() throws Exception {
    close();
  }

  public void close() throws Exception {
    zkClient.close();
  }

  public String getZkConnects() { return this.zkConnects ; }
  
  public KafkaAdminTool getKafkaTool() { return new KafkaAdminTool(name, zkConnects); }

  public String getKafkaBrokerList() throws Exception {
    StringBuilder b = new StringBuilder();
    List<BrokerRegistration> registrations = getBrokerRegistration();
    for (int i = 0; i < registrations.size(); i++) {
      BrokerRegistration registration = registrations.get(i);
      if (i > 0) b.append(",");
      b.append(registration.getHost()).append(":").append(registration.getPort());
    }
    return b.toString();
  }

  public List<BrokerRegistration> getBrokerRegistration() throws Exception {
    ZKClient.Operation<List<BrokerRegistration>> op = new ZKClient.Operation<List<BrokerRegistration>>()  {
      @Override
      public List<BrokerRegistration> execute(ZooKeeper zkClient) throws InterruptedException, KeeperException {
        List<String> ids = zkClient.getChildren("/brokers/ids", false);
        List<BrokerRegistration> holder = new ArrayList<BrokerRegistration>();
        for (int i = 0; i < ids.size(); i++) {
          String brokerId = ids.get(i);
          BrokerRegistration registration = getDataAs(zkClient, "/brokers/ids/" + brokerId, BrokerRegistration.class);
          registration.setBrokerId(brokerId);
          holder.add(registration);
        }
        return holder;
      }
    };
    List<BrokerRegistration> result = zkClient.execute(op, 3);
    return result;
  }

  public TopicMetadata findTopicMetadata(final String topic) throws Exception {
    return findTopicMetadata(topic, 3);
  }

  public TopicMetadata findTopicMetadata(final String topic, int retries) throws Exception {
    SimpleConsumerOperation<TopicMetadata> findTopicOperation = new SimpleConsumerOperation<TopicMetadata>() {
      @Override
      public TopicMetadata execute(SimpleConsumer consumer) throws Exception {
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        TopicMetadataResponse resp = consumer.send(req);
        List<TopicMetadata> topicMetadatas = resp.topicsMetadata();
        if (topicMetadatas.size() != 1) {
          throw new Exception("Expect to find 1 topic " + topic + ", but found " + topicMetadatas.size());
        }
        TopicMetadata tMetadata = topicMetadatas.get(0);
        if(tMetadata.partitionsMetadata().size() == 0) {
          throw new Exception("Found the topic " + topic + ", but the partition metadata is empty!");
        }
        return tMetadata;
      }
    };
    return execute(findTopicOperation, retries);
  }

  public PartitionMetadata findPartitionMetadata(String topic, int partition) throws Exception {
    TopicMetadata topicMetadata = findTopicMetadata(topic);
    for (PartitionMetadata sel : topicMetadata.partitionsMetadata()) {
      if (sel.partitionId() == partition)
        return sel;
    }
    return null;
  }

  private SimpleConsumer nextConsumer() throws Exception {
    List<BrokerRegistration> registrations = getBrokerRegistration();
    if(registrations.size() == 0) return null;
    Random random = new Random();
    BrokerRegistration registration = registrations.get(random.nextInt(registrations.size()));
    return new SimpleConsumer(registration.getHost(), registration.getPort(), 100000, 64 * 1024, name /*clientId*/);
  }
  
  <T> T execute(SimpleConsumerOperation<T> op, int retries) throws Exception {
    Exception error = null ;
    for(int i = 0; i < retries; i++) {
      SimpleConsumer consumer = null;
      try {
        consumer = nextConsumer();
        return op.execute(consumer);
      } catch(Exception ex) {
        error = ex;
      } finally {
        if(consumer != null) consumer.close();
      }
    }
    throw error ;
  }
  
  static interface SimpleConsumerOperation<T> {
    public T execute(SimpleConsumer consumer) throws Exception;
  }
}
