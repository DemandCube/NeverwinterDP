package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.dataflow.DataflowMessage;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.message.Message;

public class VMTMValidatorKafkaApp extends VMApp {
  private Logger logger;
  
  @Override
  public void run() throws Exception {
    logger =  getVM().getLoggerFactory().getLogger(VMTMValidatorKafkaApp.class) ;
    logger.info("Start run()");
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    VMConfig vmConfig = vmDescriptor.getVmConfig();
    Registry registry = getVM().getVMRegistry().getRegistry();
    
    String reportPath  = vmConfig.getProperty("tracking.report-path", "/applications/tracking-message");
    int    numOfReader = vmConfig.getPropertyAsInt("tracking.num-of-reader", 3);
    long   maxRuntime  = vmConfig.getPropertyAsLong("tracking.max-runtime", 120000);
    int    expectNumOfMessagePerChunk = vmConfig.getPropertyAsInt("tracking.expect-num-of-message-per-chunk", 0);
    
    String kafkaZkConnects         = vmConfig.getProperty("kafka.zk-connects", "zookeeper-1:2181");
    String kafkaTopic              = vmConfig.getProperty("kafka.topic", null);
    long   kafkaMessageWaitTimeout = vmConfig.getPropertyAsLong("kafka.message-wait-timeout", 600000);
    
    TrackingValidatorService validatorService = new TrackingValidatorService(registry, reportPath);
    validatorService.withExpectNumOfMessagePerChunk(expectNumOfMessagePerChunk);
    validatorService.addReader(
        new KafkaTrackingMessageReader(kafkaZkConnects, kafkaTopic, numOfReader, kafkaMessageWaitTimeout)
    );
    validatorService.start();
    validatorService.awaitForTermination(maxRuntime, TimeUnit.MILLISECONDS);
  }

  public class KafkaTrackingMessageReader extends TrackingMessageReader {
    private BlockingQueue<TrackingMessage> queue = new LinkedBlockingQueue<>(5000);
    
    private String                kafkaZkConnects;
    private String                topic;
    private long                  maxMessageWaitTime;
    KafkaTopicConnector           connector;
    
    KafkaTrackingMessageReader(String kafkaZkConnects, String topic, int numOfThread, long maxMessageWaitTime) {
      this.kafkaZkConnects = kafkaZkConnects;
      this.topic = topic;
      this.maxMessageWaitTime = maxMessageWaitTime;
    }
    
    public void onInit(TrackingRegistry registry) throws Exception {
      connector = new KafkaTopicConnector(kafkaZkConnects, topic) {
        @Override
        public void onTrackingMessage(TrackingMessage tMesg) throws Exception {
          queue.offer(tMesg, maxMessageWaitTime, TimeUnit.MILLISECONDS);
        }
      };
      connector.start();
    }
   
    public void onDestroy(TrackingRegistry registry) throws Exception{
      connector.shutdown();
    }
    
    @Override
    public TrackingMessage next() throws Exception {
      return queue.poll(maxMessageWaitTime, TimeUnit.MILLISECONDS);
    }
  }
  
  abstract public class KafkaTopicConnector extends Thread {
    private KafkaPartitionReader[] partitionReader;
    
    public KafkaTopicConnector(String kafkaZkConnects, String topic) throws Exception {
      KafkaTool kafkaTool = new KafkaTool("VMTMValidatorKafkaApp", kafkaZkConnects);

      TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topic);
      List<PartitionMetadata> partitionMetas = topicMeta.partitionsMetadata();
      int numOfPartitions = partitionMetas.size();
      
      partitionReader = new KafkaPartitionReader[numOfPartitions];
      for (int i = 0; i < numOfPartitions; i++) {
        partitionReader[i] = new KafkaPartitionReader("VMTMValidatorKafkaApp", kafkaZkConnects, topic, partitionMetas.get(i));
      }
    }
    
    public void run() {
      try {
        doRun() ;
      } catch (InterruptedException e) {
      } catch (Exception e) {
        logger.error("Fail to load the data from kafka", e);
      } finally {
        try {
          shutdown();
        } catch (Exception e) {
          logger.error("Fail to shutdown kafka connector", e);
        }
      }
    }
    
    void doRun() throws Exception {
      int batchFetch = 1024 ;
      int fetchSize  = 1024 * batchFetch;
      while(true) {
        int count = 0 ;
        for(int i = 0; i < partitionReader.length; i++) {
          List<Message> messages = partitionReader[i].fetch(fetchSize, batchFetch/*max read*/, 0/*max wait*/, 3);
          count +=  messages.size();
          for(Message message : messages) {
            ByteBuffer payload = message.payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            DataflowMessage rec = JSONSerializer.INSTANCE.fromBytes(bytes, DataflowMessage.class);
            TrackingMessage tMesg = JSONSerializer.INSTANCE.fromBytes(rec.getData(), TrackingMessage.class);
            onTrackingMessage(tMesg);
          }
        }
        if(count == 0) Thread.sleep(1000);
      }
    }
    
    public void shutdown() throws Exception {
      for(int i = 0; i < partitionReader.length; i++) {
        partitionReader[i].close();
      }
    }
    
    abstract public void onTrackingMessage(TrackingMessage tMesg) throws Exception ;
  }
}