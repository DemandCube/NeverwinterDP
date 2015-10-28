package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.neverwinterdp.kafka.KafkaClient;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

public class VMTMValidatorKafkaApp extends VMApp {
  private Logger logger;
  private KafkaClient kafkaClient;
  
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
    long   kafkaMessageWaitTimeout = vmConfig.getPropertyAsLong("kafka.message-wait-timeout", 30000);
    
    kafkaClient = new KafkaClient("KafkaClient", kafkaZkConnects);
    TrackingValidatorService validatorService = new TrackingValidatorService(registry, reportPath);
    validatorService.withExpectNumOfMessagePerChunk(expectNumOfMessagePerChunk);
    validatorService.addReader(
        new KafkaTrackingMessageReader(kafkaZkConnects, kafkaTopic, numOfReader, kafkaMessageWaitTimeout)
    );
    validatorService.start();
    validatorService.awaitForTermination(maxRuntime, TimeUnit.MILLISECONDS);
    kafkaClient.close();
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
  
  abstract public class KafkaTopicConnector {
    private KafkaPartitionReader[] partitionReader;
    private BlockingQueue<KafkaPartitionReader> readerQueue = new LinkedBlockingQueue<>();
    private ExecutorService executorService;
    
    public KafkaTopicConnector(String kafkaZkConnects, String topic) throws Exception {
      TopicMetadata topicMeta = kafkaClient.findTopicMetadata(topic);
      List<PartitionMetadata> partitionMetas = topicMeta.partitionsMetadata();
      int numOfPartitions = partitionMetas.size();
      partitionReader = new KafkaPartitionReader[numOfPartitions];
      for (int i = 0; i < numOfPartitions; i++) {
        partitionReader[i] = new KafkaPartitionReader("VMTMValidatorKafkaApp", kafkaClient, topic, partitionMetas.get(i));
        readerQueue.offer(partitionReader[i]);
      }
    }
    
    public void start() {
      executorService = Executors.newFixedThreadPool(2);
      for(int i = 0; i < 2; i++) {
        Runnable runnable = new Runnable() {
          public void run() {
            try {
              while(true) {
                KafkaPartitionReader pReader = readerQueue.take();
                Record rec = null;
                int count = 0;
                while((rec = pReader.nextAs(Record.class, 1000)) != null && count < 1000) {
                  TrackingMessage tMesg = JSONSerializer.INSTANCE.fromBytes(rec.getData(), TrackingMessage.class);
                  onTrackingMessage(tMesg);
                  count++;
                }
                readerQueue.offer(pReader);
              }
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
        };
        executorService.submit(runnable);
      }
      executorService.shutdown();
    }
    
    public void shutdown() throws Exception {
      executorService.shutdownNow();
      for(int i = 0; i < partitionReader.length; i++) {
        partitionReader[i].close();
      }
    }
    
    abstract public void onTrackingMessage(TrackingMessage tMesg) throws Exception ;
  }
  
  abstract public class KafkaTopicReaderThread extends Thread {
    
  }
}