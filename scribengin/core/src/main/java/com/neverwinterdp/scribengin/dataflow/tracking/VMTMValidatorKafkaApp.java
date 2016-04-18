package com.neverwinterdp.scribengin.dataflow.tracking;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.neverwinterdp.kafka.KafkaTool;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.message.Message;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

public class VMTMValidatorKafkaApp extends VMApp {
  private Logger logger;
  private KafkaTool kafkaTool;
  
  @Override
  public void run() throws Exception {
    logger =  getVM().getLoggerFactory().getLogger(VMTMValidatorKafkaApp.class) ;
    
    VMDescriptor vmDescriptor = getVM().getDescriptor();
    VMConfig vmConfig = vmDescriptor.getVmConfig();

    TrackingConfig trackingConfig = vmConfig.getVMAppConfigAs(TrackingConfig.class);
    Registry registry = getVM().getVMRegistry().getRegistry();
    registry.setRetryable(true);
    
    runValidate(registry, trackingConfig);
  }

  public void runValidate(Registry registry, TrackingConfig trackingConfig) {
    try {
      info("Start runValidate(...)");
      kafkaTool = new KafkaTool("KafkaTool", trackingConfig.getKafkaZKConnects());
      TrackingValidatorService validatorService = new TrackingValidatorService(registry, trackingConfig);
      validatorService.addReader(new KafkaTrackingMessageReader(trackingConfig));
      validatorService.start();

      info("reportPath         = " + trackingConfig.getTrackingReportPath());
      info("maxRuntime         = " + trackingConfig.getValidatorMaxRuntime());
      info("maxMessageWaitTime = " + trackingConfig.getKafkaMessageWaitTimeout());
      info("validate topic     = " + trackingConfig.getKafkaValidateTopic());
      long startWait = System.currentTimeMillis();
      validatorService.awaitForTermination(trackingConfig.getValidatorMaxRuntime(), TimeUnit.MILLISECONDS);
      kafkaTool.close();
      long waitTime = System.currentTimeMillis() - startWait;
      info("Finish run(), waitTime = " + waitTime);
      info("Finish runValidate(...)");
    } catch(Throwable error) {
      error("Error:", error);
    }
  }
  
  void info(String message) {
    if(logger != null) logger.info(message);
    else System.out.println("VMTMValidatorKafkaApp: " + message);
  }
  
  void error(String message, Throwable error) {
    if(logger != null) {
      logger.error(message, error);
    } else {
      System.out.println("VMTMValidatorKafkaApp: " + message);
      error.printStackTrace();
    }
  }
  
  public class KafkaTrackingMessageReader extends TrackingMessageReader {
    private BlockingQueue<TrackingMessage> queue = new LinkedBlockingQueue<>(5000);
    
    private String                kafkaZkConnects;
    private String                topic;
    private long                  maxMessageWaitTime;
    KafkaTopicConnector           connector;
    
    KafkaTrackingMessageReader(TrackingConfig trackingConfig) {
      this.kafkaZkConnects = trackingConfig.getKafkaZKConnects();
      this.topic = trackingConfig.getKafkaValidateTopic();
      this.maxMessageWaitTime = trackingConfig.getKafkaMessageWaitTimeout();
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
      TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topic);
      List<PartitionMetadata> partitionMetas = topicMeta.partitionsMetadata();
      int numOfPartitions = partitionMetas.size();
      partitionReader = new KafkaPartitionReader[numOfPartitions];
      for (int i = 0; i < numOfPartitions; i++) {
        partitionReader[i] = new KafkaPartitionReader("VMTMValidatorKafkaApp", kafkaTool, topic, partitionMetas.get(i));
        readerQueue.offer(partitionReader[i]);
      }
    }
    
    public void start() {
      int NUM_OF_THREAD = 3;
      executorService = Executors.newFixedThreadPool(NUM_OF_THREAD);
      final AtomicInteger accCommit = new AtomicInteger();
      for(int i = 0; i < NUM_OF_THREAD; i++) {
        Runnable runnable = new Runnable() {
          public void run() {
            try {
              while(true) {
                KafkaPartitionReader pReader = readerQueue.take();
                Message message = null;
                int count = 0;
                while((message = pReader.nextAs(Message.class, 250)) != null) {
                  TrackingMessage tMesg = JSONSerializer.INSTANCE.fromBytes(message.getData(), TrackingMessage.class);
                  onTrackingMessage(tMesg);
                  count++;
                  if(count == 5000) break;
                }
                accCommit.addAndGet(count);
                //System.err.println("VMTMValidatorKafkaApp:  reader = "  + pReader.getPartition() + ", commit = " + count + ", acc commit = " + accCommit.get());
                pReader.commit();
                readerQueue.offer(pReader);
              }
            } catch (InterruptedException e) {
              error("Interrupted", e);
              System.err.println("VMTMValidatorKafkaApp:  KafkaPartitionReader is interruptted" );
            } catch (Throwable e) {
              error("Fail to load the data from kafka", e);
            } finally {
              try {
                shutdown();
              } catch (Exception e) {
                error("Fail to shutdown kafka connector", e);
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
}