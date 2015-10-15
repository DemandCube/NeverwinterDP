package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.neverwinterdp.kafka.consumer.KafkaMessageConsumerConnector;
import com.neverwinterdp.kafka.consumer.MessageConsumerHandler;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.storage.Record;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;

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
    long   kafkaMessageWaitTimeout = vmConfig.getPropertyAsLong("kafka.message-wait-timeout", 30000);
    
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
    private int                   numOfThread;
    private long                  maxMessageWaitTime;
    KafkaMessageConsumerConnector connector;
    
    KafkaTrackingMessageReader(String kafkaZkConnects, String topic, int numOfThread, long maxMessageWaitTime) {
      this.kafkaZkConnects = kafkaZkConnects;
      this.topic = topic;
      this.numOfThread = numOfThread;
      this.maxMessageWaitTime = maxMessageWaitTime;
    }
    
    public void onInit(TrackingRegistry registry) throws Exception {
      connector = 
          new KafkaMessageConsumerConnector("VMTMValidatorKafkaApp", kafkaZkConnects).
          withConsumerTimeoutMs(maxMessageWaitTime).
          connect();
      MessageConsumerHandler handler = new MessageConsumerHandler() {
        @Override
        public void onMessage(String topic, byte[] key, byte[] message) {
          try {
            Record rec = JSONSerializer.INSTANCE.fromBytes(message, Record.class);
            TrackingMessage tMesg = JSONSerializer.INSTANCE.fromBytes(rec.getData(), TrackingMessage.class);
            queue.offer(tMesg, maxMessageWaitTime, TimeUnit.MILLISECONDS);
          } catch(Throwable t) {
            logger.error("Error", t);
          }
        }
      };
      connector.consume(topic, handler, numOfThread);
    }
   
    public void onDestroy(TrackingRegistry registry) throws Exception{
      connector.close();
    }
    
    @Override
    public TrackingMessage next() throws Exception {
      return queue.poll(maxMessageWaitTime, TimeUnit.MILLISECONDS);
    }
  }
}