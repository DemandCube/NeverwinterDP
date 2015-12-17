package com.neverwinterdp.scribengin.dataflow.tracking;

import com.neverwinterdp.vm.VMAppConfig;

@SuppressWarnings("serial")
public class TrackingConfig extends VMAppConfig {
  final static public String REPORT_PATH              = "tracking.report-path";
  final static public String MESSAGE_SIZE             = "tracking.message-size";
  final static public String NUM_OF_CHUNK             = "tracking.num-of-chunk";
  final static public String NUM_OF_MESSAGE_PER_CHUNK = "tracking.num-of-message-per-chunk";

  final static public String GENERATOR_NUM_OF_WRITER    = "generator.num-of-writer";
  final static public String GENERATOR_BREAK_IN_PERIOD  = "generator.break-in-period";
  
  final static public String VALIDATOR_NUM_OF_READER    = "validator.num-of-reader";
  final static public String VALIDATOR_MAX_RUNTIME      = "validator.max-runtime";
  
  final static public String KAFKA_ZK_CONNECTS          = "kafka.zk-connects";
  final static public String KAFKA_INPUT_TOPIC          = "kafka.input-topic";
  final static public String KAFKA_VALIDATE_TOPIC       = "kafka.validate-topic";
  final static public String KAFKA_NUM_OF_PARTITION     = "kafka.num-of-partition";
  final static public String KAFKA_NUM_OF_REPLICATION   = "kafka.replication";
  final static public String KAFKA_MESSAGE_WAIT_TIMEOUT = "kafka.message-wait-timeout";
  
  public TrackingConfig() {
    setReportPath("/applications/tracking-message");
    setGeneratorNumOfWriter(1);
  }
  
  public String getReportPath() { return attribute(REPORT_PATH); }
  public void   setReportPath(String path) { attribute(REPORT_PATH, path); }

  
  public int getMessageSize() { return intAttribute(MESSAGE_SIZE, 512); }
  public void setMessageSize(int size) { attribute(MESSAGE_SIZE, size); }
  
  public int getNumOfChunk() { return intAttribute(NUM_OF_CHUNK, 5); }
  public void setNumOfChunk(int num) { attribute(NUM_OF_CHUNK, num); }

  public int getNumOfMessagePerChunk() { return intAttribute(NUM_OF_MESSAGE_PER_CHUNK, 1000); }
  public void setNumOfMessagePerChunk(int num) { attribute(NUM_OF_MESSAGE_PER_CHUNK, num); }

  public int getGeneratorNumOfWriter() { return intAttribute(GENERATOR_NUM_OF_WRITER, 1) ; }
  public void setGeneratorNumOfWriter(int num) { attribute(GENERATOR_NUM_OF_WRITER, num) ; }
  
  public long getGeneratorBreakInPeriod() { return longAttribute(GENERATOR_BREAK_IN_PERIOD, -1); }
  public void setGeneratorBreakInPeriod(long period) { attribute(GENERATOR_BREAK_IN_PERIOD, period); }
  
  public int getValidatorNumOfReader() { return intAttribute(VALIDATOR_NUM_OF_READER, 3) ; }
  public void setValidatorNumOfReader(int num) { attribute(VALIDATOR_NUM_OF_READER, num) ; }
  
  public long getValidatorMaxRuntime() { return longAttribute(VALIDATOR_MAX_RUNTIME, 120000) ; }
  public void setValidatorMaxRuntime(long max) { attribute(VALIDATOR_MAX_RUNTIME, max) ; }
  
  public String getKafkaZKConnects() { return attribute(KAFKA_ZK_CONNECTS); }
  public void setKafkaZKConnects(String zkConnects) { attribute(KAFKA_ZK_CONNECTS, zkConnects); }
  
  public String getKafkaInputTopic() { return attribute(KAFKA_INPUT_TOPIC); }
  public void setKafkaInputTopic(String topic) { attribute(KAFKA_INPUT_TOPIC, topic); }
  
  public String getKafkaValidateTopic() { return attribute(KAFKA_VALIDATE_TOPIC); }
  public void setKafkaValidateTopic(String topic) { attribute(KAFKA_VALIDATE_TOPIC, topic); }

  public int getKafkaNumOfPartition() { return intAttribute(KAFKA_NUM_OF_PARTITION, 5); }
  public void setKafkaNumOfPartition(int num) { attribute(KAFKA_NUM_OF_PARTITION, num); }

  public int getKafkaNumOfReplication() { return intAttribute(KAFKA_NUM_OF_REPLICATION, 1); }
  public void setKafkaNumOfReplication(int num) { attribute(KAFKA_NUM_OF_REPLICATION, num); }
  
  public long getKafkaMessageWaitTimeout() { return longAttribute(KAFKA_MESSAGE_WAIT_TIMEOUT, 30000); }
  public void setKafkaMessageWaitTimeout(long timeout) { attribute(KAFKA_MESSAGE_WAIT_TIMEOUT, timeout); }
}
