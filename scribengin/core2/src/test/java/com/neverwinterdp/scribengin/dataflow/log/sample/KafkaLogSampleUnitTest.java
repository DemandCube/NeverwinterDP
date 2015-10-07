package com.neverwinterdp.scribengin.dataflow.log.sample;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaLogSampleUnitTest  {
  KafkaLogSampleRunner logSampleRunner = new KafkaLogSampleRunner();
  
  @Before
  public void setup() throws Exception {
    logSampleRunner.setup();
  }
  
  @After
  public void teardown() throws Exception {
    logSampleRunner.teardown();
  }
  
  @Test
  public void testLogSampleChain() throws Exception {
    logSampleRunner.runDataflow();
  }
}