package com.neverwinterdp.scribengin.dataflow.sample;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaTrackingSampleUnitTest  {
  KafkaTrackingSampleRunner trackingSampleRunner = new KafkaTrackingSampleRunner();
  
  @Before
  public void setup() throws Exception {
    trackingSampleRunner.setup();
  }
  
  @After
  public void teardown() throws Exception {
    trackingSampleRunner.teardown();
  }
  
  @Test
  public void testTrackingSample() throws Exception {
    trackingSampleRunner.runDataflow();
  }
}