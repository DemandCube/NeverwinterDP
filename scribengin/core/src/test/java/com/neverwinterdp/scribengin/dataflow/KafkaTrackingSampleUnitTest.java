package com.neverwinterdp.scribengin.dataflow;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaTrackingSampleUnitTest  {
  TrackingSampleApiRunner trackingSampleRunner = new TrackingSampleApiRunner();
  
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
    trackingSampleRunner.submitVMTMGenrator();
    trackingSampleRunner.runTMDataflow();
    trackingSampleRunner.submitKafkaVMTMValidator();
    trackingSampleRunner.runMonitor();
  }
}