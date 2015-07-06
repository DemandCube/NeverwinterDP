package com.neverwinterdp.tool.message;

import org.junit.Test;

public class BitSetMessageTrackerUnitTest {
  @Test
  public void test() {
    BitSetMessageTracker tracker = new BitSetMessageTracker(100) ;
    for(int i = 0; i < 100; i++) {
      tracker.log("p1", i);
      tracker.log("p2", i);
      tracker.log("p3", i);
    }
    tracker.log("p1", 10);
    System.out.println(tracker.getFormatedReport());
  }
}
