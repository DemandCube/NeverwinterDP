package com.neverwinterdp.yara;

import java.util.Random;

import org.junit.Test;

public class MeterUnitTest {
  @Test
  public void testMeter() throws Exception {
    MetricRegistry mRegistry = new MetricRegistry();
    Random rand = new Random() ;
    Meter meter = mRegistry.getMeter("meter", "byte") ;
    for(int i = 0; i < 150; i++) {
      meter.mark(rand.nextInt(100000));
      Thread.sleep(100);
    }
    MetricPrinter mPrinter = new MetricPrinter() ;
    mPrinter.print(mRegistry);
  }
}
