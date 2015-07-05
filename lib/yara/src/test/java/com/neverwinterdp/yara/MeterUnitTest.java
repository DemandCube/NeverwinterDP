package com.neverwinterdp.yara;

import java.util.Random;

import org.junit.Test;

public class MeterUnitTest {
  @Test
  public void testMeter() throws InterruptedException {
    MetricRegistry mRegistry = new MetricRegistry();
    Random rand = new Random() ;
    Meter meter = mRegistry.getMeter("meter") ;
    for(int i = 0; i < 121; i++) {
      meter.mark(rand.nextInt(100000)) ;
      Thread.sleep(1000);
    }
    System.out.println("Count       = " + meter.getCount()) ;
    System.out.println("1  Min Rate = " + meter.getOneMinuteRate()) ;
    System.out.println("5  Min Rate = " + meter.getFiveMinuteRate()) ;
    System.out.println("15 Min Rate = " + meter.getFifteenMinuteRate()) ;
    System.out.println("Mean Rate   = " + meter.getMeanRate()) ;
  }
}
