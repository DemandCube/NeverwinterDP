package com.neverwinterdp.monitor.jhiccup;

import java.util.Random;

import org.junit.Test;

import com.neverwinterdp.os.RuntimeEnv;

public class PeterJHiccupMeterUnitTest {
  
  @Test
  public void testJHiccupMetter() throws Exception {
    RuntimeEnv runtimeEnv = new RuntimeEnv("localhost", "localhost", "./build");
    PeterHiccupMeter hiccupMeter =  new PeterHiccupMeter(runtimeEnv);
    Random rand = new Random();
    for(int i = 0; i < 50; i++) {
      JHiccupInfo hiccupInfo = hiccupMeter.getHiccupInfo();
      System.out.println(JHiccupInfo.getFormattedText(hiccupInfo));
      for(int j = 0; j < 1000; j++) {
        //produce some data so gc will pause jvm to collect the objects.
        byte[] data = new byte[1024 * 1024]; 
        rand.nextBytes(data);
      }
      Thread.sleep(5000);
    }
  }

}
