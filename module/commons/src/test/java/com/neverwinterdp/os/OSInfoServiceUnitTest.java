package com.neverwinterdp.os;

import org.junit.Test;

public class OSInfoServiceUnitTest {
  @Test
  public void testMemory() {
    OSInfoService service = new OSInfoService() ;
    MemoryInfo[] memoryInfo = service.getMemoryInfo() ;
    System.out.println(MemoryInfo.getFormattedText(memoryInfo));
  }
  
  @Test
  public void testGC() {
    OSInfoService service = new OSInfoService() ;
    System.gc();
    GCInfo[] gcInfo = service.getGCInfo() ;
    System.out.println(GCInfo.getFormattedText(gcInfo)) ;
  }
}
