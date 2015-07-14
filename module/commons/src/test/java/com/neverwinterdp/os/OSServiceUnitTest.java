package com.neverwinterdp.os;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class OSServiceUnitTest {
  OSManagement service ;
  
  @Before
  public void setup() {
    service = new OSManagement(new RuntimeEnv("localhost", "localhost", "./build") ) ;
  }
  
  @Test
  public void testMemory() {
    MemoryInfo[] memoryInfo = service.getMemoryInfo() ;
    System.out.println(MemoryInfo.getFormattedText(memoryInfo));
  }
  
  @Test
  public void testGC() {
    System.gc();
    GCInfo[] gcInfo = service.getGCInfo() ;
    System.out.println(GCInfo.getFormattedText(gcInfo)) ;
  }
  
  @Test
  public void testFileStore() throws IOException {
    FileStoreInfo[] fsInfo = service.getFileStoreInfo();
    System.out.println(FileStoreInfo.getFormattedText(fsInfo)) ;
  }
  
  @Test
  public void testThreadCountInfo() throws IOException {
    System.out.println(service.getThreadCountInfo().getFormattedText()) ;
  }
  
  @Test
  public void testDetailThreadInfo() throws IOException {
    System.out.println(DetailThreadInfo.getFormattedText(service.getDetailThreadInfo())) ;
  }
  
  @Test
  public void testOSInfo() {
    System.out.println(OSInfo.getFormattedText(service.getOSInfo())) ;
  }
}
