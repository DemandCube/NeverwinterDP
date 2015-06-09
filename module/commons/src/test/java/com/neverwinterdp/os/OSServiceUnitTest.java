package com.neverwinterdp.os;

import java.io.IOException;

import org.junit.Test;

public class OSServiceUnitTest {
  @Test
  public void testMemory() {
    OSManagement service = new OSManagement() ;
    MemoryInfo[] memoryInfo = service.getMemoryInfo() ;
    System.out.println(MemoryInfo.getFormattedText(memoryInfo));
  }
  
  @Test
  public void testGC() {
    OSManagement service = new OSManagement() ;
    System.gc();
    GCInfo[] gcInfo = service.getGCInfo() ;
    System.out.println(GCInfo.getFormattedText(gcInfo)) ;
  }
  
  @Test
  public void testFileStore() throws IOException {
    OSManagement service = new OSManagement() ;
    FileStoreInfo[] fsInfo = service.getFileStoreInfo();
    System.out.println(FileStoreInfo.getFormattedText(fsInfo)) ;
  }
  
  @Test
  public void testThreadCountInfo() throws IOException {
    OSManagement service = new OSManagement() ;
    System.out.println(service.getThreadCountInfo().getFormattedText()) ;
  }
  
  @Test
  public void testDetailThreadInfo() throws IOException {
    OSManagement service = new OSManagement() ;
    System.out.println(DetailThreadInfo.getFormattedText(service.getDetailThreadInfo())) ;
  }
  
  @Test
  public void testOSInfo() {
    OSManagement service = new OSManagement() ;
    System.out.println(OSInfo.getFormattedText(service.getOSInfo())) ;
  }
}
