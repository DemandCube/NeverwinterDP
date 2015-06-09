package com.neverwinterdp.os;

import java.io.IOException;

import org.junit.Test;

public class OSServiceUnitTest {
  @Test
  public void testMemory() {
    OSService service = new OSService() ;
    MemoryInfo[] memoryInfo = service.getMemoryInfo() ;
    System.out.println(MemoryInfo.getFormattedText(memoryInfo));
  }
  
  @Test
  public void testGC() {
    OSService service = new OSService() ;
    System.gc();
    GCInfo[] gcInfo = service.getGCInfo() ;
    System.out.println(GCInfo.getFormattedText(gcInfo)) ;
  }
  
  @Test
  public void testFileStore() throws IOException {
    OSService service = new OSService() ;
    FileStoreInfo[] fsInfo = service.getFileStoreInfo();
    System.out.println(FileStoreInfo.getFormattedText(fsInfo)) ;
  }
  
  @Test
  public void testThreadCountInfo() throws IOException {
    OSService service = new OSService() ;
    System.out.println(service.getThreadCountInfo().getFormattedText()) ;
  }
  
  @Test
  public void testDetailThreadInfo() throws IOException {
    OSService service = new OSService() ;
    System.out.println(DetailThreadInfo.getFormattedText(service.getDetailThreadInfo())) ;
  }
  
  @Test
  public void testOSInfo() {
    OSService service = new OSService() ;
    System.out.println(OSInfo.getFormattedText(service.getOSInfo())) ;
  }
}
