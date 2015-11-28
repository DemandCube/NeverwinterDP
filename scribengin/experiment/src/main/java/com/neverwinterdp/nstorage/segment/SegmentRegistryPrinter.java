package com.neverwinterdp.nstorage.segment;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;

public class SegmentRegistryPrinter {
  private Appendable             out;
  private SegmentRegistry segStorageReg;
  
  public SegmentRegistryPrinter(Appendable out, SegmentRegistry segStorageReg) {
    this.out           = out;
    this.segStorageReg = segStorageReg;
  }
  
  public void print() throws IOException, RegistryException {
     println("", segStorageReg.getRegistryPath());
     printSegments();
     printReaders();
     printWriters();
  }
  
  public void printSegments() throws IOException, RegistryException {
    println("  ", "segments");
    List<String> segments = segStorageReg.getSegments() ;
    for(int i = 0; i < segments.size(); i++) {
      SegmentDescriptor segment = segStorageReg.getSegmentByName(segments.get(i));
      println("    "  , segment.toString()) ;
    }
  }
  
  public void printReaders() throws IOException, RegistryException {
    println("  ", "readers");
    List<String> allReaders = segStorageReg.getAllReaders() ;
    println("    ", "all");
    for(int i = 0; i < allReaders.size(); i++) {
      ReaderDescriptor reader = segStorageReg.getReader(allReaders.get(i));
      println("      "  , reader.toString()) ;
    }
    println("    ", "active");
    List<String> activeReaders = segStorageReg.getActiveReaders() ;
    for(int i = 0; i < activeReaders.size(); i++) {
      println("      "  , activeReaders.get(i)) ;
    }
    println("    ", "heartbeat");
    List<String> heartbeatReaders = segStorageReg.getActiveReaders() ;
    for(int i = 0; i < heartbeatReaders.size(); i++) {
      println("      "  , heartbeatReaders.get(i)) ;
    }
    println("    ", "history");
    List<String> historyReaders = segStorageReg.getHistoryReaders() ;
    for(int i = 0; i < historyReaders.size(); i++) {
      println("      "  , historyReaders.get(i)) ;
    }
  }
  
  public void printWriters() throws IOException, RegistryException {
    println("  ", "writers");
    List<String> allWriters = segStorageReg.getAllWriters() ;
    println("    ", "all");
    for(int i = 0; i < allWriters.size(); i++) {
      WriterDescriptor writer = segStorageReg.getWriter(allWriters.get(i));
      println("      "  , writer.getId()) ;
      println("        Info: ", writer.basicInfo()) ;
      println("        Logs: ") ;
      if(writer.getLogs() != null) {
        for(String log : writer.getLogs()) {
          println("          ", log) ;
        }
      }
    }
    println("    ", "active");
    List<String> activeWriters = segStorageReg.getActiveWriters() ;
    for(int i = 0; i < activeWriters.size(); i++) {
      println("      "  , activeWriters.get(i)) ;
    }
    println("    ", "heartbeat");
    List<String> heartbeatWriters = segStorageReg.getActiveWriters() ;
    for(int i = 0; i < heartbeatWriters.size(); i++) {
      println("      "  , heartbeatWriters.get(i)) ;
    }
    println("    ", "history");
    List<String> historyWriters = segStorageReg.getHistoryWriters() ;
    for(int i = 0; i < historyWriters.size(); i++) {
      println("      "  , historyWriters.get(i)) ;
    }
  }
  
  void println(String indent, String ... token) throws IOException {
    out.append(indent);
    for(int i = 0; i < token.length; i++) {
      out.append(token[i]).append(' ');
    }
    out.append('\n');
  }
}
