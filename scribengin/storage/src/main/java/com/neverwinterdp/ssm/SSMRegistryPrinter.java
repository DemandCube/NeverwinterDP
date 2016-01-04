package com.neverwinterdp.ssm;

import java.io.IOException;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;

public class SSMRegistryPrinter {
  private Appendable             out;
  private SSMRegistry segStorageReg;
  
  public SSMRegistryPrinter(Appendable out, SSMRegistry segStorageReg) {
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
    SegmentsDescriptor segsDescriptor = segStorageReg.getSegmentsDescriptor();
    println("  ", "segments:", segsDescriptor.toString());
    List<String> segments = segStorageReg.getSegments() ;
    for(int i = 0; i < segments.size(); i++) {
      SegmentDescriptor segment = segStorageReg.getSegmentBySegmentId(segments.get(i));
      println("    "  , segment.toString()) ;
    }
  }
  
  public void printReaders() throws IOException, RegistryException {
    println("  ", "readers");
    List<String> allReaders = segStorageReg.getActiveReaders() ;
    println("    ", "all");
    for(int i = 0; i < allReaders.size(); i++) {
      SSMReaderDescriptor reader = segStorageReg.getReader(allReaders.get(i));
      println("      "  , reader.toString()) ;
      List<String> segmentIds = segStorageReg.getSegmentReadDescriptors(reader);
      for(int j = 0; j < segmentIds.size(); j++) {
        SegmentReadDescriptor segmentReadDescriptor = segStorageReg.getSegmentReadDescriptor(reader, segmentIds.get(j));
        println("        ", segmentReadDescriptor.toString()) ;
      }
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
    List<String> allWriters = segStorageReg.getActiveWriters() ;
    println("    ", "all");
    for(int i = 0; i < allWriters.size(); i++) {
      SSMWriterDescriptor writer = segStorageReg.getWriter(allWriters.get(i));
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
