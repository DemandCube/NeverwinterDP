package com.neverwinterdp.ssm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SegmentConsistency.Consistency;
import com.neverwinterdp.ssm.SegmentDescriptor.Status;
import com.neverwinterdp.util.text.DateUtil;
import com.neverwinterdp.util.text.TabularFormater;

abstract public class SSMConsistencyVerifier {
  private SSMRegistry segRegistry;
  private List<SegmentConsistency> segementConsistencies;
  private List<SegmentDescriptor>  segmentDescriptors;
  
  public SSMConsistencyVerifier(SSMRegistry segReg) {
    segRegistry = segReg;
  }
  
  public void verify() throws RegistryException, IOException {
    List<String> segments = segRegistry.getSegments() ;
    segementConsistencies = new ArrayList<>();
    segmentDescriptors    = new ArrayList<>();
    for(int i = 0; i < segments.size(); i++) {
      SegmentDescriptor segment = segRegistry.getSegmentBySegmentId(segments.get(i));
      SegmentConsistency sc = verify(segment);
      segmentDescriptors.add(segment);
      segementConsistencies.add(sc);
    }
  }
  
  SegmentConsistency verify(SegmentDescriptor segment) throws RegistryException, IOException {
    long dataLength = getDataSegmentLength(segment);
    SegmentConsistency sc = new SegmentConsistency(segment.getSegmentId());
    sc.setStatus(segment.getStatus().toString());
    sc.setDataLength(dataLength);
    sc.setLastCommitPosition(segment.getDataSegmentLastCommitPos());
    sc.setCommitCount(segment.getDataSegmentCommitCount());
    Status segStatus = segment.getStatus();
    if(segStatus == SegmentDescriptor.Status.WritingComplete || segStatus == Status.Complete) {
      sc.setStatusConsistency(Consistency.GOOD);
      if(segment.getFinishedTime() >= segment.getCreatedTime()) sc.setTimeConsistency(Consistency.GOOD);
      else sc.setTimeConsistency(Consistency.ERROR);
      
      if(segment.getDataSegmentLastCommitPos() == dataLength) sc.setCommitConsistency(Consistency.GOOD);
      else if(segment.getDataSegmentLastCommitPos() < dataLength) sc.setCommitConsistency(Consistency.OK_WITH_BROKEN_COMMIT);
      else if(segment.getDataSegmentLastCommitPos() == dataLength) sc.setCommitConsistency(Consistency.OK);
      else sc.setCommitConsistency(Consistency.ERROR);
    } else if(segStatus == SegmentDescriptor.Status.Writing) {
      sc.setStatusConsistency(Consistency.GOOD);
      if(segment.getFinishedTime() == -1) sc.setTimeConsistency(Consistency.GOOD);
      else sc.setTimeConsistency(Consistency.ERROR);
      
      if(segment.getDataSegmentLastCommitPos() >= dataLength) sc.setCommitConsistency(Consistency.OK);
      else sc.setCommitConsistency(Consistency.ERROR);
    } 
    return sc;
  }
  
  abstract protected long getDataSegmentLength(SegmentDescriptor segment) throws IOException ;
  
  public SegmentConsistency.Consistency getMinCommitConsistency() {
    SegmentConsistency.Consistency minConsistency = null;
    for(int i = 0; i < segementConsistencies.size(); i++) {
      SegmentConsistency sc = segementConsistencies.get(i);
      if(minConsistency == null) minConsistency = sc.getCommitConsistency();
      else if(minConsistency.compare(sc.getCommitConsistency()) > 0) minConsistency = sc.getCommitConsistency();
    }
    return minConsistency;
  }
  
  
  public String getSegmentConsistencyTextReport() throws RegistryException, IOException {
    String[] header = {
      "Segment Id", "Status", "Data Length", "Last Commit Pos", "Commit Count", "*Status", "*Time", "*Commit"
    };
    
    TabularFormater ft = new TabularFormater(header);
    ft.setTitle("Segment Consistency");
    for(int i = 0; i < segementConsistencies.size(); i++) {
      SegmentConsistency sc = segementConsistencies.get(i);
      ft.addRow(
        sc.getSegmentId(),
        sc.getStatus(), sc.getDataLength(), sc.getLastCommitPosition(), sc.getCommitCount(),
        sc.getStatusConsistency(), sc.getTimeConsistency(), sc.getCommitConsistency()
      );
    }
    return ft.getFormattedText();
  }
  
  public String getSegmentDescriptorTextReport() throws RegistryException, IOException {
    String[] header = {
      "Segment Id", "Creator", "C Time", "F Time", "Status", "From", "To", "Num Rec", "Commit Pos", "Commit Count"
    };
    TabularFormater ft = new TabularFormater(header);
    ft.setTitle("Segment Descriptor");
    for(int i = 0; i < segementConsistencies.size(); i++) {
      SegmentDescriptor segment = segmentDescriptors.get(i);
      ft.addRow(
        segment.getSegmentId(),
        segment.getWriter(), 
        DateUtil.asCompactDateTime(segment.getCreatedTime()),
        DateUtil.asCompactDateTime(segment.getFinishedTime()),
        segment.getStatus(),
        segment.getRecordFrom(),
        segment.getRecordTo(),
        segment.getDataSegmentNumOfRecords(),
        segment.getDataSegmentLastCommitPos(),
        segment.getDataSegmentCommitCount()
      );
    }
    return ft.getFormattedText();
  }
}