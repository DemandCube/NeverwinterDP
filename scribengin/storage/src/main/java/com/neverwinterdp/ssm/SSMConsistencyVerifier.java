package com.neverwinterdp.ssm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.ssm.SegmentConsistency.Consistency;
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
    SegmentConsistency sc = new SegmentConsistency(segment.getSegmentId());
    if(segment.getStatus() == SegmentDescriptor.Status.WritingComplete) {
      sc.setStatus(Consistency.GOOD);
      if(segment.getFinishedTime() >= segment.getCreatedTime()) sc.setTime(Consistency.GOOD);
      else sc.setTime(Consistency.ERROR);
      
      if(segment.getDataSegmentLastCommitPos() == getDataSegmentLength(segment)) sc.setCommit(Consistency.GOOD);
      else if(segment.getDataSegmentLastCommitPos() < getDataSegmentLength(segment)) sc.setCommit(Consistency.OK);
      else sc.setCommit(Consistency.ERROR);
    } else if(segment.getStatus() == SegmentDescriptor.Status.Writing) {
      sc.setStatus(Consistency.GOOD);
      if(segment.getFinishedTime() == -1) sc.setTime(Consistency.GOOD);
      else sc.setTime(Consistency.ERROR);
      
      if(segment.getDataSegmentLastCommitPos() >= getDataSegmentLength(segment)) sc.setCommit(Consistency.OK);
      else sc.setCommit(Consistency.ERROR);
    } 
    return sc;
  }
  
  abstract protected long getDataSegmentLength(SegmentDescriptor segment) throws IOException ;
  
  public SegmentConsistency.Consistency getMinCommitConsistency() {
    SegmentConsistency.Consistency minConsistency = null;
    for(int i = 0; i < segementConsistencies.size(); i++) {
      SegmentConsistency sc = segementConsistencies.get(i);
      if(minConsistency == null) minConsistency = sc.getCommit();
      else if(minConsistency.compare(sc.getCommit()) > 0) minConsistency = sc.getCommit();
    }
    return minConsistency;
  }
  
  
  public String getSegmentConsistencyTextReport() throws RegistryException, IOException {
    TabularFormater ft = new TabularFormater("Segment Id", "Status", "Time", "Commit");
    ft.setTitle("Segment Consistency");
    for(int i = 0; i < segementConsistencies.size(); i++) {
      SegmentConsistency sc = segementConsistencies.get(i);
      ft.addRow(
        sc.getSegmentId(), sc.getStatus(), sc.getTime(), sc.getCommit()
      );
    }
    return ft.getFormattedText();
  }
  
  public String getSegmentDescriptorTextReport() throws RegistryException, IOException {
    String[] header = {
      "Segment Id", "Creator", "C Time", "F Time", "Status", "From", "To", 
      "Num Rec", "Commit Pos", "Commit Count"
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