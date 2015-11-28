package com.neverwinterdp.nstorage.segment;

public class SegmentConsistency {
  static public enum Consistency { 
    ERROR((byte)0), OK((byte)1), GOOD((byte)2);
    
    private byte level ;
    
    private Consistency(byte level) {
      this.level = level ;
    }
    
    public int compare(Consistency other) { return level - other.level; }
    
    public boolean greaterThan(Consistency other) { return level > other.level ;}
    
    public boolean equalOrGreaterThan(Consistency other) { return level >= other.level; }
  }
  
  private String      segmentId ;
  
  private Consistency status;
  private Consistency time;
  private Consistency commit;

  public SegmentConsistency() {}
  
  public SegmentConsistency(String segmentId) {
    this.segmentId = segmentId;
  }
  
  public String getSegmentId() { return segmentId;}
  public void setSegmentId(String segmentId) { this.segmentId = segmentId; }
  
  public Consistency getStatus() { return status; }
  public void setStatus(Consistency status) { this.status = status;}
  
  public Consistency getTime() { return time; }
  public void setTime(Consistency time) { this.time = time;}
  
  public Consistency getCommit() { return commit; }
  public void setCommit(Consistency commit) { this.commit = commit; }
}
