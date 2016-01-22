package com.neverwinterdp.ssm;

public class SegmentConsistency {
  static public enum Consistency { 
    ERROR((byte)0), OK_WITH_BROKEN_COMMIT((byte)1), OK((byte)2), GOOD((byte)3);
    
    private byte level ;
    
    private Consistency(byte level) {
      this.level = level ;
    }
    
    public int compare(Consistency other) { return level - other.level; }
    
    public boolean greaterThan(Consistency other) { return level > other.level ;}
    
    public boolean equalOrGreaterThan(Consistency other) { return level >= other.level; }
  }
  
  private String      segmentId ;
  private String      status;
  private long        dataLength;
  private long        lastCommitPosition;
  private int         commitCount;
  private Consistency statusConsistency;
  private Consistency timeConsistency;
  private Consistency commitConsistency;

  public SegmentConsistency() {}
  
  public SegmentConsistency(String segmentId) {
    this.segmentId = segmentId;
  }
  
  public String getSegmentId() { return segmentId;}
  public void setSegmentId(String segmentId) { this.segmentId = segmentId; }
  
  public String getStatus() { return status; }
  public void setStatus(String status) { this.status = status; }

  public long getDataLength() { return dataLength; }
  public void setDataLength(long dataLength) { this.dataLength = dataLength; }

  public long getLastCommitPosition() { return lastCommitPosition; }
  public void setLastCommitPosition(long lastCommitPosition) { this.lastCommitPosition = lastCommitPosition; }

  public int getCommitCount() { return commitCount; }
  public void setCommitCount(int count) { this.commitCount = count; }

  public Consistency getStatusConsistency() { return statusConsistency; }
  public void setStatusConsistency(Consistency status) { this.statusConsistency = status;}
  
  public Consistency getTimeConsistency() { return timeConsistency; }
  public void setTimeConsistency(Consistency time) { this.timeConsistency = time;}
  
  public Consistency getCommitConsistency() { return commitConsistency; }
  public void setCommitConsistency(Consistency commit) { this.commitConsistency = commit; }
}
