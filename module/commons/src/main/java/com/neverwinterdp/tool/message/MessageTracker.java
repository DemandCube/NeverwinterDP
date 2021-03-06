package com.neverwinterdp.tool.message;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.tap4j.model.TestResult;
import org.tap4j.model.TestSet;
import org.tap4j.producer.TapProducer;
import org.tap4j.producer.TapProducerFactory;
import org.tap4j.util.StatusValues;

import com.neverwinterdp.tool.message.PartitionMessageTracker.SequenceMap;
import com.neverwinterdp.util.text.TabularFormater;

public class MessageTracker {
  private TreeMap<String, PartitionMessageTracker> partitions = new TreeMap<>();

  synchronized public int getLogCount() {
    int logCount = 0;
    for (PartitionMessageTracker sel : partitions.values()) {
      logCount += sel.getLogCount();
    }
    return logCount;
  }

  synchronized public int getDuplicatedCount() {
    int duplicatedCount = 0;
    for (PartitionMessageTracker sel : partitions.values()) {
      duplicatedCount += sel.getDuplicatedCount();
    }
    return duplicatedCount;
  }

  synchronized public boolean isInSequence() {
    for (PartitionMessageTracker sel : partitions.values()) {
      if (!sel.isInSequence())
        return false;
    }
    return true;
  }

  synchronized public void log(Message message) {
    log(message.getPartition(), message.getTrackId());
  }

  public void log(int partition, int trackId) {
    log(Integer.toString(partition), trackId);
  }
  
  public void log(String partition, int trackId) {
    PartitionMessageTracker partitionTracker = getPartitionMessageTracker(partition, true);
    partitionTracker.log(trackId);
  }

  public TreeMap<String, PartitionMessageTracker> getPartitionMessageTrackers() { return partitions; }

  public void setPartitions(TreeMap<String, PartitionMessageTracker> partitions) {
    this.partitions = partitions;
  }

  public PartitionMessageTracker getPartitionMessageTracker(int partition) {
    return partitions.get(partition);
  }

  PartitionMessageTracker getPartitionMessageTracker(String partition, boolean create) {
    PartitionMessageTracker partitionTracker = partitions.get(partition);
    if (partitionTracker != null)
      return partitionTracker;
    if (!create)
      return null;
    synchronized (partitions) {
      partitionTracker = partitions.get(partition);
      if (partitionTracker != null)
        return partitionTracker;
      partitionTracker = new PartitionMessageTracker(partition);
      partitions.put(partitionTracker.getPartition(), partitionTracker);
      return partitionTracker;
    }
  }

  public void optimize() {
    for (PartitionMessageTracker sel : partitions.values()) {
      sel.optimize();
    }
  }

  public String getFormattedReport() throws IOException {
    StringBuilder out = new StringBuilder();
    out.append("\nMessage Tracker: \n\n");
    String[] header = {
        "Partition", "From", "To", "In Sequence", "Duplication"
    };
    TabularFormater formater = new TabularFormater(header);
    formater.setTitle("Message Tracker");
    
    for (Map.Entry<String, PartitionMessageTracker> entry : partitions.entrySet()) {
      String partition = entry.getKey();
      formater.addRow("    " + partition, "", "", "", "");
      PartitionMessageTracker partitionTracker = entry.getValue();
      List<SequenceMap> map = partitionTracker.getSequenceMap() ;
      SequenceMap prevSeqMap = null;
      for (int i = 0; i < map.size(); i++) {
        SequenceMap seqMap = map.get(i);
        boolean inSequence = true;
        if (prevSeqMap != null) {
          inSequence = prevSeqMap.getCurrent() + 1 == seqMap.getFrom();
        }
        Object[] cells = {
          "", seqMap.getFrom(), seqMap.getCurrent(), inSequence, seqMap.getDuplicatedDescription()
        };
        formater.addRow(cells);
        prevSeqMap = seqMap;
      }
    }
    out.append(formater.getFormatText()).append("\n");
    out.append("\nLog Count: " + getLogCount() + "\n");
    return out.toString();
  }
  
  public void dump(Appendable out) throws IOException {
    out.append(getFormattedReport());
  }

  //prefer using details for each partitionTracker for a thorough report
  public void junitReport(String fileName) throws IOException {
    TestSet testSet = new TestSet();
    int testNum = 0;
    optimize();
    // a test per partition
    for (Map.Entry<String, PartitionMessageTracker> entry : partitions.entrySet()) {
      String partition = entry.getKey();
      PartitionMessageTracker partitionTracker = entry.getValue();

      testSet.addTestResult(newTestResult(++testNum,
          "Partition: " + partition
              + " From:" + partitionTracker.getMinMessageId()
              + " To:" + partitionTracker.getMaxMessageId()
              + " Duplicates:" + partitionTracker.getDuplicatedCount()
              + " Num messages:" + partitionTracker.getLogCount()
              + " In Sequence: " + partitionTracker.isInSequence(),
          partitionTracker.isInSequence()));
    }
    
    File file = new File(fileName);
    file.getParentFile().mkdirs();
    file.createNewFile();

    TapProducer tapProducer = TapProducerFactory.makeTapJunitProducer(getClass().getSimpleName());
    tapProducer.dump(testSet, file);
  }

  private TestResult newTestResult(int testNum, String desc, boolean success) {
    TestResult tr = null;
    if (success) {
      tr = new TestResult(StatusValues.OK, testNum);
    } else {
      tr = new TestResult(StatusValues.NOT_OK, testNum);
    }
    tr.setDescription(desc);
    return tr;
  }
}
