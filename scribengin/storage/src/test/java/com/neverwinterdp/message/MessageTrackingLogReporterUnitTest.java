package com.neverwinterdp.message;

import org.junit.Test;

public class MessageTrackingLogReporterUnitTest {
  @Test
  public void test() {
    System.out.println("Merge 1 - 5, then 11 - 15");
    MessageTrackingReporter reporter = new MessageTrackingReporter("input");
    for(int i = 1; i <= 5; i++) {
      MessageTrackingChunkStat inputChunk = new MessageTrackingChunkStat("input", i, 10000);
      reporter.mergeFinished(inputChunk);
    }
    for(int i = 11; i <= 15; i++) {
      MessageTrackingChunkStat inputChunk = new MessageTrackingChunkStat("input", i, 10000);
      reporter.mergeFinished(inputChunk);
    }
    
    System.out.println(reporter.toFormattedText());
    
    System.out.println("Merge 6 - 10");
    for(int i = 6; i <= 10; i++) {
      MessageTrackingChunkStat inputChunk = new MessageTrackingChunkStat("input", i, 10000);
      reporter.mergeFinished(inputChunk);
    }
    System.out.println(reporter.toFormattedText());
    
    System.out.println("After Merge:");
    reporter.optimize();
    System.out.println(reporter.toFormattedText());
  }
}
