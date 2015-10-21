package com.neverwinterdp.scribengin.dataflow.tool.tracking;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MemTrackingMessageStorage {
  private double                         duplicatedRatio  = 0.01;
  private double                         lostRatio        = 0.01;
  private Random                         duplicatedRandom = new Random();
  private Random                         lostRandom       = new Random();
  private BlockingQueue<TrackingMessage> queue            = new LinkedBlockingQueue<>(5000);

  public TrackingMessageWriter newWriter() {
    return new MemTrackingMessageWriter();
  }
  
  public TrackingMessageReader newReader() {
    return new MemTrackingMessageReader();
  }
  
  public class MemTrackingMessageWriter extends TrackingMessageWriter {
    @Override
    public void write(TrackingMessage message) throws Exception {
      if(lostRandom.nextDouble() < lostRatio) return;
      queue.offer(message, 5, TimeUnit.SECONDS);
      if(duplicatedRandom.nextDouble() < duplicatedRatio) {
        queue.offer(message, 5, TimeUnit.SECONDS);
      }
    }
  }
  
  public class MemTrackingMessageReader extends TrackingMessageReader {
    @Override
    public TrackingMessage next() throws Exception {
      return queue.poll(5, TimeUnit.SECONDS);
    }
  }
}
