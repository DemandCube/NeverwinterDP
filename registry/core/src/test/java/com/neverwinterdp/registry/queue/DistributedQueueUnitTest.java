package com.neverwinterdp.registry.queue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zookeeper.tool.server.EmbededZKServer;

public class DistributedQueueUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  private EmbededZKServer zkServerLauncher ;
  private Registry registry;
  private DistributedQueue queue;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
    registry = new RegistryImpl(RegistryConfig.getDefault()).connect() ;
    queue = new DistributedQueue(registry, "/queue") ;
  }
  
  @After
  public void teardown() throws Exception {
    registry.shutdown();
    zkServerLauncher.shutdown();
  }
  
  @Test
  public void testOffer() throws Exception {
    offer(10);
  }

  @Test
  public void testPoll() throws Exception {
    offer(10);
    byte[] data = null ;
    int count = 0;
    while((data = queue.poll()) != null) {
      Assert.assertEquals("hello " + count++, new String(data));
    }
  }
  
  @Test
  public void testTake() throws Exception {
    final AtomicInteger counter = new AtomicInteger() ;
    Thread thread = new Thread() {
      public void run() {
        try {
          while(true) {
            byte[] data = queue.take() ;
            Assert.assertEquals("hello " + counter.getAndIncrement(), new String(data));
          }
        } catch (RegistryException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    };
    thread.start();
    
    offer(10);
    Thread.sleep(1000);
    thread.interrupt();
    System.out.println("take = " + counter.get()) ;
  }
  
  @Test
  public void testTakeTimeout() throws Exception {
    final AtomicInteger counter = new AtomicInteger() ;
    Thread thread = new Thread() {
      public void run() {
        try {
          while(true) {
            byte[] data = queue.take(1000) ;
            if(data == null) return ;
            Assert.assertEquals("hello " + counter.getAndIncrement(), new String(data));
          }
        } catch (RegistryException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
        } catch (Throwable e) {
          e.printStackTrace();
        }
      }
    };
    thread.start();
    for(int i = 0 ; i < 10; i++) {
      String data = "hello " + i ;
      queue.offer(data.getBytes());
      Thread.sleep((i + 1) * 200);
    }
    Assert.assertTrue(counter.get() < 8) ;
    System.out.println("take = " + counter.get()) ;
  }
  
  void offer(int size) throws Exception {
    for(int i = 0 ; i < size; i++) {
      String data = "hello " + i ;
      queue.offer(data.getBytes());
    }
  }
}
