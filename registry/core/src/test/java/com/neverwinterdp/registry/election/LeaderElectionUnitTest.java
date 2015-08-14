package com.neverwinterdp.registry.election;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.io.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServer;

public class LeaderElectionUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  final static String ELECTION_PATH = "/election" ;
  
  private EmbededZKServer zkServerLauncher ;
  
  @Before
  public void setup() throws Exception {
    FileUtil.removeIfExist("./build/data", false);
    zkServerLauncher = new EmbededZKServer("./build/data/zookeeper") ;
    zkServerLauncher.start();
  }
  
  @After
  public void teardown() throws Exception {
    zkServerLauncher.shutdown();
  }

  private Registry newRegistry() {
    return new RegistryImpl(RegistryConfig.getDefault()) ;
  }
  
  @Test
  public void testDisconnect() throws Exception {
    Registry registry1 = newRegistry().connect();
    registry1.createIfNotExist(ELECTION_PATH) ;
    
    Node leader1Path =  registry1.get(ELECTION_PATH) ;
    LeaderElection leader1 = leader1Path.getLeaderElection();
    leader1.setListener(new LeaderElectionListener() {
      public void onElected() {
        System.out.println("Elected leader 1");
      }
    });
    leader1.start();

    Registry registry2   = newRegistry().connect();
    Node     leader2Path =  registry2.get(ELECTION_PATH) ;

    LeaderElection leader2 = leader2Path.getLeaderElection();
    leader2.setListener(new LeaderElectionListener() {
      public void onElected() {
        System.out.println("Elected leader 2");
      }
    });
    leader2.start();
    registry1.disconnect();
    Thread.sleep(1000);
  }
  
  @Test
  public void testElection() throws Exception {
    Registry registry = newRegistry().connect(); 
    registry.createIfNotExist(ELECTION_PATH) ;
    
    Leader[] leader = new Leader[3];
    ExecutorService executorService = Executors.newFixedThreadPool(leader.length);
    for(int i = 0; i < leader.length; i++) {
      leader[i] = new Leader("worker-" + i) ;
      executorService.execute(leader[i]);
      if(i % 10 == 0) Thread.sleep(new Random().nextInt(50));
    }
    executorService.shutdown();
    executorService.awaitTermination(3 * 60 * 1000, TimeUnit.MILLISECONDS);
    System.err.println("End Test----------------------------------------------------");
    registry.disconnect();
  }
  
  public class Leader implements Runnable {
    String name ;
    LeaderElection election;
    int electedCount = 0 ;
    
    public Leader(String name) {
      this.name = name ;
    }
    
    public void run() {
      Registry registry = null;
      try {
        registry = newRegistry().connect();
        Node electionPath =  registry.get(ELECTION_PATH) ;
        election = electionPath.getLeaderElection();
        election.setListener(new LeaderElectionListener() {
          public void onElected() {
            electedCount++ ;
            System.out.println(name + " is elected, elected count = " + electedCount);
          }
        });
        election.start();
        Node node = election.getNode();
        node.setData(name.getBytes());
        Assert.assertEquals(name, new String(node.getData())) ;
        while(electedCount < 10) {
          Thread.sleep(1000);
          if(election.isElected()) {
            election.stop();
            Thread.sleep(500);
            election.start();
          }
        }
        election.stop();
      } catch(Throwable e) {
        e.printStackTrace();
      } finally {
        if(registry != null) {
          try {
            registry.disconnect();
          } catch (RegistryException e) {
            e.printStackTrace();
          }
        }
      }
    }
  }
}
