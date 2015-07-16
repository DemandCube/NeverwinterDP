package com.neverwinterdp.scribengin.shell;

import java.util.ArrayList;
import java.util.List;

public class ExecutorScheduler {
  private List<GroupExecutor> groupExecutors = new ArrayList<>();
  
  public GroupExecutor newGroupExcecutor(String name) {
    GroupExecutor group = new GroupExecutor(name) ;
    groupExecutors.add(group) ;
    return group ;
  }
  
  public void run() throws Exception {
    for(int i = 0; i < groupExecutors.size(); i++) {
      long start = System.currentTimeMillis();
      GroupExecutor groupExecutor = groupExecutors.get(i);
      groupExecutor.execute();
      groupExecutor.waitForReady();
      groupExecutor.awaitTermination();
      System.out.println("Group Executor " + groupExecutor.getName() + "  Run In " + (System.currentTimeMillis() - start) + "ms");
    }
  }
}
