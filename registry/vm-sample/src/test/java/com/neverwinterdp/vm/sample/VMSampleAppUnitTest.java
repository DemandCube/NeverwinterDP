package com.neverwinterdp.vm.sample;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.tool.LocalVMCluster;

public class VMSampleAppUnitTest  {
  LocalVMCluster  vmCluster ;
  
  @Before
  public void setup() throws Exception {
    vmCluster = new LocalVMCluster("build/vm-cluster") ;
    vmCluster.clean();
    vmCluster.start();
  }
  
  @After
  public void teardown() throws Exception {
    vmCluster.shutdown();
  }
  
  @Test
  public void testMaster() throws Exception {
    VMClient vmClient = vmCluster.getVMClient();
    Shell shell = new Shell(vmClient) ;
    String command =
      "vm submit " +
      "  --dfs-app-home /opt/scribengin/vm-sample" +
      "  --name vm-dummy-1 --role vm-dummy" +
      "  --registry-connect 127.0.0.1:2181 --registry-db-domain /NeverwinterDP --registry-implementation " + RegistryImpl.class.getName() +
      "  --vm-application " + VMSampleApp.class.getName();
    System.err.println(command);
    shell.execute(command);
    Thread.sleep(10000);
  }
}