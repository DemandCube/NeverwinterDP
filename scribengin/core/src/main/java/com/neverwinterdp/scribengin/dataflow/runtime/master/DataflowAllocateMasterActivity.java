package com.neverwinterdp.scribengin.dataflow.runtime.master;

import java.util.List;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowSubmitter;
import com.neverwinterdp.scribengin.dataflow.registry.DataflowRegistry;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMConfig.ClusterEnvironment;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.LocalVMClient;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.YarnVMClient;

public class DataflowAllocateMasterActivity implements DataflowMasterActivity {
  private MasterService service ;
  
  public DataflowAllocateMasterActivity(MasterService service) {
    this.service = service;
  }
  
  public void execute() throws Exception {
    DataflowRegistry dflRegistry = service.getDataflowRegistry();
    List<VMDescriptor> vmDescriptors = dflRegistry.getMasterRegistry().getMasterVMDescriptors();
    if(vmDescriptors.size() < 2) {
      VMClient vmClient = null;
      VMConfig currentVMConfig = service.getVMConfig();
      if(currentVMConfig.getClusterEnvironment() ==  ClusterEnvironment.JVM) {
        vmClient = new LocalVMClient(dflRegistry.getRegistry());
      } else {
        vmClient = new YarnVMClient(dflRegistry.getRegistry(), currentVMConfig.getClusterEnvironment(), currentVMConfig.getHadoopProperties());
      }
      ScribenginClient   scribenginClient = new ScribenginClient(vmClient);
      DataflowDescriptor descriptor = dflRegistry.getConfigRegistry().getDataflowDescriptor();
      DataflowSubmitter  submitter = new DataflowSubmitter(scribenginClient, descriptor);
      submitter.submit();
      submitter.waitForMasterRunning(60000);
    }
  }
}