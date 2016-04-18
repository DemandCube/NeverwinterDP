package com.neverwinterdp.scribengin.webui;

import java.util.LinkedHashMap;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.registry.txevent.TXEvent;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.dataflow.DataStreamOperatorReportWithStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowClient;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowEvent;
import com.neverwinterdp.scribengin.dataflow.runtime.master.DataflowMasterRuntimeReport;
import com.neverwinterdp.scribengin.dataflow.runtime.worker.DataflowWorkerRuntimeReport;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowCommand {
  static public class ListActive extends Command {
    @Override
    public List<DataflowDescriptor> execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      return scribenginClient.getActiveDataflowDescriptors();
    }
  }
  
  static public class ListHistory extends Command {
    @Override
    public List<DataflowDescriptor> execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      return scribenginClient.getHistoryDataflowDescriptors();
    }
  }
  
  static public class Info extends Command {
    @Parameter(names = "--dataflowId", required=true, description="")
    String dataflowId;
    
    @Override
    public DataflowDescriptor execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      return dflClient.getDataflowRegistry().getConfigRegistry().getDataflowDescriptor();
    }
  }
  
  static public class Resume extends Command {
    @Parameter(names = "--dataflowId", required=true, description="")
    String dataflowId;
    
    @Override
    public Boolean execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      return scribenginClient.resume(dataflowId);
    }
  }
  
  static public class Stop extends Command {
    @Parameter(names = "--dataflowId", required=true, description="")
    String dataflowId;
    
    @Override
    public Boolean execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      TXEvent stopEvent = new TXEvent("stop", DataflowEvent.Stop);
      dflClient.getDataflowRegistry().getMasterRegistry().getMasterEventBroadcaster().broadcast(stopEvent);
      return true;
    }
  }
  
  static public class Report extends Command {
    @Parameter(names = "--dataflowId", required=true, description="")
    String dataflowId;
    
    @Override
    public DataflowDescriptor execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      return dflClient.getDataflowRegistry().getConfigRegistry().getDataflowDescriptor();
    }
  }
  
  static public class OperatorReport extends Command {
    @Parameter(names = "--dataflowId", required=true, description="")
    String dataflowId;
    
    @Parameter(names = "--groupBy", description="")
    String groupBy = "operator";
    
    @Override
    public LinkedHashMap<String, List<DataStreamOperatorReportWithStatus>> execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      if("executor".equals(groupBy)) {
        return dflClient.getDataflowRegistry().getTaskRegistry().getDataStreamOperatorReportGroupByExecutor();
      } else {
        return dflClient.getDataflowRegistry().getTaskRegistry().getDataStreamOperatorReportGroupByOperator();
      }
    }
  }
  
  static public class MasterReport extends Command {
    @Parameter(names = "--dataflowId", required=true, description="")
    String dataflowId;
    
    @Override
    public List<DataflowMasterRuntimeReport> execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      return dflClient.getDataflowRegistry().getMasterRegistry().getDataflowMasterRuntimeReports();
    }
  }
  
  static public class MasterKill extends Command {
    @Parameter(names = "--dataflowId", required=true, description="")
    String dataflowId;
    
    @Parameter(names = "--vmId", required=true, description="")
    private String vmId;
    
    @Override
    public Boolean execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      VMDescriptor vmDescriptor = dflClient.findActiveDataflowMaster(vmId);
      if(vmDescriptor != null) {
        VMClient vmClient = scribenginClient.getVMClient();
        if(vmClient.isLocalVMClient()) {
          return vmClient.simulateKill(vmDescriptor);
        } else {
          return vmClient.kill(vmDescriptor);
        }
      }
      return false;
    }
  }
  
  static public class WorkerReport extends Command {
    @Parameter(names = "--dataflowId", required=true, description="")
    String dataflowId;
    
    @Parameter(names = "--groupBy", required=true, description="")
    String groupBy = "active";
    
    @Override
    public List<DataflowWorkerRuntimeReport> execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      if("all".equals(groupBy)) {
        return dflClient.getDataflowRegistry().getWorkerRegistry().getAllDataflowWorkerRuntimeReports();
      } else if("history".equals(groupBy)) {
        return dflClient.getDataflowRegistry().getWorkerRegistry().getHistoryDataflowWorkerRuntimeReports();
      } else {
        return dflClient.getDataflowRegistry().getWorkerRegistry().getActiveDataflowWorkerRuntimeReports();
      }
    }
  }
  
  static public class WorkerKill extends Command {
    @Parameter(names = "--dataflowId", required=true, description="")
    String dataflowId;
    
    @Parameter(names = "--vmId", required=true, description="")
    private String vmId;
    
    @Override
    public Boolean execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      DataflowClient dflClient = scribenginClient.getDataflowClient(dataflowId);
      VMDescriptor vmDescriptor = dflClient.findActiveDataflowWorker(vmId);
      if(vmDescriptor != null) {
        VMClient vmClient = scribenginClient.getVMClient();
        if(vmClient.isLocalVMClient()) {
          return vmClient.simulateKill(vmDescriptor);
        } else {
          return vmClient.kill(vmDescriptor);
        }
      }
      return false;
    }
  }
}