package com.neverwinterdp.scribengin.webui;

import java.util.List;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class VMCommand {
  static public class ListActive extends Command {
    @Override
    public List<VMDescriptor>  execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      VMClient vmClient = scribenginClient.getVMClient();
      return vmClient.getActiveVMDescriptors();
    }
  }
  
  static public class ListHistory extends Command {
    @Override
    public List<VMDescriptor>  execute(CommandContext ctx) throws Exception {
      ScribenginClient scribenginClient = ctx.getScribenginClient();
      VMClient vmClient = scribenginClient.getVMClient();
      return vmClient.getHistoryVMDescriptors();
    }
  }
}