package com.neverwinterdp.scribengin.client.shell;

import java.util.List;

import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMDescriptor;

public class Formater {

  static public class DataflowList {
    List<DataflowDescriptor> descriptors;

    public DataflowList(List<DataflowDescriptor> descriptors) {
      this.descriptors = descriptors;
    }

    public String format(String title) { return format(title, ""); }

    public String format(String title, String ident) {
      TabularFormater formater = new TabularFormater("Id", "Name", "App Home", "Workers", "Executor Per Worker");
      formater.setIndent("  ");
      for (int i = 0; i < descriptors.size(); i++) {
        DataflowDescriptor descriptor = descriptors.get(i);
        formater.addRow(
            descriptor.getId(),
            descriptor.getName(),
            descriptor.getDataflowAppHome(),
            descriptor.getNumberOfWorkers(),
            descriptor.getNumberOfExecutorsPerWorker()
            );
      }
      formater.setTitle(title);
      return formater.getFormatText();
    }
  }

  static public class VmList {

    private List<VMDescriptor> descriptors;
    String leaderPath;

    public VmList(List<VMDescriptor> descriptors, String leaderPath) {
      this.descriptors = descriptors;
      this.leaderPath = leaderPath;
    }

    public String format(String title) {
      return format(title, "");
    }

    public String format(String title, String ident) {
      TabularFormater formater = 
          new TabularFormater("dataflowName", "CPU Cores", "Memory", "Path", "is Leader");
      formater.setIndent("  ");
      for (VMDescriptor descriptor : descriptors) {
        formater.addRow(descriptor.getVmConfig().getName(),
            descriptor.getCpuCores(),
            descriptor.getMemory(),
            descriptor.getRegistryPath(),
            descriptor.getRegistryPath().equals(leaderPath));
      }
      formater.setTitle(title);
      return formater.getFormatText();
    }
  }
}