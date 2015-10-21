package com.neverwinterdp.vm.client.shell;

import java.io.IOException;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.JsonNode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.client.VMClient;

public class RegistryCommand extends Command {
  public RegistryCommand() {
    add("dump", Dump.class);
    add("info", NodeInfo.class);
  }

  static public class NodeInfo extends SubCommand {
    @Parameter(names = "--path", description = "The path to dump, the default path is root /")
    private String path = "/";
    
    @Parameter(names = "--print-data-as-json", description = "Print data as json")
    private boolean printDataAsJson = false;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      Registry registry = vmClient.getRegistry();
      shell.console.println("Node " + path);
      boolean exists = registry.exists(path);
      shell.console.println("  Exists: " + exists);
      if(!exists) return ;
      if(printDataAsJson) {
        Node node = registry.get(path);
        byte[] data = node.getData();
        if(data != null && data.length > 0) {
          JsonNode jsonNode = JSONSerializer.INSTANCE.fromString(new String(data));
          String prettyJson = JSONSerializer.INSTANCE.toString(jsonNode);
          shell.console.println(prettyJson);
        }
      }
    }
    
    @Override
    public String getDescription() {
      return "dump contents of the registry path";
    }
  }
  
  static public class Dump extends SubCommand {
    @Parameter(names = "--path", description = "The path to dump, the default path is root /")
    private String path = "/";

    @Parameter(names = "--max-print-data-length", description = "The max number of characters to print")
    private int maxPrintDataLength = 80;
    
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
      VMClient vmClient = shell.getVMClient();
      Registry registry = vmClient.getRegistry();
      shell.console().println(path);
      List<String> nodes = registry.getChildren(path);
      for (String node : nodes) {
        dump(path, node, registry, shell.console(), "  ");
      }

    }

    private void dump(String parent, String node, Registry registry, Console console,
        String indentation) throws IOException, RegistryException {
      // During the recursive traverse, a node can be added or removed by the
      // other process
      // So we can ignore all the No node exists exception
      String path = parent + "/" + node;
      if ("/".equals(parent))
        path = "/" + node;
      byte[] data = {};
      try {
        data = registry.getData(path);
      } catch (RegistryException ex) {
      }
      String stringData = "";
      if (data != null && data.length > 0) {
        stringData = " - " + new String(data);
        stringData = stringData.replace("\r\n", " ");
        stringData = stringData.replace("\n", " ");
        if (stringData.length() > maxPrintDataLength) {
          stringData = stringData.substring(0, maxPrintDataLength);
        }
      }
      console.println(indentation + node + stringData);
      List<String> children = null;
      try {
        children = registry.getChildren(path);
      } catch (RegistryException ex) {
      }
      if (children != null) {
        for (String child : children) {
          dump(path, child, registry, console, indentation + "  ");
        }
      }
    }

    @Override
    public String getDescription() {
      return "dump contents of the registry path";
    }
  }

  @Override
  public String getDescription() {
    return "Commands for querying the registry";
  }
}
