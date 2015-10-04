package com.neverwinterdp.scribengin.shell;

import com.neverwinterdp.vm.client.shell.Command;
import com.neverwinterdp.vm.client.shell.CommandInput;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.client.shell.SubCommand;

public class DataflowCommand extends Command {
  public DataflowCommand() {
    add("list", List.class) ;
    add("submit", Submit.class) ;
    add("info", Info.class) ;
  }
  
  @Override
  public String getDescription() {
        return "commands for interacting with dataflows";
  }
  
  static public class List extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    }

    @Override
    public String getDescription() {
      return "List all the dataflows";
    }
  }
  
  static public class Submit extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    }

    @Override
    public String getDescription() {
      return "Submit a dataflow";
    }
  }
  
  static public class Info extends SubCommand {
    @Override
    public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    }

    @Override
    public String getDescription() {
      return "Display the information of a dataflow";
    }
  }
}