package com.neverwinterdp.vm.client.shell;

public class PluginCommand extends Command {
  public void execute(Shell shell, CommandInput cmdInput) throws Exception {
    String pluginClass = cmdInput.getSubCommand();
    Class<? extends SubCommand> type = (Class<? extends SubCommand>) Class.forName(pluginClass) ;
    SubCommand subcommand = type.newInstance();
    cmdInput.mapRemainArgs(subcommand);
    subcommand.execute(shell, cmdInput);
  }

  @Override
  public String getDescription() { return "Execute the plugin class"; }
}
