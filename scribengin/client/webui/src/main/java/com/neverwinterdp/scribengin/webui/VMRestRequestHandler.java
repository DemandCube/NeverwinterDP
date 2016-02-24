package com.neverwinterdp.scribengin.webui;

import com.neverwinterdp.scribengin.ScribenginClient;

public class VMRestRequestHandler extends RestRequestHandler {
  public VMRestRequestHandler(ScribenginClient client) {
    super(client);
    addCommand("list-active",  VMCommand.ListActive.class);
    addCommand("list-history", VMCommand.ListHistory.class);
  }
} 