package com.neverwinterdp.util.io;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public class NetworkUtil {
  static public String getHostname() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostName();
  }

  static public String[] getHostNames() {
    final Set<String> hostNames = new HashSet<String>();
    hostNames.add("localhost");
    try {
      final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      for (final Enumeration<NetworkInterface> ifaces = networkInterfaces; ifaces.hasMoreElements();) {
        final NetworkInterface iface = ifaces.nextElement();
        InetAddress ia = null;
        for (final Enumeration<InetAddress> ips = iface.getInetAddresses(); ips.hasMoreElements();) {
          ia = ips.nextElement();
          hostNames.add(ia.getCanonicalHostName());
        }
      }
    } catch (final SocketException e) {
      throw new RuntimeException("unable to retrieve host names of localhost");
    }
    return hostNames.toArray(new String[hostNames.size()]);
  }
}
