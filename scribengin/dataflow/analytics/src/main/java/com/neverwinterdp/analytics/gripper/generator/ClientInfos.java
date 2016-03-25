package com.neverwinterdp.analytics.gripper.generator;

import java.util.Random;

import com.neverwinterdp.netty.http.client.ClientInfo;

public class ClientInfos {
  static String[] regions = {
    "Ha Noi", "Paris", "London", "San Francisco", "San Jose", "San Diego", "Dallas", "New York"
  };
  
  static public ClientInfo nextRandomClientInfo() {
    ClientInfo[] all = {
      createMacbookAirClientInfo(), createIphone6SClientInfo(), createRedmiNoteClientInfo(), createXRobotClientInfo()
    } ;
    int sel = new Random().nextInt(all.length);
    ClientInfo selClientInfo = all[sel];
    return selClientInfo;
  }
  
  static public ClientInfo createMacbookAirClientInfo() {
    ClientInfo clientInfo = new ClientInfo(true);
    clientInfo.navigator.platform = "MacIntel";
    clientInfo.navigator.appCodeName =  "Mozilla";
    clientInfo.navigator.appName = "Netscape";
    clientInfo.navigator.appVersion = "5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36";
    clientInfo.navigator.cookieEnabled = true;
    clientInfo.navigator.userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36";
    clientInfo.navigator.language = "en-US";
    clientInfo.screen.width  = 1440;
    clientInfo.screen.height = 900;
    clientInfo.window.width  = 900;
    clientInfo.window.height = 750;
    clientInfo.geoLocation.region = getRandomRegion();
    return clientInfo;
  }
  
  static public ClientInfo createIphone6SClientInfo() {
    ClientInfo clientInfo = new ClientInfo(true);
    clientInfo.navigator.platform = "MacIntel";
    clientInfo.navigator.appCodeName =  "Mozilla";
    clientInfo.navigator.appName = "Netscape";
    clientInfo.navigator.appVersion = "5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36";
    clientInfo.navigator.cookieEnabled = true;
    clientInfo.navigator.userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36";
    clientInfo.navigator.language = "en-US";
    clientInfo.screen.width  = 1440;
    clientInfo.screen.height = 900;
    clientInfo.window.width  = 900;
    clientInfo.window.height = 750;
    clientInfo.geoLocation.region = getRandomRegion();
    return clientInfo;
  }
  
  static public ClientInfo createRedmiNoteClientInfo() {
    ClientInfo clientInfo = new ClientInfo(true);
    clientInfo.navigator.platform = "Linux aarch64";
    clientInfo.navigator.appCodeName =  "Mozilla";
    clientInfo.navigator.appName = "Netscape";
    clientInfo.navigator.appVersion = "5.0 (Linux; U; Android 5.0.2; en-us; Redmi Note 3 Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/42.0.0.0 Mobile Safari/537.36 XiaoMi/MiuiBrowser/2.1.1";
    clientInfo.navigator.cookieEnabled = true;
    clientInfo.navigator.userAgent = "Mozilla/5.0 (Linux; U; Android 5.0.2; en-us; Redmi Note 3 Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/42.0.0.0 Mobile Safari/537.36 XiaoMi/MiuiBrowser/2.1.1";
    clientInfo.navigator.language = "en-US";
    clientInfo.screen.width  = 1440;
    clientInfo.screen.height = 900;
    clientInfo.window.width  = 900;
    clientInfo.window.height = 750;
    clientInfo.geoLocation.region = getRandomRegion();
    return clientInfo;
  }
  
  static public ClientInfo createXRobotClientInfo() {
    ClientInfo clientInfo = new ClientInfo(true);
    clientInfo.navigator.platform = "MacIntel";
    clientInfo.navigator.appCodeName =  "Mozilla";
    clientInfo.navigator.appName = "Crawler";
    clientInfo.navigator.appVersion = "5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36";
    clientInfo.navigator.cookieEnabled = true;
    clientInfo.navigator.userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36";
    clientInfo.navigator.language = "en-US";
    clientInfo.geoLocation.region = getRandomRegion();
    return clientInfo;
  }
  
  static Random rand = new Random();

  static String getRandomRegion() { 
    return regions[rand.nextInt(regions.length)]; 
  }
}
