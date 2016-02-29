function VisitCookie() {
  var guid = function() {
    function s4() {
      return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1);
    }
    return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
  };

  var  deleteAllCookies = function() {
    var cookies = document.cookie.split(";");
    for (var i = 0; i < cookies.length; i++) {
      var cookie = cookies[i];
      var eqPos = cookie.indexOf("=");
      var name = eqPos > -1 ? cookie.substr(0, eqPos) : cookie;
      document.cookie = name + "=;expires=Thu, 01 Jan 1970 00:00:00 GMT";
    }
  }

  
  var parseCookie = function() {
    var cookies = document.cookie ;
    var cookie = cookies.split(';');
    var info = {} ;
    for(var i = 0; i < cookie.length; i++) {
      var nameValue = cookie[i].trim();
      var pair  = nameValue.split('=');
      var name  = pair[0];
      var value = pair[1];
      if('userId' == name) info.userId = value;
      else if('visitorId' == name) info.visitorId = value;
      console.log("=> " + nameValue);
    }
    if(!info.visitorId) info.visitorId = guid();
    return info;
  };

  this.userInfo = parseCookie();

  this.update = function() {
    document.cookie = '';
    var expireDate = new Date();
    expireDate.setTime(expireDate.getTime() + (1* 24 * 60 * 60 * 1000));
    var expires = "expires=" + expireDate.toUTCString();
    var string = '';
    for (var key in this.userInfo) {
      if(this.userInfo.hasOwnProperty(key)) {
        string += key + '=' + this.userInfo[key] + ";";
        document.cookie = key + '=' + this.userInfo[key] + "; expires="+ expires + "; path=/";
        console.log("add " + document.cookie) ;
      }
    }
    //document.cookie = string + "expires="+ expires + "; path=/";
  };

  deleteAllCookies();
  this.update();

  this.getUserInfo = function(token) { return this.userInfo; };
}

function InfoCollectorService(serviceUrl) {
  var visitCookie = new VisitCookie();

  var info = {
    user: visitCookie.getUserInfo(),

    navigator: {
      platform:      navigator.platform,
      appCodeName:   navigator.appCodeName,
      appName:       navigator.appName,
      appVersion:    navigator.appVersion,
      cookieEnabled: navigator.cookieEnabled,
      userAgent:     navigator.userAgent,
      language:      navigator.language,
      languages:     navigator.languages
    },

    screen: { width: screen.width, height:  screen.height },

    window: {
     width:   window.innerWidth || document.documentElement.clientWidth || document.body.clientWidth,
     height:  window.innerHeight || document.documentElement.clientHeight || document.body.clientHeight
    },

    geoLocation: { latitude: 0, longitude: 0, accuracy:  0 }
  };

  var pushClientInfo = function(info) {
    var url = serviceUrl + "?jsonp=" + encodeURIComponent(JSON.stringify(info));
    /*
    var ele = document.createElement("script");
    ele.src = url;
    ele.type = "text/javascript";
    document.getElementsByTagName('head')[0].appendChild(ele);
    */
    var ele = document.createElement("iframe");
    ele.src = url;
    ele.width  = 0;
    ele.height = 0;
    document.getElementsByTagName('body')[0].appendChild(ele);
  };

  var options = { enableHighAccuracy: true, timeout: 5000, maximumAge: 0 };

  function onGeoUpdateSuccess(pos) {
    var crd = pos.coords;
    info.geoLocation.latitude  = crd.latitude ;
    info.geoLocation.longitude = crd.longitude ;
    info.geoLocation.accuracy  = crd.accuracy ;
    pushClientInfo(info);
  };

  function onGeoUpdateError(err) {
    //console.warn('ERROR(' + err.code + '): ' + err.message);
    pushClientInfo(info);
  };

  navigator.geolocation.getCurrentPosition(onGeoUpdateSuccess, onGeoUpdateError, options);
   
  this.info = info;

  this.getInfo = function() { return this.info ; }
}
