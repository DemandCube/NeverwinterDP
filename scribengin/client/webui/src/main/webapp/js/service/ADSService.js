function ADSVisitCookie() {
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
      if('adsVisitorId' == name) info.adsVisitorId = value;
      console.log("=> " + nameValue);
    }
    if(!info.adsVisitorId) info.adsVisitorId = guid();
    return info;
  };

  this.info = parseCookie();

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

  this.getInfo = function() { return this.info; };
}

function ADSService(serviceUrl) {
  var visitCookie = new ADSVisitCookie();

  this.visitorId = visitCookie.getInfo().adsVisitorId

  this.onClickADS = function(name, adUrl) {
    var adsEvent = {
      name: name,
      visitorId:  this.visitorId,
      adUrl:      adUrl,
      webpageUrl: window.location.href 
    };

    var url = serviceUrl + "?jsonp=" + encodeURIComponent(JSON.stringify(adsEvent));
    var ele = document.createElement("iframe");
    ele.src = url;
    ele.width  = 0;
    ele.height = 0;
    document.getElementsByTagName('body')[0].appendChild(ele);

    var newTab = window.open(adUrl, '_blank');
    newTab.focus();
  };
}
