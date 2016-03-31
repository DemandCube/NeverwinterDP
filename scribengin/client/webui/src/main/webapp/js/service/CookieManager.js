function CookieManager() {
  this.cookies = {};

  this.parseCookie = function() {
    var cookie = document.cookie.split(';');
    var info = {} ;
    for(var i = 0; i < cookie.length; i++) {
      var nameValue = cookie[i].trim();
      console.log("cookie name value = " + nameValue);
      var pair  = nameValue.split('=');
      var name  = pair[0];
      var value = pair[1];
      this.cookies[name] = value;
    }
  };

  this.getCookie = function(name) { return this.cookies[name]; };

  this.setCookie = function(name, value, path, expireDate) {
    if(name == null || path == null) {
      throw new Error("Cookie name or path cannot be null");
    }
    var cookie = name + '=' + value;
    if(expireDate != null) {
      cookie = cookie  + ";" + "expires=" + expireDate.toUTCString();
    }
    cookie = cookie + ";path=" + path;
    document.cookie = cookie;
    console.log("set cookie " + cookie) ;
  };

  this.deleteCookie = function(name) {
    this.cookies[name] = null;
    document.cookie = name + "=;expires=Thu, 01 Jan 1970 00:00:00 GMT";
  };

  this.deleteAllCookies = function() {
    var cookies = document.cookie.split(";");
    for (var i = 0; i < cookies.length; i++) {
      var cookie = cookies[i];
      var eqPos = cookie.indexOf("=");
      var name = eqPos > -1 ? cookie.substr(0, eqPos) : cookie;
      document.cookie = name + "=;expires=Thu, 01 Jan 1970 00:00:00 GMT";
    }
  };

  this.randomGUID = function() {
    function s4() { return Math.floor((1 + Math.random()) * 0x10000).toString(16).substring(1); }
    return s4() + s4() + '-' + s4() + '-' + s4() + '-' + s4() + '-' + s4() + s4() + s4();
  };

  this.parseCookie();
};

var CookieManager = new CookieManager();
