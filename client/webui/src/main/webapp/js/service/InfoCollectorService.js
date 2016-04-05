function VisitCookie() {
  this.info = { };
  
  this.info.visitorId = CookieManager.getCookie('visitor.visitorId');
  if(!this.info.visitorId) {
    this.info.visitorId = CookieManager.randomGUID();
    var expireDate = new Date();
    expireDate.setTime(expireDate.getTime() + (1* 24 * 60 * 60 * 1000));
    CookieManager.setCookie('visitor.visitorId', this.info.visitorId, "/", expireDate);
  }

  this.getUserInfo = function() { return this.info; };
}

function GeoLocation() {
  var options = { enableHighAccuracy: true, timeout: 10000, maximumAge: 0 };
  
  var info = {} ;

  function onGeoUpdateSuccess(pos) {
    var crd = pos.coords;
    info.latitude  = crd.latitude ;
    info.longitude = crd.longitude ;
    info.accuracy  = crd.accuracy ;
  };

  function onGeoUpdateError(err) {
    console.warn('ERROR(' + err.code + '): ' + err.message);
  };

  navigator.geolocation.getCurrentPosition(onGeoUpdateSuccess, onGeoUpdateError, options);

  this.info = info ;
}

function InfoCollectorService(serviceUrl) {
  var startTime = new Date().getTime();
  var visitCookie = new VisitCookie();

  var info = {
    user: visitCookie.getUserInfo(),
    
    webpage: {
      url: window.location.href ,
      startVisitTime: startTime,
      endVisitTime: startTime + Math.floor((Math.random() * 100000) + 1)
    },

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

    geoLocation: { 
      region:    geoplugin_region(),
      latitude:  geoplugin_latitude() , 
      longitude: geoplugin_longitude(), 
      accuracy:  0 
    }
  };


  this.info = info;

  this.getInfo = function() { return this.info ; }

  this.pushClientInfo = function() {
    this.info.webpage.endVisitTime = new Date().getTime();
    var url = serviceUrl + "?jsonp=" + encodeURIComponent(JSON.stringify(this.info));
    var img = new Image(100,100);
    img.src = url;
  };
}
