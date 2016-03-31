function ADSService(serviceUrl) {
  this.visitorId = CookieManager.getCookie('ads.visitorId');
  if(!this.visitorId) {
    this.visitorId = CookieManager.randomGUID();
  }

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
