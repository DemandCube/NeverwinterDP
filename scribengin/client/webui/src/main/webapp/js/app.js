define([
  'jquery',
  'util/console',
  'site/UIBanner',
  'site/UIFooter',
  'site/UIBody'
], function($, console, UIBanner, UIFooter, UIBody) {
  var app = {
    view : {
      UIBanner: new UIBanner(),
      UIBody: UIBody,
      UIFooter: new UIFooter(),
    },

    initialize: function() {
      console.log("start initialize app") ;
      this.render() ;
      console.log("finish initialize app") ;
    },

    render: function() {
      this.view.UIBanner.render() ;
      this.view.UIBody.render() ;
      this.view.UIFooter.render() ;
    },

    reload: function() {
      window.location = ROOT_CONTEXT + "/index.html" ;
    }
  } ;
  
  return app ;
});
