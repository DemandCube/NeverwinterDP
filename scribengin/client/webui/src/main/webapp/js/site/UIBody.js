define([
  'jquery',
  'underscore', 
  'backbone',
], function($, _, Backbone) {
  var UIBody = Backbone.View.extend({
    el: $("#UIBody"),
    
    initialize: function () {
      _.bindAll(this, 'render') ;
    },
    
    _template: _.template("<div>Welcome to NeverwinterDP Project</div>"),
    
    render: function() {
      var params = { 
      } ;
      $(this.el).html(this._template(params));
      this.selectPlugin("scribengin");
    },
    
    selectPlugin: function(name) {
      var UIBody = this;
      require(['site/' + name + '/UIBody'], function(UIBodyComponent) { 
        UIBody.setUIBody(UIBodyComponent);
      }) ;
    },

    setUIBody: function(uicomp) {
      this.uicomponent  = uicomp ;
      $(this.el).empty();
      $(this.el).unbind();
      uicomp.setElement($('#UIBody')).render();
      if(uicomp.onActivate != undefined) {
        uicomp.onActivate();
      }
    }
  });
  
  return new UIBody() ;
});
