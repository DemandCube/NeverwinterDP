define([
  'jquery',
  'underscore', 
  'backbone',
  'site/UIBody',
  'text!site/UIBanner.jtpl'
], function($, _, Backbone, UIBody, BannerTmpl) {
  var UIBanner = Backbone.View.extend({
    el: $("#UIBanner"),
    
    initialize: function () {
      _.bindAll(this, 'render') ;
    },
    
    _template: _.template(BannerTmpl),
    
    render: function() {
      var params = { 
      } ;
      $(this.el).html(this._template(params));
    },

    events: {
      'click .onSelectPlugin': 'onSelectPlugin'
    },
    
    onSelectPlugin: function(evt) {
      var name = $(evt.target).attr('plugin') ;
      UIBody.selectPlugin(name);
    }
  });
  
  return UIBanner ;
});
