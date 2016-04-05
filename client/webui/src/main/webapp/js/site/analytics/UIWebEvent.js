define([
  'jquery',
  'underscore', 
  'backbone',
], function($, _, Backbone) {
  var UIWebEvent = Backbone.View.extend({
    label: "Web Event Test Page",

    initialize: function (options) {
      _.bindAll(this, 'render') ;
    },
    
    _template: _.template(
      "<iframe height='600' width='100%' src='/web-event.html' style='border: 1px dashed lightgray'></iframe>"
    ),

    render: function() {
      var params = {  } ;
      $(this.el).html(this._template(params));
    }
  });
  
  return UIWebEvent ;
});
