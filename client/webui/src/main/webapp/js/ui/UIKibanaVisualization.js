define([
  'jquery',
  'underscore', 
  'backbone',
  'text!ui/UIKibanaVisualization.jtpl'
], function($, _, Backbone, Template) {
  var UIKibanaVisualization = Backbone.View.extend({
    
    initialize: function (options) {
      this.label = options.label;
      this.url = options.url;
      this.url = this.url.replace(/http:\/\/.*?\//, options.server);
      console.log("url = " + this.url); 
      _.bindAll(this, 'render') ;
    },
    
    _template: _.template(Template),

    render: function() {
      var params = { url: this.url } ;
      $(this.el).html(this._template(params));
    },

    events: {
      'click .onSelectTimeLast15min': 'onSelectTimeLast15min',
      'click .onSelectTimeLast30min': 'onSelectTimeLast30min',

      'click .onSelectTimeLast1hour':  'onSelectTimeLast1hour',
      'click .onSelectTimeLast2hour':  'onSelectTimeLast2hour',
      'click .onSelectTimeLast4hour':  'onSelectTimeLast4hour',
      'click .onSelectTimeLast12hour': 'onSelectTimeLast12hour',
      'click .onSelectTimeLast24hour': 'onSelectTimeLast24hour',

      'click .onSelectTimeLast3day':  'onSelectTimeLast3day',
      'click .onSelectTimeLast7day':  'onSelectTimeLast7day',
      'click .onSelectTimeLast30day': 'onSelectTimeLast30day',
      'click .onSelectTimeLast60day': 'onSelectTimeLast60day',
      'click .onSelectTimeLast90day': 'onSelectTimeLast90day',

      'click .onSelectTimeAll': 'onSelectTimeAll',

      'click .onRefresh': 'onRefresh',
      'click .onEditVisualization': 'onEditVisualization'
    },

    onSelectTimeLast15min: function(evt) { this.setTimeParam("time:(from:now-15m,mode:quick,to:now)"); },
    onSelectTimeLast30min: function(evt) { this.setTimeParam("time:(from:now-30m,mode:quick,to:now)"); },

    onSelectTimeLast1hour: function(evt) { this.setTimeParam("time:(from:now-1h,mode:quick,to:now)"); },
    onSelectTimeLast2hour: function(evt) { this.setTimeParam("time:(from:now-2h,mode:quick,to:now)"); },
    onSelectTimeLast4hour: function(evt) { this.setTimeParam("time:(from:now-4h,mode:quick,to:now)"); },
    onSelectTimeLast12hour: function(evt) { this.setTimeParam("time:(from:now-12h,mode:quick,to:now)"); },
    onSelectTimeLast24hour: function(evt) { this.setTimeParam("time:(from:now-24h,mode:quick,to:now)"); },

    onSelectTimeLast3day: function(evt) { this.setTimeParam("time:(from:now-3d,mode:quick,to:now)"); },
    onSelectTimeLast7day: function(evt) { this.setTimeParam("time:(from:now-7d,mode:quick,to:now)"); },
    onSelectTimeLast30day: function(evt) { this.setTimeParam("time:(from:now-30d,mode:quick,to:now)"); },
    onSelectTimeLast60day: function(evt) { this.setTimeParam("time:(from:now-60d,mode:quick,to:now)"); },
    onSelectTimeLast90day: function(evt) { this.setTimeParam("time:(from:now-90d,mode:quick,to:now)"); },

    onSelectTimeAll: function(evt) { this.setTimeParam("time:(from:now-10y,mode:quick,to:now)"); },

    onEditVisualization: function(evt) { 
      var newUrl = this.url.replace(/embed&/, '');
      window.open(newUrl,'_blank');
    },

    onRefresh: function(timeParam) { 
      this.render();
    },


    setTimeParam: function(timeParam) { 
      this.url = this.url.replace(/time:\(.*to:now\)/, timeParam);
      console.log("new url = " + this.url);
      this.render();
    }
  });
  
  return UIKibanaVisualization ;
});
