define([
  'jquery', 
  'underscore', 
  'backbone',
  "nv",
  "css!../../../libs/d3/nv/nv.d3.min.css"
], function($, _, Backbone, nv) {
  var UINVChart = Backbone.View.extend({
    data:  [],

    initialize: function (options) {
      if(!options) options = {};
      if(!options.width)  options.width  = '100%';
      if(!options.height) options.height = '600px';
      if(!options.id)     options.id     = 'UINVBarChart';

      if(this.onInit) this.onInit(options);
      this.config = options ;
      _.bindAll(this, 'render');
    },

    _template: _.template(
      "<div id='<%=config.id%>' style='width: <%=config.width%>; height: <%=config.height%>'><svg></svg></div>"
    ),

    render: function() {
      var params = { config: this.config };
      $(this.el).html(this._template(params));
      var chart = this.createChart(this.config, this.data) ;
      nv.addGraph(function() { return chart; });
    },

    createChart: function() { 
      throw new Error('This method should be overrided'); 
    }
  });
  
  return UINVChart ;
});
