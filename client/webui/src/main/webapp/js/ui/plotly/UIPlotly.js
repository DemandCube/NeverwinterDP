define([
  'jquery', 
  'underscore', 
  'backbone',
  "plotly"
], function($, _, Backbone, plotly) {
  var UIPlotly = Backbone.View.extend({
    initialize: function (config) {
    },

    _template: _.template(
      "<div id='plotly' style='width: 900px; height: 600px'></div>"
    ),

    render: function() {
      var params = { };
      $(this.el).html(this._template(params));

      var trace1 = {
        x: [1, 2, 3, 4],
        y: [10, 15, 13, 17],
        type: 'scatter'
      };

      var trace2 = {
        x: [1, 2, 3, 4],
        y: [16, 5, 11, 9],
        type: 'scatter'
      };

      var data = [trace1, trace2];

      plotly.newPlot('plotly', data);
    }
  });
  
  return UIPlotly ;
});
