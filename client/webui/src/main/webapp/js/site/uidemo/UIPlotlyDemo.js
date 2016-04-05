define([
  'jquery', 
  'underscore', 
  'backbone',
  'ui/plotly/UIPlotly'
], function($, _, Backbone, UIPlotly) {
  var UIPlotlyDemo = UIPlotly.extend({
    label: 'Plotly Demo',

    config: {
    },
    
    onInit: function(options) {
    }
  });
  return new UIPlotlyDemo({}) ;
});
