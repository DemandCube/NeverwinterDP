define([
  'jquery', 
  'underscore', 
  'backbone',
  "ui/d3/nv/UINVChart"
], function($, _, Backbone, UINVChart) {

  var UINVLinePlusBarChart = UINVChart.extend({
    setData: function (data) { this.data = data ; },

    createChart: function(config, chartData) {
      var chart = nv.models.linePlusBarChart();
      chart.
        margin({top: 50, right: 80, bottom: 30, left: 80}).
        legendRightAxisHint(' [Using Right Axis]').
        color(d3.scale.category10().range());

      chart.xAxis.tickFormat(function(d) { return d3.time.format('%x')(new Date(d)) }).showMaxMin(false);

      chart.y2Axis.tickFormat(function(d) { return '$' + d3.format(',f')(d) });
      chart.bars.forceY([0]).padData(false);

      chart.x2Axis.tickFormat(function(d) { return d3.time.format('%x')(new Date(d)) }).showMaxMin(false);

      d3.select('#' + config.id + ' svg').datum(chartData).transition().duration(500).call(chart);

      nv.utils.windowResize(chart.update);

      chart.dispatch.on('stateChange', function(e) { nv.log('New State:', JSON.stringify(e)); });

      return chart;
    }
  });
  
  return UINVLinePlusBarChart ;
});
