define([
  'jquery', 
  'underscore', 
  'backbone',
  "ui/d3/nv/UINVChart"
], function($, _, Backbone, UINVChart) {

  var UINVMultiChart = UINVChart.extend({

    setData: function (data) { this.data = data ; },

    addChartData: function (name, xyCoordinates) { 
      var chartData = {
        key: name,
        values: xyCoordinates
      };
      this.data.push(chartData) ; 
    },

    clearChartData: function () { this.data = []; },

    createChart: function(config, chartData) {
      var chart = nv.models.multiChart().margin({top: 30, right: 60, bottom: 50, left: 70}).color(d3.scale.category10().range());

      chart.xAxis.tickFormat(d3.format(',f'));
      chart.yAxis1.tickFormat(d3.format(',.1f'));
      chart.yAxis2.tickFormat(d3.format(',.1f'));

      d3.select('#' + config.id + ' svg').datum(chartData).transition().duration(500).call(chart);

      return chart;
    }
  });
  
  return UINVMultiChart ;
});
