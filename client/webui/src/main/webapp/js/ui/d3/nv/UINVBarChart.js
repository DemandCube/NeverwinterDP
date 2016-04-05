define([
  'jquery', 
  'underscore', 
  'backbone',
  "ui/d3/nv/UINVChart"
], function($, _, Backbone, UINVChart) {

  var UINVBarChart = UINVChart.extend({

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
      var chart = nv.models.multiBarChart();
      chart.
        barColor(d3.scale.category20().range()).
        duration(300).margin({bottom: 50, left: 50}).
        rotateLabels(45).
        groupSpacing(0.1) ;

      chart.reduceXTicks(false).staggerLabels(true);

      chart.xAxis.axisLabel("ID of Furry Cat Households");
      chart.xAxis.axisLabelDistance(35).showMaxMin(false).tickFormat(d3.format(',.3f')) ;

      chart.yAxis.axisLabel("Change in Furry Cat Population").axisLabelDistance(-5).tickFormat(d3.format(',.01f')) ;

      chart.dispatch.on('renderEnd', function() { nv.log('Render Complete'); });

      d3.select('#' + config.id + ' svg').datum(chartData).call(chart);

      nv.utils.windowResize(chart.update);

      chart.dispatch.on('stateChange', function(e) { nv.log('New State:', JSON.stringify(e)); });

      chart.state.dispatch.on('change', function(state) { nv.log('state', JSON.stringify(state)); });

      return chart;
    }
  });
  
  return UINVBarChart ;
});
