define([
  'jquery',
  'underscore', 
  'backbone',
  'Env',
  'ui/UIBreadcumbs',
  'ui/UIKibanaVisualization',
  'site/scribengin/UIListDataflow',
  'text!site/scribengin/UIBody.jtpl'
], function($, _, Backbone, Env, UIBreadcumbs, UIKibanaVisualization, UIListDataflow, Template) {
  var UIBody = Backbone.View.extend({
    el: $("#UIBody"),
    
    initialize: function () {
      _.bindAll(this, 'render') ;
    },
    
    _template: _.template(Template),

    render: function() {
      var params = { } ;
      $(this.el).html(this._template(params));
    },

    events: {
      'click .onSelectUIActiveDataflow':  'onSelectUIActiveDataflow',
      'click .onSelectUIHistoryDataflow': 'onSelectUIHistoryDataflow',

      'click .onLogDetail': 'onLogDetail',

      'click .onJVMCpu': 'onJVMCpu',
      'click .onJVMMemory': 'onJVMMemory',
      'click .onJVMGC': 'onJVMGC',
      'click .onJVMJHiccup': 'onJVMJHiccup',

      'click .onRecordThroughput': 'onRecordThroughput',
      'click .onByteThroughput': 'onByteThroughput'

    },
    
    onSelectUIActiveDataflow: function(evt) {
      var uiListDataflow = new UIListDataflow();
      uiListDataflow.initActive();
      this._workspace(uiListDataflow);
    },

    onSelectUIHistoryDataflow: function(evt) {
      var uiListDataflow = new UIListDataflow();
      uiListDataflow.initHistory();
      this._workspace(uiListDataflow);
    },

    onLogDetail: function(evt) {
      var kibanaSharedUrl = "http://188.166.155.208:5601/#/visualize/edit/Scribengin-Log4j-Detail?embed&_g=(refreshInterval:(display:Off,pause:!f,section:0,value:0),time:(from:now-12h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'*')),vis:(aggs:!((id:'1',params:(),schema:metric,type:count),(id:'2',params:(field:host,order:desc,orderBy:'1',row:!f,size:25),schema:split,type:terms),(id:'3',params:(field:loggerName,order:desc,orderBy:'1',size:50),schema:group,type:terms),(id:'4',params:(field:level,order:desc,orderBy:'1',size:5),schema:segment,type:terms)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,mode:stacked,scale:linear,setYExtents:!f,shareYAxis:!t,times:!(),yAxis:()),type:histogram))";
      var uiKibanaVisualization = new UIKibanaVisualization({ label: "Log Detail Summary", server: Env.kibana.server, url: kibanaSharedUrl });
      uiKibanaVisualization.onSelectTimeAll();
      this._workspace(uiKibanaVisualization);
    },

    onJVMCpu: function(evt) {
      var kibanaSharedUrl = "http://188.166.155.208:5601/#/visualize/edit/Scribengin-JVM-CPU?embed&_g=(refreshInterval:(display:Off,pause:!f,section:0,value:0),time:(from:now-15m,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'*')),vis:(aggs:!((id:'1',params:(field:processCpuLoad),schema:metric,type:avg),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:timestamp,interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(field:host,order:desc,orderBy:'1',row:!t,size:15),schema:split,type:terms),(id:'4',params:(field:systemCpuLoad),schema:metric,type:avg)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,drawLinesBetweenPoints:!t,interpolate:linear,radiusRatio:9,scale:linear,setYExtents:!f,shareYAxis:!t,showCircles:!t,smoothLines:!t,times:!(),yAxis:()),type:line))";
      var uiKibanaVisualization = new UIKibanaVisualization({ label: "CPU", server: Env.kibana.server, url: kibanaSharedUrl });
      this._workspace(uiKibanaVisualization);
    },

    onJVMMemory: function(evt) {
      var kibanaSharedUrl = "http://188.166.155.208:5601/#/visualize/edit/Scribengin-JVM-Memory?embed&_g=(refreshInterval:(display:Off,pause:!f,section:0,value:0),time:(from:now-15m,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'*')),vis:(aggs:!((id:'1',params:(field:used),schema:metric,type:max),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:timestamp,interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(field:host,order:desc,orderBy:'1',row:!t,size:15),schema:split,type:terms),(id:'4',params:(field:max),schema:metric,type:max),(id:'5',params:(field:committed),schema:metric,type:max)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,drawLinesBetweenPoints:!t,interpolate:linear,radiusRatio:9,scale:linear,setYExtents:!f,shareYAxis:!t,showCircles:!t,smoothLines:!f,spyPerPage:10,times:!(),yAxis:()),type:line))";
      var uiKibanaVisualization = new UIKibanaVisualization({ label: "JVM Memory", server: Env.kibana.server, url: kibanaSharedUrl });
      this._workspace(uiKibanaVisualization);
    },

    onJVMGC: function(evt) {
      var kibanaSharedUrl = "http://188.166.155.208:5601/#/visualize/edit/Scribengin-JVM-GC?embed&_g=(refreshInterval:(display:Off,pause:!f,section:0,value:0),time:(from:now-15m,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'*')),vis:(aggs:!((id:'1',params:(field:diffCollectionCount),schema:metric,type:avg),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:timestamp,interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(field:host,order:desc,orderBy:'1',row:!t,size:15),schema:split,type:terms)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,drawLinesBetweenPoints:!t,interpolate:linear,radiusRatio:9,scale:linear,setYExtents:!f,shareYAxis:!t,showCircles:!t,smoothLines:!f,times:!(),yAxis:()),type:line))";
      var uiKibanaVisualization = new UIKibanaVisualization({ label: "JVM GC", server: Env.kibana.server, url: kibanaSharedUrl });
      this._workspace(uiKibanaVisualization);
    },

    onJVMJHiccup: function(evt) {
      var kibanaSharedUrl = "http://188.166.155.208:5601/#/visualize/edit/Scribengin-JVM-JHiccup?embed&_g=(refreshInterval:(display:Off,pause:!f,section:0,value:0),time:(from:now-15m,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'*')),vis:(aggs:!((id:'1',params:(field:maxValue),schema:metric,type:max),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:timestamp,interval:auto,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(field:host,order:desc,orderAgg:(id:'3-orderAgg',params:(field:maxValue),schema:orderAgg,type:sum),orderBy:custom,row:!t,size:15),schema:split,type:terms)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,drawLinesBetweenPoints:!t,interpolate:linear,radiusRatio:9,scale:linear,setYExtents:!f,shareYAxis:!t,showCircles:!t,smoothLines:!f,times:!(),yAxis:()),type:line))";
      var uiKibanaVisualization = new UIKibanaVisualization({ label: "JVM JHiccup", server: Env.kibana.server, url: kibanaSharedUrl });
      this._workspace(uiKibanaVisualization);
    },

    onRecordThroughput: function(evt) {
      var kibanaSharedUrl = "http://188.166.155.208:5601/#/visualize/edit/Scribengin-Metric-Record-Throughput?embed&_g=(refreshInterval:(display:Off,pause:!f,section:0,value:0),time:(from:now-15m,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'name:*record')),vis:(aggs:!((id:'1',params:(field:m1Rate),schema:metric,type:sum),(id:'2',params:(customInterval:'30s',extended_bounds:(),field:timestamp,interval:custom,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(field:name,order:desc,orderBy:'1',size:20),schema:group,type:terms)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,drawLinesBetweenPoints:!t,interpolate:linear,radiusRatio:9,scale:linear,setYExtents:!f,shareYAxis:!t,showCircles:!t,smoothLines:!t,times:!(),yAxis:()),type:line))";
      var uiKibanaVisualization = new UIKibanaVisualization({ label: "Record Throughput", server: Env.kibana.server, url: kibanaSharedUrl });
      this._workspace(uiKibanaVisualization);
    },

    onByteThroughput: function(evt) {
      var kibanaSharedUrl = "http://188.166.155.208:5601/#/visualize/edit/Scribengin-Metric-Byte-Throughput?embed&_g=(refreshInterval:(display:Off,pause:!f,section:0,value:0),time:(from:now-15m,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'name:*byte')),vis:(aggs:!((id:'1',params:(field:m1Rate),schema:metric,type:sum),(id:'2',params:(customInterval:'30s',extended_bounds:(),field:timestamp,interval:custom,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(field:name,order:desc,orderBy:'1',size:20),schema:group,type:terms)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,drawLinesBetweenPoints:!t,interpolate:linear,radiusRatio:9,scale:linear,setYExtents:!f,shareYAxis:!t,showCircles:!t,smoothLines:!t,times:!(),yAxis:()),type:line))";
      var uiKibanaVisualization = new UIKibanaVisualization({ label: "Byte Throughput", server: Env.kibana.server, url: kibanaSharedUrl });
      this._workspace(uiKibanaVisualization);
    },

    _workspace: function(uicomp) {
      $('#UIWorkspace').empty();
      $('#UIWorkspace').unbind();
      var uiContainer = new UIBreadcumbs({el: null}) ;
      uiContainer.setElement($('#UIWorkspace')).render();
      uiContainer.add(uicomp) ;
    }
  });
  
  return new UIBody() ;
});
