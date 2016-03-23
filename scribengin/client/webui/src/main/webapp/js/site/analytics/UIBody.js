define([
  'jquery',
  'underscore', 
  'backbone',
  'Env',
  'ui/UIBreadcumbs',
  'ui/UIKibanaVisualization',
  'site/analytics/UIMousePlayback',
  'site/analytics/UIAction',
  'site/analytics/UIUserInfo',
  'site/analytics/UIWebEvent',
  'text!site/analytics/UIBody.jtpl'
], function($, _, Backbone, Env, UIBreadcumbs, UIKibanaVisualization,  UIMousePlayback, UIAction, UIUserInfo, UIWebEvent, Template) {
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
      'click .onSelectDiagram':  'onSelectDiagram',

      'click .onSelectWebEventStatReport':  'onSelectWebEventStatReport',

      'click .onSelectUserInfoInput':  'onSelectUserInfoInput',
      'click .onSelectWebEventInput':  'onSelectWebEventInput',
      'click .onMouseReplay':  'onMouseReplay',
      'click .onAction': 'onAction'
    },

    onSelectDiagram: function(evt) {
      this.render();
    },

    onSelectWebEventStatReport: function(evt) {
      var kibanaSharedUrl = "http://188.166.155.208:5601/#/visualize/edit/Analytics-Webpage-Stat?embed&_g=(refreshInterval:(display:Off,pause:!f,section:0,value:0),time:(from:now-4h,mode:quick,to:now))&_a=(filters:!(),linked:!f,query:(query_string:(analyze_wildcard:!t,query:'*')),vis:(aggs:!((id:'1',params:(field:hitCount),schema:metric,type:sum),(id:'2',params:(customInterval:'2h',extended_bounds:(),field:timestamp,interval:m,min_doc_count:1),schema:segment,type:date_histogram),(id:'3',params:(field:host,order:desc,orderBy:'1',size:50),schema:group,type:terms)),listeners:(),params:(addLegend:!t,addTimeMarker:!f,addTooltip:!t,defaultYExtents:!f,drawLinesBetweenPoints:!t,interpolate:linear,radiusRatio:9,scale:linear,setYExtents:!f,shareYAxis:!t,showCircles:!t,smoothLines:!f,times:!(),yAxis:()),type:line))";
      var uiKibanaVisualization = new UIKibanaVisualization({ label: "Web Event Report", server: Env.kibana.server, url: kibanaSharedUrl });
      this._workspace(uiKibanaVisualization);
    },

    onSelectUserInfoInput: function(evt) {
      var uiUserInfo = new UIUserInfo();
      this._workspace(uiUserInfo);
    },

    onSelectWebEventInput: function(evt) {
      var uiWebEvent = new UIWebEvent();
      this._workspace(uiWebEvent);
    },
    
    onMouseReplay: function(evt) {
      var uiMousePlayback = new UIMousePlayback();
      this._workspace(uiMousePlayback);
    },

    onAction: function(evt) {
      var uiAction = new UIAction();
      this._workspace(uiAction);
    },

    _workspace: function(uicomp) {
      $('#UIWorkspace').empty();
      $('#UIWorkspace').unbind();
      var uiContainer = new UIBreadcumbs({el: null}) ;
      uiContainer.setElement($('#UIWorkspace')).render();
      uiContainer.add(uicomp) ;
    }
  });
  
  return new UIBody();
});
