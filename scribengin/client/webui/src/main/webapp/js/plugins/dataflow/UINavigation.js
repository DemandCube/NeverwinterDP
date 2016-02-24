define([
  'jquery',
  'underscore', 
  'service/Rest',
  'ui/UIBreadcumbs',
  'site/UIWorkspace',
  'plugins/dataflow/UIListDataflow',
  'text!plugins/dataflow/UINavigation.jtpl'
], function($, _, Rest, UIBreadcumbs, UIWorkspace, UIListDataflow, Template) {
  var UINavigation = Backbone.View.extend({

    initialize: function () {
      _.bindAll(this, 'render') ;
    },
    
    _template: _.template(Template),
    
    render: function() {
      var params = { 
      } ;
      $(this.el).html(this._template(params));
      $(this.el).trigger("create") ;
    },

    events: {
      'click .onSelectUIActiveDataflow':  'onSelectUIActiveDataflow',
      'click .onSelectUIHistoryDataflow': 'onSelectUIHistoryDataflow'
    },
    
    onSelectUIActiveDataflow: function(evt) {
      var dataflows = Rest.dataflow.getActiveDataflows();
      var uiListDataflow = new UIListDataflow();
      uiListDataflow.label = "List Active Dataflow";
      uiListDataflow.setDataflows(dataflows);
      this._workspace(uiListDataflow);
    },

    onSelectUIHistoryDataflow: function(evt) {
      var dataflows = Rest.dataflow.getHistoryDataflows();
      var uiListDataflow = new UIListDataflow();
      uiListDataflow.label = "List History Dataflow";
      uiListDataflow.setDataflows(dataflows);
      this._workspace(uiListDataflow);
    },

    _workspace: function(uicomp) {
      var uiContainer = new UIBreadcumbs({el: null}) ;
      UIWorkspace.setUIComponent(uiContainer) ;
      uiContainer.add(uicomp) ;
    }
  });

  return UINavigation ;
});
