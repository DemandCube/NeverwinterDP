define([
  'jquery',
  'underscore', 
  'backbone',
  'ui/UIBreadcumbs',
  'site/scribengin/UIListDataflow',
  'text!site/scribengin/UIBody.jtpl'
], function($, _, Backbone, UIBreadcumbs, UIListDataflow, Template) {
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
      'click .onSelectUIHistoryDataflow': 'onSelectUIHistoryDataflow'
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
