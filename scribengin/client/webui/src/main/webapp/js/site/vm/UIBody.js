define([
  'jquery',
  'underscore', 
  'backbone',
  'service/Rest',
  'ui/UIBreadcumbs',
  'site/vm/UIListVM',
  'text!site/vm/UIBody.jtpl'
], function($, _, Backbone, Rest, UIBreadcumbs, UIListVM, Template) {
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
      'click .onSelectUIActiveVM':  'onSelectUIActiveVM',
      'click .onSelectUIHistoryVM': 'onSelectUIHistoryVM'
    },
    
    onSelectUIActiveVM: function(evt) {
      var vmList = Rest.vm.getActiveVMs();
      var uiListVM = new UIListVM();
      uiListVM.label = "List Active VM";
      uiListVM.setVMs(vmList);
      this._workspace(uiListVM);
    },

    onSelectUIHistoryVM: function(evt) {
      var vmList = Rest.vm.getHistoryVMs();
      var uiListVM = new UIListVM();
      uiListVM.label = "List History VM";
      uiListVM.setVMs(vmList);
      this._workspace(uiListVM);
    },

    _workspace: function(uicomp) {
      var uiContainer = new UIBreadcumbs({el: null}) ;
      $('#UIWorkspace').empty();
      $('#UIWorkspace').unbind();
      uiContainer.setElement($('#UIWorkspace')).render();
      uiContainer.add(uicomp) ;
    }
  });
  
  return new UIBody() ;
});
