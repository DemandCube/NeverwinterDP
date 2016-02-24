define([
  'jquery',
  'underscore', 
  'backbone',
  'service/Rest',
  'ui/UIBreadcumbs',
  'site/UIWorkspace',
  'plugins/vm/UIListVM',
  'text!plugins/vm/UINavigation.jtpl'
], function($, _, Backbone, Rest, UIBreadcumbs, UIWorkspace, UIListVM, Template) {
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
      UIWorkspace.setUIComponent(uiContainer) ;
      uiContainer.add(uicomp) ;
    }
  });

  return UINavigation ;
});
