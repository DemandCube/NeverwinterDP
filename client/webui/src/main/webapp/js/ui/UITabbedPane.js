define([
  'jquery', 
  'underscore', 
  'backbone',
  'text!ui/UITabbedPane.jtpl',
  'css!ui/UITabbedPane.css'
], function($, _, Backbone, UITabbedPaneTmpl) {
  /**@type ui.UITabbedPane */
  var UITabbedPane = Backbone.View.extend({

    initialize: function(options) {
      this.onInit(options) ;
      _.bindAll(this, 'render', 'onSelectTab') ;
    },

    onInit: function(options) { },

    setSelectedTab: function(name, uicomponent) {
      var tabConfig = this._getTabConfig(name) ;
      this.state = {
        tabConfig: tabConfig,
        uicomponent: uicomponent
      }
    },
    
    _template: _.template(UITabbedPaneTmpl),
    
    render: function() {
      if(this.state == null) {
        var tabConfig = this.config.tabs[0] ;
        tabConfig.onSelect(this, tabConfig) ;
      }
      var params = { 
        config: this.config,
        state: this.state
      } ;
      $(this.el).html(this._template(params));
      $(this.el).trigger("create") ;
      
      this.$('.UITab').unbind() ;
      this.state.uicomponent.setElement(this.$('.UITab')).render();
    },
    
    events: {
      'click a.onSelectTab': 'onSelectTab',
    },
    
    onSelectTab: function(evt) {
      var tabName = $(evt.target).closest("a").attr('tab') ;
      var tabConfig = this._getTabConfig(tabName) ;
      tabConfig.onSelect(this, tabConfig) ;
      this.render() ;
    },
    
    _getTabConfig: function(name) {
      for(var i = 0; i < this.config.tabs.length; i++) {
        var tab = this.config.tabs[i] ;
        if(name == tab.name) return tab ;
      }
      return null ;
    }
  });
  
  return UITabbedPane ;
});
