define([
  'jquery', 
  'underscore', 
  'backbone',
  'ui/UITabbedPane',
  'ui/UIContent'
], function($, _, Backbone, UITabbedPane, UIContent) {
  var UITabbedPaneDemo = UITabbedPane.extend({
    label: 'Tabbed Pane Demo',

    config: {
      tabs: [
        { 
          label: "Tab 1",  name: "tab1",
          onSelect: function(thisUI, tabConfig) {
            var uiTab1 = new UIContent( { content: "Tab 1" }) ;
            thisUI.setSelectedTab(tabConfig.name, uiTab1) ;
          }
        },
        { 
          label: "Tab 2",  name: "tab2",
          onSelect: function(thisUI, tabConfig) {
            var uiTab2 = new UIContent( { content: "Tab 2" }) ;
            thisUI.setSelectedTab(tabConfig.name, uiTab2) ;
          }
        }
      ]
    },
    
    onInit: function(options) {
    }
  });
  return new UITabbedPaneDemo({}) ;
});
