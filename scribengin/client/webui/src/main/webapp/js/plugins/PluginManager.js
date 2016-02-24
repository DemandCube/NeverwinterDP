define([
  'jquery',
  'plugins/uidemo/Plugin',
  'plugins/dataflow/Plugin',
  'plugins/vm/Plugin',
], function($, DemoPlugin, DataflowPlugin, VMPlugin) {
  var PluginManager = {
    plugins: [] ,
    
    size: function() { return this.plugins.length ; },

    getDefaultPlugin: function() {
      return this.plugins[0] ;
    },

    getPlugin: function(name) {
      for(var i = 0; i < this.plugins.length; i++) {
        if(name == this.plugins[i].name) return this.plugins[i] ;
      }
      return null ;
    },

    getPlugins: function() { return this.plugins ; },

    addPlugin: function(Plugin) {
      this.plugins.push(Plugin) ;
    }
  };

  PluginManager.addPlugin(DataflowPlugin) ;
  PluginManager.addPlugin(VMPlugin) ;
  PluginManager.addPlugin(DemoPlugin) ;

  return PluginManager ;
});
