define([
  'plugins/dataflow/UINavigation'
], function(UINavigation) {
  var Plugin = {
    name: "dataflow",
    label: "Dataflow",
    uiNavigation: [ new UINavigation()]
  }

  return Plugin ;
});
