define([
  'plugins/vm/UINavigation'
], function(UINavigation) {
  var Plugin = {
    name: "vm",
    label: "VM",
    uiNavigation: [ new UINavigation()]
  }

  return Plugin ;
});
