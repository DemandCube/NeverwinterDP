define([
  'Env',
  'service/Server'
], function(Env, Server) {
  var OdysseyRest = {
    saveActionEvent: function(obj) {
      Server.restGETJsonpPush(Env.gripper.server + "/rest/odyssey/action.collector", obj); 
    },

    listActionEvent: function(source) {
      return Server.restGET(Env.gripper.server + "/rest/odyssey/action.list", { source: source }); 
    },

    saveMouseMoveEvent: function(obj) {
      var result = Server.restPOST(Env.gripper.server + "/rest/odyssey/mouse-move.collector", obj, true); 
      console.printJSON(result);
    },

    listMouseMoveEvent: function(source) {
      return Server.restGET(Env.gripper.server + "/rest/odyssey/mouse-move.list", { source: source }); 
    },
  }
  return OdysseyRest ;
});
