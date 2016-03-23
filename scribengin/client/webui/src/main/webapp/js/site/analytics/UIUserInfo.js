define([
  'jquery', 
  'underscore', 
  'backbone',
  'Env',
  'ui/UIBean',
], function($, _, Backbone, Env, UIBean) {
  
  var UIUserInfo = UIBean.extend({
    label: "User Info",
    config: {
      beans: {
        record: {
          name: 'record', label: 'User Info',
          fields: [
            { field: "userId",   label: "User Id", required: "true" },
            { field: "visistorId",   label: "Visitor Id", required: "true" },
            { field: "sessionId",   label: "Session Id", required: true }
          ],
          
          edit: {
            actions: [
              {
                action:'save', label: "Save", icon: "bars",
                onClick: function(thisUI, beanConfig, beanState) { 
                  var bean = beanState.bean;
                }
              }
            ]
          }
        }
      }
    },

    onInit: function(options) {
      this.bind('record', Env.user, true) ;
    }
  });
  
  return UIUserInfo ;
});
