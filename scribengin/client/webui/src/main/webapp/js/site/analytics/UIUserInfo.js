define([
  'jquery', 
  'underscore', 
  'backbone',
  'ui/UIBean',
], function($, _, Backbone, UIBean) {
  
  var UIUserInfo = UIBean.extend({
    label: "User Info",
    config: {
      beans: {
        record: {
          name: 'record', label: 'User Info',
          fields: [
            { 
              field: "userId",   label: "User Id", disable: true
            },
            { 
              field: "sessionId",   label: "Session Id", required: true
            }
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
      var user = {
        userId: "user",
        sessionId: "session-1"
      };
      this.bind('record', {}, true) ;
    }
  });
  
  return UIUserInfo ;
});
