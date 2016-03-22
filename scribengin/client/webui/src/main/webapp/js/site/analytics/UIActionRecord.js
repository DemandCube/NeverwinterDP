define([
  'jquery', 
  'underscore', 
  'backbone',
  'ui/UIBean',
  'service/OdysseyRest'
], function($, _, Backbone, UIBean, OdysseyRest) {
  
  var UIActionRecord = UIBean.extend({
    label: "Action Record",
    config: {
      beans: {
        record: {
          name: 'record', label: 'Action Record',
          fields: [
            { 
              field: "eventId",   label: "Event Id", disable: true
            },
            { 
              field: "name",   label: "Name", required: true,  
              validator: { name: 'empty', errorMsg: "custom error message" } 
            },
            { 
              field: "assignee",   label: "Assignee", required: true,  
              validator: { name: 'empty', errorMsg: "custom error message" } 
            },
            { 
              field: "createdDate",   label: "Created Date", required: true,  
              validator: { name: 'empty', errorMsg: "custom error message" } 
            },
            { 
              field: "dueDate",   label: "Due Date", required: true,  
              validator: { name: 'empty', errorMsg: "custom error message" } 
            }
          ],
          
          edit: {
            actions: [
              {
                action:'save', label: "Save", icon: "bars",
                onClick: function(thisUI, beanConfig, beanState) { 
                  var bean = beanState.bean;
                  OdysseyRest.saveActionEvent(bean)
                  console.printJSON(bean) ;
                }
              }
            ]
          }
        }
      }
    },


    onInit: function(options) {
      this.bind('record', {}, true) ;
    },

    onEditRecord: function(record) {
      this.setBean('record', record) ;
    }
  });
  
  return UIActionRecord ;
});
