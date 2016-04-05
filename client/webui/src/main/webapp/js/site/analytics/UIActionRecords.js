define([
  'jquery', 
  'underscore', 
  'backbone',
  'ui/UITable',
  'service/OdysseyRest'
], function($, _, Backbone, UITable, OdysseyRest) {
  var UIActionRecords = UITable.extend({
    label: "Action Records",

    config: {
      toolbar: {
        dflt: {
          actions: [
            {
              action: "onReloadAll", icon: "refresh", label: "Reload All", 
              onClick: function(thisTable) { 
                var result = OdysseyRest.listActionEvent('all');
                thisTable.setBeans(result);
                thisTable.render();
              } 
            },
            {
              action: "onReloadUser", icon: "refresh", label: "Reload User", 
              onClick: function(thisTable) { 
                var result = OdysseyRest.listActionEvent('user');
                thisTable.setBeans(result);
                thisTable.render();
              } 
            }
          ]
        }
      },
      
      bean: {
        label: 'Action Record',
        fields: [
          { 
            field: "eventId",   label: "Event Id", defaultValue: '', toggled: true, filterable: false,
            onClick: function(thisTable, row) {
              var bean = thisTable.getItemOnCurrentPage(row) ;
            }
          },
          { 
            field: "name",   label: "Name", defaultValue: '', toggled: true, filterable: true
          },
          { 
            field: "assignee",   label: "Assignee", defaultValue: '', toggled: true, filterable: true
          },
          { 
            field: "createdDate",   label: "Created Date", defaultValue: '', toggled: true, filterable: true
          },
          { 
            field: "dueDate",   label: "Due Date", defaultValue: '', toggled: true, filterable: true
          }
        ],
        actions:[
          {
            icon: "edit", label: "Edit",
            onClick: function(thisTable, row) { 
              var uiAction = thisTable.getAncestorOfType('UICollapsible') ;
              var record = thisTable.getItemOnCurrentPage(row) ;
              console.printJSON(record);
              uiAction.onEditRecord(record);
            }
          }
        ]
      }
    }
  });
  
  return UIActionRecords ;
});
