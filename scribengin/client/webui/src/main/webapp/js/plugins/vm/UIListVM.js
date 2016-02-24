define([
  'jquery', 
  'underscore', 
  'backbone',
  'ui/UIBean',
  'ui/UITable',
  'plugins/vm/UIVMInfo'
], function($, _, Backbone, UIBean, UITable, UIVMInfo) {
  var UIListVM = UITable.extend({
    label: "List VM",

    config: {
      toolbar: {
        dflt: {
          actions: [
            {
              action: "onReload", icon: "refresh", label: "Refresh", 
              onClick: function(thisTable) { 
                console.log("call onReload");
              } 
            }
          ]
        }
      },
      
      bean: {
        label: 'VM List',
        fields: [
          { 
            field: "dataflowId",   label: "Dataflow Id", defaultValue: '?', toggled: true, filterable: true,
            onClick: function(thisTable, row) {
              var vmDescriptor = thisTable.getItemOnCurrentPage(row) ;
              var uiVMInfo = new UIVMInfo() ;
              uiVMInfo.setVMDescriptor(vmDescriptor);
              var uiBreadcumbs = thisTable.getAncestorOfType('UIBreadcumbs') ;
              uiBreadcumbs.push(uiVMInfo) ;
            },
            custom: {
              getDisplay: function(bean) { return bean.vmConfig.vmId ; }
            }
          },
          { 
            field: "roles",   label: "Roles",  defaultValue: '', toggled: true, filterable: true,
            custom: {
              getDisplay: function(bean) { return bean.vmConfig.roles ; }
            }
          },
          { 
            field: "cpuCores",   label: "CPU Cores", type: 'number', defaultValue: '', toggled: true, filterable: true
          },
          { 
            field: "memory",   label: "Memory", type: 'number',  defaultValue: '', toggled: true, filterable: true
          }
        ],
        actions:[
          {
            icon: "delete", label: "Kill",
            onClick: function(thisTable, row) { 
              thisTable.markDeletedItemOnCurrentPage(row) ;
              console.log('Kill VM on row ' + row);
            }
          }
        ]
      }
    },
    
    setVMs: function(vmList) {
      this.setBeans(vmList) ;
    }
  });
  
  return UIListVM ;
});
