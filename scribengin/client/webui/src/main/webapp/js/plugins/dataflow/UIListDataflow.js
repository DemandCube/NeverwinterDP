define([
  'ui/UIBean',
  'ui/UITable',
  'plugins/dataflow/UIDataflowConfig',
  'plugins/dataflow/UIDataflowReport',
], function(UIBean, UITable, UIDataflowConfig, UIDataflowReport) {
  var UIListDataflow = UITable.extend({
    label: "List Dataflow",

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
        label: 'List Dataflow',
        fields: [
          { 
            field: "id",   label: "Dataflow Id", defaultValue: '', toggled: true, filterable: true,
            onClick: function(thisTable, row) {
              var dataflowDescriptor = thisTable.getItemOnCurrentPage(row) ;
              var uiBreadcumbs = thisTable.getAncestorOfType('UIBreadcumbs') ;
              uiBreadcumbs.push(new UIDataflowReport({ dataflowDescriptor: dataflowDescriptor })) ;
            }
          },
          { 
            field: "numOfMaster",   label: "Masters", defaultValue: '', toggled: true, filterable: true,
            custom: {
              getDisplay: function(bean) { return bean.master.numOfInstances ; },
            }
          },
          { 
            field: "numOfWorkers",   label: "Workers", defaultValue: '', toggled: true, filterable: true,
            custom: {
              getDisplay: function(bean) { return bean.worker.numOfInstances ; },
            }
          }
        ],
        actions:[
          {
            icon: "stop", label: "Stop",
            onClick: function(thisTable, row) { 
              thisTable.markDeletedItemOnCurrentPage(row) ;
              console.log('Kill VM on row ' + row);
            }
          },
          {
            icon: "start", label: "Start",
            onClick: function(thisTable, row) { 
              thisTable.markDeletedItemOnCurrentPage(row) ;
              console.log('Kill VM on row ' + row);
            }
          },
          {
            icon: "config", label: "Config",
            onClick: function(thisTable, row) { 
              var dataflowDescriptor = thisTable.getItemOnCurrentPage(row) ;
              var uiBreadcumbs = thisTable.getAncestorOfType('UIBreadcumbs') ;
              uiBreadcumbs.push(new UIDataflowConfig({ dataflowDescriptor: dataflowDescriptor})) ;
            }
          }
        ]
      }
    },
    
    setDataflows: function(dataflowList) {
      this.setBeans(dataflowList) ;
    }
  });
  
  return UIListDataflow ;
});
