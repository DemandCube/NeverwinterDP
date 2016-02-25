define([
  'service/Rest',
  'ui/UICollapsible',
  'ui/UITable'
], function(Rest, UICollapsible, UITable) {
  var UIListDataflowWorkerReport = UITable.extend({
    label: "Dataflow Worker Report",

    config: {
      bean: {
        label: "Dataflow Worker Report",
        fields: [
          { field: "worker",   label: "Worker VM Id", defaultValue: '', toggled: true, filterable: true },
          { field: "status",   label: "Status", defaultValue: '', toggled: true, filterable: true },
        ],

        actions:[
          {
            icon: "stop", label: "Kill",
            onClick: function(thisTable, row) { 
              var report = thisTable.getItemOnCurrentPage(row) ;
              Rest.dataflow.killWorker(thisTable.dataflowDescriptor.id, report.worker);
              thisTable.markDeletedItemOnCurrentPage(row) ;
            }
          }
        ]
      }
    },

    onInit: function(options) {
      this.label = "Dataflow Worker Report(" + options.filterBy + ")";
      this.dataflowDescriptor = options.dataflowDescriptor;
      var reports = Rest.dataflow.getDataflowWorkerReports(this.dataflowDescriptor.id, options.filterBy);
      this.setBeans(reports);
    }
  });

  var UIDataflowWorkerReport = UICollapsible.extend({
    label: "Worker Report", 
    config: {
      actions: [
        { 
          action: "refresh", label: "Reload", 
          onClick: function(thisUI) { 
            thisUI.clear();
            thisUI.add(new UIListDataflowWorkerReport({ dataflowDescriptor: thisUI.dataflowDescriptor, filterBy: "active" })) ;
            thisUI.add(new UIListDataflowWorkerReport({ dataflowDescriptor: thisUI.dataflowDescriptor, filterBy: "history" })) ;
            thisUI.render();
          } 
        },
        { 
          action: "back", label: "Back", 
          onClick: function(thisUI) { 
            var uiBreadcumbs = thisUI.getAncestorOfType('UIBreadcumbs') ;
            uiBreadcumbs.back() ;
          } 
        }
      ]
    },

    onInit: function(options) {
      this.dataflowDescriptor = options.dataflowDescriptor;
      this.add(new UIListDataflowWorkerReport({ dataflowDescriptor: this.dataflowDescriptor, filterBy: "active" })) ;
      this.add(new UIListDataflowWorkerReport({ dataflowDescriptor: this.dataflowDescriptor, filterBy: "history" })) ;
    }
  }) ;

  return UIDataflowWorkerReport ;
});
