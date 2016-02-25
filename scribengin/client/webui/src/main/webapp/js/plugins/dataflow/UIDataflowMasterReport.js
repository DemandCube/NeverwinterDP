define([
  'service/Rest',
  'ui/UICollapsible',
  'ui/UITable'
], function(Rest, UICollapsible, UITable) {
  var UIListDataflowMasterReport = UITable.extend({
    label: "Active Master Report",

    config: {
      bean: {
        label: "Master Report",
        fields: [
          { field: "vmId",   label: "VM Id", defaultValue: '', toggled: true, filterable: true },
          { field: "leader",   label: "Leader", defaultValue: '', toggled: true, filterable: true },
        ],

        actions:[
          {
            icon: "stop", label: "Kill",
            onClick: function(thisTable, row) { 
              var report = thisTable.getItemOnCurrentPage(row) ;
              Rest.dataflow.killMaster(thisTable.dataflowDescriptor.id, report.vmId);
              thisTable.markDeletedItemOnCurrentPage(row) ;
            }
          }
        ]
      }
    },

    onInit: function(options) {
      this.dataflowDescriptor = options.dataflowDescriptor;
      var reports = Rest.dataflow.getDataflowMasterReports(this.dataflowDescriptor.id);
      //console.printJSON(reports);
      this.setBeans(reports);
    }
  });

  var UIDataflowMasterReport = UICollapsible.extend({
    label: "Master Report", 
    config: {
      actions: [
        { 
          action: "refresh", label: "Reload", 
          onClick: function(thisUI) { 
            thisUI.clear();
            thisUI.add(new UIListDataflowMasterReport({ dataflowDescriptor: thisUI.dataflowDescriptor })) ;
            thisUI.render();
          } 
        }
      ]
    },

    onInit: function(options) {
      this.dataflowDescriptor = options.dataflowDescriptor;
      this.add(new UIListDataflowMasterReport({ dataflowDescriptor: this.dataflowDescriptor })) ;
    }
  }) ;

  return UIDataflowMasterReport ;
});
