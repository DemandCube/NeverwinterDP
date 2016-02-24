define([
  'service/Rest',
  'ui/UITable'
], function(Rest, UITable) {
  var UIDataflowMasterReport = UITable.extend({
    label: "Dataflow Master Report",

    config: {
      bean: {
        label: "Dataflow Master Report",
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

  return UIDataflowMasterReport ;
});
