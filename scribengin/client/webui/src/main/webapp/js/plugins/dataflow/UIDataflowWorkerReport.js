define([
  'service/Rest',
  'ui/UITable'
], function(Rest, UITable) {
  var UIDataflowWorkerReport = UITable.extend({
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
      this.dataflowDescriptor = options.dataflowDescriptor;
      var reports = Rest.dataflow.getDataflowWorkerReports(this.dataflowDescriptor.id, "active");
      //console.printJSON(reports);
      this.setBeans(reports);
    }
  });

  return UIDataflowWorkerReport ;
});
