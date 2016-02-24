define([
  'service/Rest',
  'ui/UITableTree'
], function(Rest, UITableTree) {
  var UIDataflowOperatorReport = UITableTree.extend({
    label: "Stream Operator",

    config: {
      bean: {
        label: 'Stream Operator',
        fields: [
          { field: "streamOperator",   label: "Stream Operator", defaultValue: '', toggled: true, filterable: true },
          { field: "status", label: "Status", defaultValue: '', toggled: true, filterable: true },
          { field: "assigned", label: "Assigned", defaultValue: '', toggled: true, filterable: true },
          { field: "AHE", label: "AHE", defaultValue: '', toggled: true, filterable: true },
          { field: "AWNM", label: "AWNM", defaultValue: '', toggled: true, filterable: true },
          { field: "LAWNM", label: "LAWNM", defaultValue: '', toggled: true, filterable: true },
          { field: "AC", label: "AC", defaultValue: '', toggled: true, filterable: true },
          { field: "CC", label: "CC", defaultValue: '', toggled: true, filterable: true },
          { field: "CFC", label: "CFC", defaultValue: '', toggled: true, filterable: true },
          { field: "lastCommitTime", label: "Last Commit", defaultValue: '', toggled: true, filterable: true },
          { field: "startTime", label: "Start Time", defaultValue: '', toggled: true, filterable: true },
          { field: "finishTime", label: "Finish Time", defaultValue: '', toggled: true, filterable: true },
          { field: "execTime", label: "Exec Time", defaultValue: '', toggled: true, filterable: true },
          { field: "duration", label: "Duration", defaultValue: '', toggled: true, filterable: true },
        ]
      }
    },

    onInit: function(options) {
      var dataflowDescriptor = options.dataflowDescriptor;
      var reports = Rest.dataflow.getDataflowOperatorReports(dataflowDescriptor.id);
      //console.printJSON(reports);
      for(key in reports) {
        if(!reports.hasOwnProperty(key)) {
          continue;
        }
        var groupReport = reports[key]
        var operator = this.addNode( { "streamOperator": key });
        operator.setCollapse(false);
        var totalAC = 0;
        for(var i = 0; i < groupReport.length; i++) {
          var status = groupReport[i].status;
          var report = groupReport[i].report;
          var rowModel = { 
            "streamOperator": report["taskId"], "status": status, "assigned": report.assignedCount, 
            "AHE": report.assignedHasErrorCount, "AWNM": report.assignedWithNoMessageProcess, "LAWNM": report.lastAssignedWithNoMessageProcess,
            "AC": report.accCommitProcessCount, "CC": report.commitCount, "CFC": report.commitFailCount,
            "lastCommitTime": "TODO", "startTime": "TODO", "finishTime": "TODO",
            "execTime": report.accRuntime, "duration": report.durationTime
          };
          var streamOperator = operator.addChild(rowModel);
          totalAC += report.accCommitProcessCount;
        }
        var all =  operator.addChild({"streamOperator": "ALL", "AC": totalAC});
      }
    }
  });

  return UIDataflowOperatorReport ;
});
