define([
  'service/Server'
], function(Server) {
  console.log("Init Rest....................") ;

  function VMRest() {
    this.getActiveVMs = function() {
      return Server.cmdRestGET("/rest/vm/list-active", {});
    },

    this.getHistoryVMs = function() {
      return Server.cmdRestGET("/rest/vm/list-history", {});
    }
  };

  function DataflowRest() {
    this.getActiveDataflows = function() {
      return Server.cmdRestGET("/rest/dataflow/active", {});
    },

    this.getHistoryDataflows = function() {
      return Server.cmdRestGET("/rest/dataflow/history", {});
    },

    this.stop = function(dataflowId) {
      var params = { dataflowId: dataflowId } ;
      return Server.cmdRestGET("/rest/dataflow/stop", params);
    },

    this.resume = function(dataflowId) {
      var params = { dataflowId: dataflowId } ;
      return Server.cmdRestGET("/rest/dataflow/resume", params);
    },

    this.getDataflowOperatorReports = function(dataflowId, groupBy) {
      var params = { dataflowId: dataflowId, groupBy: groupBy } ;
      return Server.cmdRestGET("/rest/dataflow/operator.report", params);
    },

    this.getDataflowMasterReports = function(dataflowId) {
      var params = { dataflowId: dataflowId } ;
      return Server.cmdRestGET("/rest/dataflow/master.report", params);
    },

    this.getDataflowWorkerReports = function(dataflowId, groupBy) {
      var params = { dataflowId: dataflowId, "groupBy": groupBy } ;
      return Server.cmdRestGET("/rest/dataflow/worker.report", params);
    },


    this.killMaster = function(dataflowId, vmId) {
      var params = { dataflowId: dataflowId, vmId: vmId } ;
      return Server.cmdRestGET("/rest/dataflow/master.kill", params);
    },

    this.killWorker = function(dataflowId, vmId) {
      var params = { dataflowId: dataflowId, vmId: vmId } ;
      return Server.cmdRestGET("/rest/dataflow/worker.kill", params);
    }
  };

  var Rest = {
    vm:       new VMRest(),
    dataflow: new DataflowRest()
  }
  return Rest ;
});
