define([
  'ui/UITabbedPane',
  'site/scribengin/UIDataflowOperatorReport',
  'site/scribengin/UIDataflowMasterReport',
  'site/scribengin/UIDataflowWorkerReport'
], function(UITabbedPane, UIDataflowOperatorReport, UIDataflowMasterReport, UIDataflowWorkerReport) {
  var UIDataflowReport = UITabbedPane.extend({
    label: "Dataflow Report", 

    config: {
      tabs: [
        { 
          name: "operator", label: "Operator",
          onSelect: function(thisUI, tabConfig) {
            thisUI.setSelectedTab(tabConfig.name, new UIDataflowOperatorReport({ dataflowDescriptor: thisUI.dataflowDescriptor, groupBy: "operator" })) ;
          }
        },
        { 
          name: "executor", label: "Executors",
          onSelect: function(thisUI, tabConfig) {
            thisUI.setSelectedTab(tabConfig.name, new UIDataflowOperatorReport({ dataflowDescriptor: thisUI.dataflowDescriptor, groupBy: "executor" })) ;
          }
        },
        { 
          name: "master", label: "Master",
          onSelect: function(thisUI, tabConfig) {
            thisUI.setSelectedTab(tabConfig.name, new UIDataflowMasterReport({ dataflowDescriptor: thisUI.dataflowDescriptor })) ;
          }
        },
        { 
          name: "worker", label: "Worker",
          onSelect: function(thisUI, tabConfig) {
            thisUI.setSelectedTab(tabConfig.name, new UIDataflowWorkerReport({ dataflowDescriptor: thisUI.dataflowDescriptor })) ;
          }
        }
      ]
    },
    
    onInit: function(options) {
      this.dataflowDescriptor = options.dataflowDescriptor;
      this.label = "Dataflow Report - " + options.dataflowDescriptor.id;
    }
  });
  
  return UIDataflowReport ;
});
