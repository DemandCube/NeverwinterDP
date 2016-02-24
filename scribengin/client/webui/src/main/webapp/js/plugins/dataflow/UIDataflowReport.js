define([
  'ui/UICollapsible',
  'plugins/dataflow/UIDataflowOperatorReport',
  'plugins/dataflow/UIDataflowMasterReport',
  'plugins/dataflow/UIDataflowWorkerReport'
], function(UICollapsible, UIDataflowOperatorReport, UIDataflowMasterReport, UIDataflowWorkerReport) {

  var UIDataflowReport = UICollapsible.extend({
    label: "Dataflow Report", 
    config: {
      actions: [
        { 
          action: "refresh", label: "Reload", 
          onClick: function(thisUI) { 
            thisUI.reload(); 
          } 
        },
        { action: "back", label: "Back", onClick: function(thisUI) { thisUI.back(); } }
      ]
    },

    onInit: function(options) {
      this.dataflowDescriptor = options.dataflowDescriptor;
      this.label = "Dataflow Report - " + options.dataflowDescriptor.id;
      this.add(new UIDataflowOperatorReport({ dataflowDescriptor: this.dataflowDescriptor })) ;
      this.add(new UIDataflowMasterReport({ dataflowDescriptor: this.dataflowDescriptor })) ;
      this.add(new UIDataflowWorkerReport({ dataflowDescriptor: this.dataflowDescriptor })) ;
    },

    back: function() {
      var uiBreadcumbs = this.getAncestorOfType('UIBreadcumbs') ;
      uiBreadcumbs.back() ;
    },

    reload: function() {
      this.clear();
      this.add(new UIDataflowOperatorReport({ dataflowDescriptor: this.dataflowDescriptor })) ;
      this.add(new UIDataflowMasterReport({ dataflowDescriptor: this.dataflowDescriptor })) ;
      this.add(new UIDataflowWorkerReport({ dataflowDescriptor: this.dataflowDescriptor })) ;
      this.render();
    }
  }) ;
  
  return UIDataflowReport ;
});
