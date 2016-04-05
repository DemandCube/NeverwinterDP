define([
  'ui/UICollapsible',
  'ui/UIContainer',
  'ui/UITable',
  'ui/UIBean'
], function(UICollapsible, UIContainer, UITable, UIBean) {

  var UIDataflowDescriptor = UIBean.extend({
    label: "Dataflow Descriptor",
    config: {
      beans: {
        dataflowDescriptor: {
          name: 'bean', label: 'Bean',
          fields: [
            { field: "id",  label: "Dataflow Id" }
          ]
        }
      }
    },

    onInit: function(options) {
      this.bind('dataflowDescriptor', options.dataflowDescriptor, true) ;
    }
  });

  var UIMasterConfig = UIBean.extend({
    label: "Master Config",
    config: {
      beans: {
        masterConfig: {
          name: 'bean', label: 'Bean',
          fields: [
            { field: "numOfInstances",  label: "Num Of Instances" },
            { field: "cpuCores",  label: "Request CPU Cores" },
            { field: "memory",  label: "Request Memory" },
            { field: "log4jConfigUrl",  label: "Log4j Config" }
          ]
        }
      }
    },

    onInit: function(options) {
      this.bind('masterConfig', options.masterConfig, true) ;
    }
  });

  var UIWorkerConfig = UIBean.extend({
    label: "Worker Config",
    config: {
      beans: {
        workerConfig: {
          name: 'bean', label: 'Bean',
          fields: [
            { field: "numOfInstances",  label: "Num Of Instances" },
            { field: "cpuCores",  label: "Request CPU Cores" },
            { field: "memory",  label: "Request Memory" },
            { field: "numOfExecutor",  label: "Num Of Executors" },
            { field: "log4jConfigUrl",  label: "Log4j Config" }
          ]
        }
      }
    },

    onInit: function(options) {
      this.bind('workerConfig', options.workerConfig, true) ;
    }
  });

  var UIStreamConfig = UIContainer.extend({
    label: "Stream Config", 
    config: {
      actions: [ ]
    },

    onInit: function(options) {
      this.setHideHeader(true);
      this.setHideFooter(true);

      var UICommonStreamConfig = UIBean.extend({
        label: "Default Stream Config",
        config: {
          beans: {
            streamConfig: {
              name: 'bean', label: 'Bean',
              fields: [
                { field: "parallelism",  label: "Default Parallelism" },
                { field: "replication",  label: "Default Replication" },
              ]
            }
          }
        }
      });
      var uiCommonStreamConfig = new UICommonStreamConfig();
      uiCommonStreamConfig.bind('streamConfig', options.streamConfig, true) ;
      this.add(uiCommonStreamConfig) ;

      var UIListStream = UITable.extend({
        label: "Streams",

        config: {
          toolbar: { dflt: { actions: [ ] } },
          
          bean: {
            label: 'Streams',
            fields: [
              { 
                field: "name",   label: "Name", defaultValue: '', toggled: true, filterable: true,
                onClick: function(thisTable, row) {
                  var bean = thisTable.getItemOnCurrentPage(row) ;
                }
              }
            ],
            actions:[ ]
          }
        },
        
        onInit: function(options) {
          var streamMap = options.streams;
          var availStreams = [] ;
          for (var key in streamMap) {
            if(!streamMap.hasOwnProperty(key)) {
              continue;
            }
            var stream = options.streams[key];
            availStreams.push(stream);
          }
          this.setBeans(availStreams);
        }
      });
      this.add(new UIListStream({ streams: options.streamConfig.streams })) ;
    }
  });
  
  var UIOperatorConfig = UITable.extend({
    label: "Operators",

    config: {
      toolbar: { dflt: { actions: [ ] } },
      
      bean: {
        label: 'Operatot',
        fields: [
          { 
            field: "name",   label: "Name", defaultValue: '', toggled: true, filterable: true,
            onClick: function(thisTable, row) {
              var bean = thisTable.getItemOnCurrentPage(row) ;
            }
          }
        ],
        actions:[ ]
      }
    },
    
    onInit: function(options) {
      var operatorMap = options.operators;
      var availOperators = [] ;
      for (var key in operatorMap) {
        if(!operatorMap.hasOwnProperty(key)) {
          continue;
        }
        var operator = operatorMap[key];
        availOperators.push(operator);
      }
      this.setBeans(availOperators);
    }
  });

  var UIDataflowConfig = UICollapsible.extend({
    label: "Dataflow Info", 
    config: {
      actions: [
        { 
          action: "reportByOperator", label: "Report",
          onClick: function(thisUI) {
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
      var dataflowDescriptor = options.dataflowDescriptor;
      this.label = "Dataflow Config - " + dataflowDescriptor.id;
      this.add(new UIDataflowDescriptor({ dataflowDescriptor: dataflowDescriptor })) ;
      this.add(new UIMasterConfig({ masterConfig: dataflowDescriptor.master })) ;
      this.add(new UIWorkerConfig({ workerConfig: dataflowDescriptor.worker})) ;
      this.add(new UIStreamConfig({ streamConfig: dataflowDescriptor.streamConfig })) ;
      this.add(new UIOperatorConfig({ operators: dataflowDescriptor.operators })) ;
    }
  }) ;
  
  return UIDataflowConfig ;
});
