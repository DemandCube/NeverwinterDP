define([
  'jquery', 
  'underscore', 
  'backbone',
  'ui/UICollapsible',
  'ui/UIBean'
], function($, _, Backbone, UICollapsible, UIBean) {
  var UIVMDescriptor = UIBean.extend({
    label: "VM Descriptor",
    config: {
      beans: {
        vmDescriptor: {
          name: 'bean', label: 'Bean',
          fields: [
            { 
              field: "vmId",  label: "VM Id",
              custom: {
                getDisplay: function(bean) { return bean.vmConfig.vmId; }
              }
            },
            { 
              field: "cpuCores",   label: "CPU Cores", defaultValue: 1,
              validator: { 
                name: 'integer', from: 1, to: 100, errorMsg: "Expect an integer from 1 to 100" 
              }
            },
            { 
              field: "memory",   label: "Memory", defaultValue: 128,
              validator: { 
                name: 'integer', from: 1, to: 100000, errorMsg: "Expect an integer from 1 to 10000" 
              }
            },
          ]
        }
      }
    }
  });

  var UIVMConfig = UIBean.extend({
    label: "VM Config",
    config: {
      beans: {
        vmConfig: {
          name: 'vmConfig', label: 'VM Config',
          fields: [
            { 
              field: "vmId",  label: "VM Id"
            },
            { 
              field: "roles",  label: "Roles", multiple: true, defaultValue: [ ]
            },
          ]
        }
      }
    }
  });

  var UIVMInfo = UICollapsible.extend({
    label: "VM Info", 
    config: {
      actions: [
        { 
          action: "back", label: "Back",
          onClick: function(thisUI) {
            var uiBreadcumbs = thisUI.getAncestorOfType('UIBreadcumbs') ;
            uiBreadcumbs.back() ;
          }
        }
      ]
    },

    setVMDescriptor: function(vmDescriptor) {
     this.label = "VM Config - " + vmDescriptor.vmConfig.vmId;
     var uiVMDescriptor = new UIVMDescriptor();
     uiVMDescriptor.bind('vmDescriptor', vmDescriptor, true) ;
     this.add(uiVMDescriptor) ;

     var uiVMConfig = new UIVMConfig();
     uiVMConfig.bind('vmConfig', vmDescriptor.vmConfig, true) ;
     this.add(uiVMConfig) ;
    }
  }) ;
  
  return UIVMInfo ;
});
