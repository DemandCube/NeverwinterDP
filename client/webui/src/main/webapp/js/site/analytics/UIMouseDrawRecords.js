define([
  'jquery', 
  'underscore', 
  'backbone',
  'service/OdysseyRest',
  'ui/UIBean',
  'ui/UITable'
], function($, _, Backbone, OdysseyRest, UIBean, UITable) {
  var UIMouseDrawRecords = UITable.extend({
    label: "Mouser Draw Records",

    config: {
      toolbar: {
        dflt: {
          actions: [
            {
              action: "onReloadAll", icon: "refresh", label: "Reload All", 
              onClick: function(thisTable) { 
                var result = OdysseyRest.listMouseMoveEvent('all');
                thisTable.setBeans(result);
                thisTable.render();
              } 
            },
            {
              action: "onReloadUser", icon: "refresh", label: "Reload User", 
              onClick: function(thisTable) { 
                var result = OdysseyRest.listMouseMoveEvent('user');
                thisTable.setBeans(result);
                thisTable.render();
              } 
            }
          ]
        }
      },
      
      bean: {
        label: 'Contact Bean',
        fields: [
          { field: "source",   label: "Source", defaultValue: '', toggled: true, filterable: true },
          { field: "eventId",   label: "Event Id", defaultValue: '', toggled: true, filterable: true }
        ],
        actions:[
          {
            icon: "play", label: "Play",
            onClick: function(thisTable, row) { 
              var uiMousePlayback = thisTable.getAncestorOfType('UICollapsible') ;
              var record = thisTable.getItemOnCurrentPage(row) ;
              uiMousePlayback.playback(record.mouseMoveData);
            }
          }
        ]
      }
    }
  });
  
  return UIMouseDrawRecords ;
});
