define([
  'jquery', 
  'underscore', 
  'backbone',
  'ui/UICollapsible',
  'site/analytics/UIActionRecord',
  'site/analytics/UIActionRecords',
], function($, _, Backbone, UICollabsible, UIActionRecord, UIActionRecords) {
  
  var UIAction = UICollabsible.extend({
    label: "Action", 
    config: {
      actions: [
        { 
          action: "back", label: "Back",
          onClick: function(thisUI) {
            console.log("on click back") ;
          }
        }
      ]
    },

    onInit: function(options) {
      this.uiActionRecord = new UIActionRecord();
      this.add(this.uiActionRecord) ;

      this.uiActionRecords = new UIActionRecords();
      this.uiActionRecords.setBeans([]);
      this.add(this.uiActionRecords);
    },

    onEditRecord: function(record) {
      this.uiActionRecord.onEditRecord(record);
    }
  }) ;
  
  return UIAction ;
});
