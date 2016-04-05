define([
  'jquery', 
  'underscore', 
  'backbone',
  'ui/UICollapsible',
  'site/analytics/UIMouseDraw',
  'site/analytics/UIMouseDrawRecords',
  'css!site/analytics/UIMousePlayback.css'
], function($, _, Backbone, UICollabsible, UIMouseDraw, UIMouseDrawRecords) {
  
  var UIMousePlayback = UICollabsible.extend({
    label: "Mouse Playback", 
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
      this.uiMouseDraw = new UIMouseDraw();
      this.add(this.uiMouseDraw) ;

      this.uiMouseDrawRecords = new UIMouseDrawRecords();
      this.uiMouseDrawRecords.setBeans([]);
      this.add(this.uiMouseDrawRecords);
    },
    
    playback: function(mouseMoveData) {
      this.uiMouseDraw.playback(mouseMoveData);
    }
  }) ;
  
  return UIMousePlayback ;
});
