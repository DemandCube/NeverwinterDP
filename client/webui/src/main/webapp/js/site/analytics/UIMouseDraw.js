define([
  'jquery',
  'underscore', 
  'backbone',
  'service/OdysseyRest',
  'site/analytics/RecordableDrawing',
  'site/analytics/drawingSerializer',
  'text!site/analytics/UIMouseDraw.jtpl'
], function($, _, Backbone, OdysseyRest,  RecordableDrawing, DrawingSerializer, Template) {

  var UIMouseDraw = Backbone.View.extend({
    label: "Mouse Draw",
    
    initialize: function () {
      _.bindAll(this, 'render') ;
    },
    
    _template: _.template(Template),

    render: function() {
      var params = { } ;
      $(this.el).html(this._template(params));
      this._stopRecordMode();
    },

    events: {
      'click .onStartRecording': 'onStartRecording',
      'click .onStopRecording': 'onStopRecording',
      'click .onClearRecording': 'onClearRecording',
      'click .onStartPlayback': 'onStartPlayback',
      'click .onStopPlayback': 'onStopPlayback',
      'click .onSave': 'onSave',
      'click .onSerializeDrawing': 'onSerializeDrawing',
    },

    onStartRecording: function() {
      this.drawing1 = new RecordableDrawing("canvas1");
      this.drawing1.startRecording();
      this._startRecordMode();
      console.log("on start recording done!!!");
    },

    onStopRecording: function() {
      this.drawing1.stopRecording();
      this._stopRecordMode();
    },


    onClearRecording: function() {
      this.drawing1.clearCanvas();
    },

    onStartPlayback: function() {
      var thisComp = this;
      this.drawing1.playRecording(function() {
        //on playback start
        thisComp.playbackInterruptCommand = "";
      }, function() {
        //on playback end
      }, function() {
        //on pause
      }, function() {
        //status callback
        return thisComp.playbackInterruptCommand;
      });
    },
    
    onStopPlayback: function() {
      this.playbackInterruptCommand = "stop";		
    },

    onSave: function() {
      var json = DrawingSerializer.serialize(this.drawing1);
      var record = {
        mouseMoveData: json
      };
      OdysseyRest.saveMouseMoveEvent(record)
      console.log('call onSave');
    },

    onSerializeDrawing: function() {
      var thisComp = this;
      var json = DrawingSerializer.serialize(this.drawing1);
      this.playback(json);
    },

    playback: function(json) {
      var thisComp = this;
      var result =  DrawingSerializer.deserialize(json);
      this.drawing2 = new RecordableDrawing("canvas2");
      this.drawing2.recordings = result;
      //set drawing property of each recording
      for (var i = 0; i < result.length; i++) {
        result[i].drawing = this.drawing2;
      }

      this.drawing2.playRecording(function() {
        //on playback start
        thisComp.playbackInterruptCommand = "";
      }, function() {
        //on playback end
      }, function() {
        //on pause
      }, function() {
        //status callback
        return thisComp.playbackInterruptCommand;
      });
    },

    _startRecordMode: function () {
      $("#onStopRecordingBtn").show();

      $("#onStartRecordingBtn").hide();
      $("#onStartPlaybackBtn").hide();
      $("#onStopPlaybackBtn").hide();
      $("#onSaveBtn").hide();
    },

    _stopRecordMode: function () {
      $("#onStopRecordingBtn").hide();

      $("#onStartRecordingBtn").show();
      $("#onStartPlaybackBtn").show();
      $("#onStopPlaybackBtn").show();
      $("#onSaveBtn").show();
    }
  });
  
  return UIMouseDraw ;
});
