define([
], function() {
  /*
  * @author : Ram Kulkarni (http://ramkulkarni.com)
  */

  function serializeDrawing (drawingObj) {
    if (drawingObj.recordings.length == 0) return "";
    var modifiedRecordings = new Array();
    for (var i = 0; i < drawingObj.recordings.length; i++) {
      modifiedRecordings.push(serializeRecording(drawingObj.recordings[i]));
    }
    return JSON.stringify(modifiedRecordings);
  }

  function serializeRecording (recording) {
    var recordingWrp = new RecordingWrapper();
    var currActionSet = recording.actionsSet;
    while (currActionSet != null) {
      recordingWrp.actionsets.push(serializeActionSet(currActionSet));
      currActionSet = currActionSet.next;
    }
    return recordingWrp;
  }

  function serializeActionSet (actionSet) {
    var actionSetWrp = new ActionSetWrapper();
    actionSetWrp.interval = actionSet.interval;
    for (var i = 0; i < actionSet.actions.length; i++) {
      var actionWrp = serializeAction(actionSet.actions[i]);
      if (actionWrp != null) actionSetWrp.actions.push(actionWrp);
    }
    return actionSetWrp;
  }

  function serializeAction (action) {
    if (action.actionType == 1) return serializePoint (action);
    return null;
  }

  function serializePoint (point) {
    var pointWrp = new PointWrapper();
    pointWrp.type = point.type;
    pointWrp.actionType = point.actionType;
    pointWrp.x = point.x;
    pointWrp.y = point.y;
    pointWrp.isMovable = point.isMovable;
    return pointWrp;
  }

  function deserializeDrawing (serData) {
    try {
      var recordings = JSON.parse(serData);
      var result = new Array();
      if (recordings instanceof Array ) {
        for (var i = 0; i < recordings.length; i++) {
          var rec = deserializeRecording(recordings[i]);
          if (rec != null)
            result.push(rec);
        }
      }
      return result;
    } catch (e) {
      return "Error : " + e.message;
    }
    return null;
  }

  function deserializeRecording(recordingWrp) {
    var rec = new Recording();
    var prevActionSet = null;
    for (var i = 0; i < recordingWrp.actionsets.length; i++) {
      var actionSet = deserializeActionSet(recordingWrp.actionsets[i]);
      if (actionSet != null) {
        if (prevActionSet == null)
          rec.actionsSet = actionSet;
        else
          prevActionSet.next = actionSet;
        prevActionSet = actionSet;
      }
    }
    return rec;
  }

  function deserializeActionSet(actionSetWrp) {
    var actionSet = new ActionsSet();
    actionSet.actions = new Array();
    actionSet.interval = actionSetWrp.interval;
    for (var i = 0; i < actionSetWrp.actions.length; i++) {
      var action = deserializeAction(actionSetWrp.actions[i]);
      if (action != null) actionSet.actions.push(action);
    }
    return actionSet;
  }

  function deserializeAction (actionWrp) {
    if (actionWrp.actionType == 1) {
      return deserializePoint(actionWrp);
    }
    return null;
  }

  function deserializePoint (pointWrp) {
    var point = new Point();
    point.type = pointWrp.type;
    point.x = pointWrp.x;
    point.y = pointWrp.y;
    point.actionType = pointWrp.actionType;
    point.isMovable = pointWrp.isMovable;
    return point;
  }

  function RecordingWrapper() {
    var self = this;
    this.actionsets = new Array();
  }

  function ActionSetWrapper() {
    var self = this;
    this.actions = new Array();
    this.interval = 0;
  }

  function ActionWapper() {
    var self = this;
    this.actionType; // 1 - Point, other action types could be added later
    this.x = 0;
    this.y = 0;
    this.isMovable = false;
  }

  function PointWrapper() {
    var self = this;
    this.type ; //0 - moveto, 1 - lineto
  }

  PointWrapper.prototype = new ActionWapper();

  var DrawingSerializer = {
    serialize:   serializeDrawing,
    deserialize: deserializeDrawing
  };

  return DrawingSerializer ;
});
