define([
  'jquery',
  'underscore', 
  'backbone',
  'text!site/uidemo/UIBody.jtpl'
], function($, _, Backbone, Template) {
  var UIBody = Backbone.View.extend({
    el: $("#UIBody"),
    
    initialize: function () {
      _.bindAll(this, 'render') ;
    },
    
    _template: _.template(Template),

    render: function() {
      var params = { } ;
      $(this.el).html(this._template(params));
    },

    onActivate: function(evt) {
      require(['site/uidemo/UITableDemo'], function(UIDemoComponent) { 
        $('#UIWorkspace').empty();
        $('#UIWorkspace').unbind();
        UIDemoComponent.setElement($('#UIWorkspace')).render();
      }) ;
    },

    events: {
      'click .onSelectUIComponent': 'onSelectUIComponent'
    },
    
    onSelectUIComponent: function(evt) {
      var name = $(evt.target).closest('.onSelectUIComponent').attr('name') ;
      console.log('on select: ' + name) ;

      require(['site/uidemo/' + name], function(UIDemoComponent) { 
        $('#UIWorkspace').empty();
        $('#UIWorkspace').unbind();
        UIDemoComponent.setElement($('#UIWorkspace')).render();
      }) ;
    }
  });
  
  return new UIBody() ;
});
