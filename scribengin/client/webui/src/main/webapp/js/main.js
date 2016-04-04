var JSLIBS = "libs" ;

require.config({
  urlArgs: "bust=" + (new Date()).getTime(), //prevent cache for development
  baseUrl: 'js',
  waitSeconds: 60,
  
  paths: {
    jquery:       JSLIBS + '/jquery/jquery',
    jqueryui:     JSLIBS + '/jquery/jquery-ui-1.11.0/jquery-ui',
    underscore:   JSLIBS + '/underscore/underscore-1.5.2',
    backbone:     JSLIBS + '/backbonejs/backbonejs-1.1.0',
    d3:           JSLIBS + '/d3/d3.min',
    nv:           JSLIBS + '/d3/nv/nv.d3.min',
    plotly:       JSLIBS + '/plotly/plotly-latest.min'
  },
  
  shim: {
    jquery: { exports: '$' },
    jqueryui: {
      deps: ["jquery"],
      exports: "jqueryui"
    },
    underscore: { exports: '_' },
    backbone: {
      deps: ["underscore", "jquery"],
      exports: "Backbone"
    },

    d3: {
      deps: [],
      exports: 'd3'
    },

    nv: {
      deps: ['d3'],
      exports: 'nv'
    },

    plotly: {
      deps: ['jquery'],
      exports: 'plotly'
    }
  }
});

require([
  'jquery', 'app'
], function($, App){
  app = App ;
  app.initialize() ;
});
