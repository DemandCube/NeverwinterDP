define([
  'jquery', 
  'underscore', 
  'backbone',
  "d3",
  'css!site/uidemo/UID3Demo.css'
], function($, _, Backbone, d3) {

  var treeData = [
    {
      "name": "Top Level",
      "parent": "null",
      "children": [
        {
          "name": "Level 2: A", "parent": "Top Level",
          "children": [
            { "name": "Son of A", "parent": "Level 2: A" },
            { "name": "1st Daughter of A", "parent": "Level 2: A" },
            { "name": "2nd Daughter of A", "parent": "Level 2: A" },
            { "name": "3rd Daughter of A", "parent": "Level 2: A" },
          ]
        },
        {
          "name": "Level 2: B", "parent": "Top Level",
          "children": [
            { "name": "Son of A", "parent": "Level 2: B" },
            { "name": "1st Daughter of A", "parent": "Level 2: B" }
          ]
        }
      ]
    }
  ];

  // ************** Generate the tree diagram	 *****************
  var margin = {top: 20, right: 120, bottom: 20, left: 120};
  var width = 960 - margin.right - margin.left;
  var height = 500 - margin.top - margin.bottom;
    
  var i = 0;

  var tree = d3.layout.tree().size([height, width]);

  var diagonal = d3.svg.diagonal().projection(function(d) { return [d.y, d.x]; });

  var svg = d3.select("#UIBody").append("svg")
    .attr("width", width + margin.right + margin.left)
    .attr("height", height + margin.top + margin.bottom).append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");


  function update(source) {
    // Compute the new tree layout.
    var nodes = tree.nodes(root).reverse();
    var links = tree.links(nodes);

    // Normalize for fixed-depth.
    nodes.forEach(function(d) { d.y = d.depth * 180; });

    // Declare the nodes…
    var node = svg.selectAll("g.node").data(nodes, function(d) { return d.id || (d.id = ++i); });

    // Enter the nodes.
    var nodeEnter = node.enter().append("g") .attr("class", "node") .attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; });

    nodeEnter.append("circle").attr("r", 10).style("fill", "#fff");

    nodeEnter.append("text")
      .attr("x", function(d) { 
        return d.children || d._children ? -13 : 13; })
      .attr("dy", ".35em")
      .attr("text-anchor", function(d) { 
        return d.children || d._children ? "end" : "start"; })
      .text(function(d) { return d.name; })
      .style("fill-opacity", 1);

    // Declare the links…
    var link = svg.selectAll("path.link").data(links, function(d) { return d.target.id; });

    // Enter the links.
    link.enter().insert("path", "g").attr("class", "link").attr("d", diagonal);
    console.log('Finish update()');
  }

  var root = treeData[0];
    

  var UID3Demo = Backbone.View.extend({
    label: 'D3 Demo',

    initialize: function (config) {
    },

    _template: _.template(
      "<div id='diagram' style='border: 1px solid'>This is a test</div>"
    ),

    render: function() {
      var params = { };
      $(this.el).html(this._template(params));

      update(root);
    }
  });
  
  return new UID3Demo({}) ;
});
