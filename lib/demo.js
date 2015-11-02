var ExprComponent, H, R, React, ReactDOM, Schema,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

React = require('react');

ReactDOM = require('react-dom');

R = React.createElement;

H = React.DOM;

Schema = require('./Schema');

$(function() {
  var schema;
  schema = new Schema();
  schema.addTable({
    id: "t1",
    name: "T1"
  });
  schema.addColumn("t1", {
    id: "text",
    name: "Text",
    type: "text"
  });
  schema.addColumn("t1", {
    id: "integer",
    name: "Integer",
    type: "integer"
  });
  schema.addColumn("t1", {
    id: "decimal",
    name: "Decimal",
    type: "decimal"
  });
  schema.addColumn("t1", {
    id: "enum",
    name: "Enum",
    type: "enum",
    values: [
      {
        id: "a",
        name: "A"
      }, {
        id: "b",
        name: "B"
      }
    ]
  });
  schema.addColumn("t1", {
    id: "date",
    name: "Date",
    type: "date"
  });
  schema.addColumn("t1", {
    id: "datetime",
    name: "Datetime",
    type: "datetime"
  });
  schema.addColumn("t1", {
    id: "boolean",
    name: "Boolean",
    type: "boolean"
  });
  schema.addColumn("t1", {
    id: "1-2",
    name: "T1->T2",
    type: "join",
    join: {
      fromTable: "t1",
      fromColumn: "primary",
      toTable: "t2",
      toColumn: "t1",
      op: "=",
      multiple: true
    }
  });
  schema.addTable({
    id: "t2",
    name: "T2",
    ordering: "integer"
  });
  schema.addColumn("t2", {
    id: "t1",
    name: "T1",
    type: "uuid"
  });
  schema.addColumn("t2", {
    id: "text",
    name: "Text",
    type: "text"
  });
  schema.addColumn("t2", {
    id: "integer",
    name: "Integer",
    type: "integer"
  });
  schema.addColumn("t2", {
    id: "decimal",
    name: "Decimal",
    type: "decimal"
  });
  schema.addColumn("t2", {
    id: "2-1",
    name: "T2->T1",
    type: "join",
    join: {
      fromTable: "t2",
      fromColumn: "t1",
      toTable: "t1",
      toColumn: "primary",
      op: "=",
      multiple: false
    }
  });
  return React.render(R(ExprComponent), document.getElementById("main"));
});

ExprComponent = (function(superClass) {
  extend(ExprComponent, superClass);

  function ExprComponent() {
    return ExprComponent.__super__.constructor.apply(this, arguments);
  }

  ExprComponent.prototype.render = function() {
    return H.div(null, "Hi!");
  };

  return ExprComponent;

})(React.Component);
