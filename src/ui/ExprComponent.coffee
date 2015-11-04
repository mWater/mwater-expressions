_ = require 'lodash'
React = require 'react'
R = React.createElement
H = React.DOM

ExprUtils = require '../ExprUtils'
SelectExprComponent = require './SelectExprComponent'

# Displays an expression of any type with controls to allow it to be altered
module.exports = class ExprComponent extends React.Component
  @propTypes:
    schema: React.PropTypes.object.isRequired
    table: React.PropTypes.string.isRequired # Current table
    value: React.PropTypes.object   # Current value
    onChange: React.PropTypes.func  # Called with new expression

    # Optionally tell the expr component what the parent op is so that it doesn't offer to double-wrap it
    # For example, if this is inside an "and" op, we don't want to offer to wrap this expression in "and"
    parentOp: React.PropTypes.string

  render: ->
    exprUtils = new ExprUtils(@props.schema)

    # If null, use SelectExprComponent, initially closed
    if not @props.value
      return R SelectExprComponent, schema: @props.schema, table: @props.table, placeholder: "None", initiallyOpen: false, onSelect: @props.onChange

    # If {} placeholder, initially open
    if _.isEmpty(@props.value)
      return R SelectExprComponent, schema: @props.schema, table: @props.table, placeholder: "Select...", initiallyOpen: true, onSelect: @props.onChange      

    # Get type of expression
    type = exprUtils.getExprType(@props.value)

    content = H.code null, JSON.stringify(@props.value)

    # If boolean, add +And link
    # if type == "boolean"
    content = R WrappedLinkComponent, links: [{ label: "+ And"}, { label: "+ Or"}], content

    return content

class WrappedLinkComponent extends React.Component
  @propTypes:
    links: React.PropTypes.array.isRequired # Shape is label, onClick

  renderLinks: ->
    H.div style: { 
      position: "absolute"
      left: 10
      bottom: 0 
    }, className: "hover-display-child",
      _.map @props.links, (link) =>
        H.a style: { 
          paddingLeft: 3
          paddingRight: 3
          backgroundColor: "white"
          cursor: "pointer"
        }, onClick: link.onClick,
          link.label

  render: ->
    H.div style: { display: "inline-block", paddingBottom: 20, position: "relative" }, className: "hover-display-parent",
      H.div style: { 
        position: "absolute"
        height: 10
        bottom: 10
        left: 0
        right: 0
        borderLeft: "solid 1px #DDD" 
        borderBottom: "solid 1px #DDD" 
        borderRight: "solid 1px #DDD" 
      }, className: "hover-display-child"
      @renderLinks(),
        @props.children