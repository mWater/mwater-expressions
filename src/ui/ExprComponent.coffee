_ = require 'lodash'
React = require 'react'
R = React.createElement
H = React.DOM
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
    # If null, use SelectExprComponent, initially closed
    if not @props.value
      return R SelectExprComponent, schema: @props.schema, table: @props.table, placeholder: "None", initiallyOpen: false, onSelect: @props.onChange

    # If {} placeholder, initially open
    if _.isEmpty(@props.value)
      return R SelectExprComponent, schema: @props.schema, table: @props.table, placeholder: "Select...", initiallyOpen: true, onSelect: @props.onChange      

    return H.code null, JSON.stringify(@props.value)
    # # If op, 
    # if @state.open
    #   # Escape regex for filter string
    #   escapeRegex = (s) -> return s.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')
    #   if @state.filter 
    #     filter = escapeRegex(@state.filter, "i")

    #   # Create tree 
    #   treeBuilder = new ScalarExprTreeBuilder(@props.schema)
    #   tree = treeBuilder.getTree(table: @props.table, types: @props.types, includeCount: @props.includeCount, filter: filter)

    #   # Create tree component with value of table and path
    #   dropdown = R ScalarExprTreeComponent, 
    #     tree: tree,
    #     onChange: @handleTreeChange
    #     height: 350

    #   # Close when clicked outside
    #   R ClickOutHandler, onClickOut: @handleClose,
    #     R DropdownComponent, dropdown: dropdown,
    #       H.input type: "text", className: "form-control input-sm", style: { maxWidth: "16em" }, ref: @inputRef, value: @state.filter, onChange: @handleFilterChange, placeholder: "Select..."

    # else
    #   R LinkComponent, onClick: @handleOpen, (@props.placeholder or "Select...")

