React = require 'react'
R = React.createElement
H = React.DOM
ClickOutHandler = require('react-onclickout')

ScalarExprTreeComponent = require './ScalarExprTreeComponent'
ScalarExprTreeBuilder = require './ScalarExprTreeBuilder'
DropdownComponent = require './DropdownComponent'
LinkComponent = require './LinkComponent'

# Selects an expression using a searchable dropdown control that activates when clicked.
module.exports = class SelectExprComponent extends React.Component
  @propTypes:
    schema: React.PropTypes.object.isRequired
    table: React.PropTypes.string.isRequired # Table to restrict selections to (can still follow joins to other tables)
    types: React.PropTypes.array # Optional types to limit to
    includeCount: React.PropTypes.bool # Optionally include count at root level of a table
    placeholder: React.PropTypes.node # Placeholder. "Select..." if not specified
    onSelect: React.PropTypes.func  # Called with new expression

  constructor: ->
    super
    @state = { 
      active: false  # True if active
      filter: ""
    }
  
  handleActivate: => @setState(active: true)
  handleDeactivate: => @setState(active: false)
  handleFilterChange: (ev) => @setState(filter: ev.target.value)
  
  handleTreeChange: (val) => 
    # Called with { table, joins, expr }
    # Make into expression
    if val.joins.length == 0 
      # Simple field expression
      @props.onSelect(val.expr)
    else
      @props.onSelect({ type: "scalar", joins: val.joins, expr: val.expr })

  inputRef: (comp) =>
    if comp
      comp.focus()

  render: ->
    if @state.active
      escapeRegex = (s) -> return s.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&')
      if @state.filter 
        filter = escapeRegex(@state.filter, "i")

      # Create tree 
      treeBuilder = new ScalarExprTreeBuilder(@props.schema)
      tree = treeBuilder.getTree(table: @props.table, types: @props.types, includeCount: @props.includeCount, filter: filter)

      # Create tree component with value of table and path
      dropdown = R ScalarExprTreeComponent, 
        tree: tree,
        onChange: @handleTreeChange
        height: 350

      R ClickOutHandler, onClickOut: @handleDeactivate,
        R DropdownComponent, dropdown: dropdown,
          H.input type: "text", className: "form-control input-sm", style: { maxWidth: "16em" }, ref: @inputRef, value: @state.filter, onChange: @handleFilterChange, placeholder: "Select..."

    else
      R LinkComponent, onClick: @handleActivate, (@props.placeholder or "Select...")

