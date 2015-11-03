React = require 'react'
ReactDOM = require 'react-dom'
R = React.createElement
H = React.DOM
ClickOutHandler = require('react-onclickout')
Schema = require './Schema'

$ ->
  schema = new Schema()
  schema.addTable({ id: "t1", name: "T1", contents: [
    { id: "text", name: "Text", type: "text" }
    { id: "integer", name: "Integer", type: "integer" }
    { id: "decimal", name: "Decimal", type: "decimal" }
    { id: "enum", name: "Enum", type: "enum", values: [{ id: "a", name: "A"}, { id: "b", name: "B"}] }
    { id: "date", name: "Date", type: "date" }
    { id: "datetime", name: "Datetime", type: "datetime" }
    { id: "boolean", name: "Boolean", type: "boolean" }
    { id: "1-2", name: "T1->T2", type: "join", join: { fromTable: "t1", fromColumn: "primary", toTable: "t2", toColumn: "t1", op: "=", multiple: true }}
  ]})

  schema.addTable({ id: "t2", name: "T2", ordering: "integer", contents: [
    { id: "t1", name: "T1", type: "uuid" }
    { id: "text", name: "Text", type: "text" }
    { id: "integer", name: "Integer", type: "integer" }
    { id: "decimal", name: "Decimal", type: "decimal" }
    { id: "2-1", name: "T2->T1", type: "join", join: { fromTable: "t2", fromColumn: "t1", toTable: "t1", toColumn: "primary", op: "=", multiple: false }}
    ]})


  ReactDOM.render(R(SelectExprComponent, table: "t1", schema: schema), document.getElementById("main"))

ScalarExprTreeComponent = require './ui/ScalarExprTreeComponent'
ScalarExprTreeBuilder = require './ui/ScalarExprTreeBuilder'
 
class SelectExprComponent extends React.Component
  @propTypes:
    schema: React.PropTypes.object.isRequired
    # dataSource: React.PropTypes.object.isRequired
    table: React.PropTypes.string # Table to restrict selections to (can still follow joins to other tables)
    types: React.PropTypes.array # Optional types to limit to
    includeCount: React.PropTypes.bool # Optionally include count at root level of a table

  constructor: ->
    super
    @state = { active: true }
  
  handleActivate: => @setState(active: true)
  handleDeactivate: => @setState(active: false)

  handleTreeChange: (val) => console.log(val)

  inputRef: (comp) =>
    if comp
      comp.focus()

  render: ->
    if @state.active
      # Create tree 
      treeBuilder = new ScalarExprTreeBuilder(@props.schema)
      tree = treeBuilder.getTree(table: @props.table, types: @props.types, includeCount: @props.includeCount, initialValue: @props.value)

      # Create tree component with value of table and path
      dropdown = R ScalarExprTreeComponent, 
        tree: tree,
        onChange: @handleTreeChange
        height: 350

      R ClickOutHandler, onClickOut: @handleDeactivate,
        R DropdownComponent, dropdown: dropdown,
          H.input type: "text", className: "form-control", style: { maxWidth: "16em" }, ref: @inputRef, initialValue: "", placeholder: "Select..."

    else
      H.a onClick: @handleActivate, "Select..."


class DropdownComponent extends React.Component
  @propTypes: 
    dropdown: React.PropTypes.node

  render: ->
    H.div className: "dropdown #{if @props.dropdown then "open" else ""}",
      @props.children
      H.div className: "dropdown-menu", style: { width: "100%" },
        @props.dropdown


# class SelectExprComponent extends React.Component
#   constructor: ->
#     super
#     @state = { active: false }
  
#   handleActivate: =>
#     @setState(active: true)

#   handleDeactivate: =>
#     @setState(active: false)

#   render: ->
#     if @state.active
#       R ClickOutHandler, onClickOut: @handleDeactivate,
#         H.input type: "text", initialValue: ""
#     else
#       H.a onClick: @handleActivate, "Select..."      