var ColumnNotFoundException,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

module.exports = ColumnNotFoundException = (function(superClass) {
  extend(ColumnNotFoundException, superClass);

  function ColumnNotFoundException() {
    ColumnNotFoundException.__super__.constructor.apply(this, arguments);
  }

  return ColumnNotFoundException;

})(Error);
