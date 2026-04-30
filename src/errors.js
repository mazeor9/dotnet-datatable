class DataTableError extends Error {
  constructor(message) {
    super(message);
    this.name = this.constructor.name;
  }
}

class ColumnNotFoundError extends DataTableError {}
class TypeMismatchError extends DataTableError {}
class ConstraintViolationError extends DataTableError {}
class DuplicatePrimaryKeyError extends ConstraintViolationError {}
class ReadOnlyColumnError extends ConstraintViolationError {}
class InvalidRowStateError extends DataTableError {}

module.exports = {
  DataTableError,
  ColumnNotFoundError,
  TypeMismatchError,
  ConstraintViolationError,
  DuplicatePrimaryKeyError,
  ReadOnlyColumnError,
  InvalidRowStateError
};
