const DataColumn = require("../DataColumn");
const {
  ColumnNotFoundError,
  DuplicatePrimaryKeyError,
  ConstraintViolationError
} = require("../errors");

class DataColumnCollection {
  constructor(table) {
    this._table = table;
    this._columns = new Map();
    this._primaryKey = [];
  }

  /**
   * @param {DataColumn|string} columnOrName - DataColumn instance or name of the column to add
   * @param {string|null} [dataType=null] - Data type of the column (only used if columnOrName is a string)
   * @returns {DataColumn} The added column
   * @throws {Error} If a column with the same name already exists
   */
  add(columnOrName, dataType = null, options = undefined) {
    const column =
      columnOrName instanceof DataColumn
        ? columnOrName
        : new DataColumn(columnOrName, dataType, options);

    if (this._columns.has(column.columnName)) {
      throw new Error(`Column '${column.columnName}' already exists`);
    }

    column._table = this._table;
    column.ordinal = this._columns.size;
    this._columns.set(column.columnName, column);

    // Adds the column to all existing rows
    if (this._table.rows && this._table.rows._rows) {
      this._table.rows._rows.forEach((row) => {
        if (typeof row._initializeNewColumn === "function") {
          row._initializeNewColumn(column);
        } else {
          row._values[column.columnName] = column.defaultValue;
        }
      });
    }

    if (column.isPrimaryKey) {
      const current = this.getPrimaryKey();
      if (current.length === 0) {
        this.setPrimaryKey([column.columnName]);
      } else if (!current.includes(column.columnName)) {
        this.setPrimaryKey([...current, column.columnName]);
      }
    }

    return column;
  }

  /**
   * @param {string} columnName - Name of the column to remove
   * @throws {Error} If the column doesn't exist
   */
  remove(columnName) {
    if (!this._columns.has(columnName)) {
      throw new Error(`Column '${columnName}' does not exist`);
    }

    this._columns.delete(columnName);

    // Removes column values ​​from all rows
    if (this._table.rows && this._table.rows._rows) {
      this._table.rows._rows.forEach((row) => {
        delete row._values[columnName];
      });
    }

    // Update the ordinals
    let ordinal = 0;
    for (const col of this._columns.values()) {
      col.ordinal = ordinal++;
    }
  }

  /**
   * @param {string} columnName - Name of the column to check
   * @returns {boolean} True if the column exists, false otherwise
   */
  contains(columnName) {
    return this._columns.has(columnName);
  }

  has(columnName) {
    return this.contains(columnName);
  }

  get(columnNameOrIndex) {
    if (typeof columnNameOrIndex === "number") {
      const column = Array.from(this._columns.values())[columnNameOrIndex];
      if (!column) {
        throw new ColumnNotFoundError(`Column at index ${columnNameOrIndex} does not exist`);
      }
      return column;
    }
    const column = this._columns.get(columnNameOrIndex);
    if (!column) {
      throw new ColumnNotFoundError(`Column '${columnNameOrIndex}' does not exist`);
    }
    return column;
  }

  toArray() {
    return Array.from(this._columns.values());
  }

  setPrimaryKey(columnNames) {
    const names = Array.isArray(columnNames) ? columnNames : [columnNames];
    if (names.length === 0) {
      this.clearPrimaryKey();
      return;
    }

    for (const name of names) {
      if (!this._columns.has(name)) {
        throw new ColumnNotFoundError(`Column '${name}' does not exist`);
      }
    }

    for (const prev of this._primaryKey) {
      const col = this._columns.get(prev);
      if (col) {
        col.isPrimaryKey = false;
      }
    }

    this._primaryKey = [...names];

    for (const name of this._primaryKey) {
      const col = this._columns.get(name);
      col.isPrimaryKey = true;
      col.allowNull = false;
    }
    if (this._primaryKey.length === 1) {
      const col = this._columns.get(this._primaryKey[0]);
      col.unique = true;
    }

    this._validatePrimaryKeyOnExistingRows();
  }

  getPrimaryKey() {
    return [...this._primaryKey];
  }

  clearPrimaryKey() {
    for (const prev of this._primaryKey) {
      const col = this._columns.get(prev);
      if (col) {
        col.isPrimaryKey = false;
      }
    }
    this._primaryKey = [];
  }

  _validatePrimaryKeyOnExistingRows() {
    if (this._primaryKey.length === 0) {
      return;
    }
    const seen = new Set();
    for (const row of this._table.rows?._rows ?? []) {
      if (typeof row.getRowState === "function" && row.getRowState() === "DELETED") {
        continue;
      }
      const keyValues = this._primaryKey.map((name) => row.get(name));
      if (keyValues.some((v) => v === null || v === undefined)) {
        throw new ConstraintViolationError(
          `Primary key '${this._primaryKey.join(",")}' cannot contain null values`
        );
      }
      const key = JSON.stringify(keyValues);
      if (seen.has(key)) {
        throw new DuplicatePrimaryKeyError(
          `Duplicate primary key (${this._primaryKey.join(",")}): ${keyValues.join(",")}`
        );
      }
      seen.add(key);
    }
  }

  get count() {
    return this._columns.size;
  }

  *[Symbol.iterator]() {
    yield* this._columns.values();
  }
}

module.exports = DataColumnCollection;
