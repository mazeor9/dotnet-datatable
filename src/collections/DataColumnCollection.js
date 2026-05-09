const DataColumn = require("../DataColumn");
const { DebugTableSerializer, NodeInspectFormatter } = require("../debug");
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

  _normalizeName(name) {
    const text = String(name);
    if (this._table && this._table.caseSensitive === true) {
      return text;
    }
    return text.toLowerCase();
  }

  resolveName(name) {
    const key = this._normalizeName(name);
    const column = this._columns.get(key);
    return column ? column.columnName : null;
  }

  _rebuildNameIndex() {
    const next = new Map();
    for (const column of this._columns.values()) {
      const key = this._normalizeName(column.columnName);
      if (next.has(key)) {
        throw new Error(`Column '${column.columnName}' already exists`);
      }
      next.set(key, column);
    }
    this._columns = next;
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

    const key = this._normalizeName(column.columnName);
    if (this._columns.has(key)) {
      throw new Error(`Column '${column.columnName}' already exists`);
    }

    column._table = this._table;
    column.ordinal = this._columns.size;
    this._columns.set(key, column);

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

    if (this._table?.rows && typeof this._table.rows._rebuildIndexes === "function") {
      this._table.rows._rebuildIndexes();
    }

    if (this._table && typeof this._table._emit === 'function') {
      this._table._emit('columnAdded', { column });
    }

    return column;
  }

  /**
   * @param {string} columnName - Name of the column to remove
   * @throws {Error} If the column doesn't exist
   */
  remove(columnName) {
    const key = this._normalizeName(columnName);
    const column = this._columns.get(key);
    if (!column) {
      throw new Error(`Column '${columnName}' does not exist`);
    }

    this._columns.delete(key);

    // Removes column values ​​from all rows
    if (this._table.rows && this._table.rows._rows) {
      this._table.rows._rows.forEach((row) => {
        delete row._values[column.columnName];
      });
    }

    // Update the ordinals
    let ordinal = 0;
    for (const col of this._columns.values()) {
      col.ordinal = ordinal++;
    }

    if (this._table?.rows && typeof this._table.rows._rebuildIndexes === "function") {
      this._table.rows._rebuildIndexes();
    }

    if (this._table && typeof this._table._emit === 'function') {
      this._table._emit('columnRemoved', { column });
    }
  }

  /**
   * @param {string} columnName - Name of the column to check
   * @returns {boolean} True if the column exists, false otherwise
   */
  contains(columnName) {
    return this._columns.has(this._normalizeName(columnName));
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
    const column = this._columns.get(this._normalizeName(columnNameOrIndex));
    if (!column) {
      throw new ColumnNotFoundError(`Column '${columnNameOrIndex}' does not exist`);
    }
    return column;
  }

  toArray() {
    return Array.from(this._columns.values());
  }

  toJSON() {
    return this.toArray().map((column) => (
      typeof column.toJSON === "function"
        ? column.toJSON()
        : DebugTableSerializer.columnToDebugView(column)
    ));
  }

  toDebugView() {
    return {
      type: "DataColumnCollection",
      tableName: this._table ? this._table.tableName : undefined,
      columns: this.toArray().map((column) => DebugTableSerializer.columnToDebugView(column)),
      columnCount: this.count
    };
  }

  [NodeInspectFormatter.customInspectSymbol]() {
    return NodeInspectFormatter.inspectDataColumnCollection(this);
  }

  setPrimaryKey(columnNames) {
    const names = Array.isArray(columnNames) ? columnNames : [columnNames];
    if (names.length === 0) {
      this.clearPrimaryKey();
      return;
    }

    const resolvedNames = names.map((name) => this.get(name).columnName);

    for (const prev of this._primaryKey) {
      const col = this._columns.get(this._normalizeName(prev));
      if (col) {
        col.isPrimaryKey = false;
      }
    }

    this._primaryKey = [...resolvedNames];

    for (const name of this._primaryKey) {
      const col = this._columns.get(this._normalizeName(name));
      col.isPrimaryKey = true;
      col.allowNull = false;
    }
    if (this._primaryKey.length === 1) {
      const col = this._columns.get(this._normalizeName(this._primaryKey[0]));
      col.unique = true;
    }

    this._validatePrimaryKeyOnExistingRows();

    if (this._table?.rows && typeof this._table.rows._rebuildIndexes === "function") {
      this._table.rows._rebuildIndexes();
    }
  }

  getPrimaryKey() {
    return [...this._primaryKey];
  }

  clearPrimaryKey() {
    for (const prev of this._primaryKey) {
      const col = this._columns.get(this._normalizeName(prev));
      if (col) {
        col.isPrimaryKey = false;
      }
    }
    this._primaryKey = [];

    if (this._table?.rows && typeof this._table.rows._rebuildIndexes === "function") {
      this._table.rows._rebuildIndexes();
    }
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
