const DataRowState = require('./enums/DataRowState');
const { DebugTableSerializer, NodeInspectFormatter } = require('./debug');
const {
    ColumnNotFoundError,
    TypeMismatchError,
    ConstraintViolationError,
    DuplicatePrimaryKeyError,
    ReadOnlyColumnError,
    InvalidRowStateError
} = require('./errors');
const { cloneValues } = require('./utils/typeUtils');

class DataRow {
    constructor(table, initialState = DataRowState.DETACHED) {
        this._table = table;
        this._values = {};
        this._rowState = initialState;
        this._originalValues = {};

        for (const column of table.columns) {
            this._values[column.columnName] = this._evaluateDefaultValue(column);
        }
        this._originalValues = cloneValues(this._values);
    }

    /**
     * Alias for get()
     * @param {string} columnName - Name of the column to retrieve value from
     * @returns {*} The value stored in the specified column
     * @throws {Error} If the column doesn't exist
     */
    item(columnName) {
        return this.get(columnName);
    }

    /**
     * @param {number|string} index - Numeric index or column name
     * @returns {*} The value stored at the specified index or column name
     * @throws {Error} If the column doesn't exist
     */
    get(index) {
        if (typeof index === 'number') {
            const column = this._table.columns.get(index);
            return this._values[column.columnName];
        }
        const column = this._table.columns.get(index);
        return this._values[column.columnName];
    }

    /**
     * @param {string} columnName - Name of the column to set value for
     * @param {*} value - Value to set in the specified column
     * @throws {Error} If the column doesn't exist
     * @throws {Error} If null is not allowed for the column
     * @throws {Error} If value type doesn't match column data type
     */
    set(columnName, value) {
        if (this._rowState === DataRowState.DELETED) {
            throw new InvalidRowStateError("Cannot set values on a DELETED row");
        }

        let column;
        try {
            column = this._table.columns.get(columnName);
        } catch (e) {
            throw new ColumnNotFoundError(`Column '${columnName}' does not exist`);
        }

        if (column.readOnly && this._rowState !== DataRowState.DETACHED) {
            if (!this._areEqual(this._values[columnName], value)) {
                throw new ReadOnlyColumnError(`Column '${columnName}' is read-only`);
            }
            return;
        }

        const coercedValue = this._coerceValue(column, value);

        if ((coercedValue === null || coercedValue === undefined) && column.allowNull === false) {
            throw new ConstraintViolationError(`Column '${columnName}' does not allow null values`);
        }

        if (
            column.maxLength !== null &&
            column.maxLength !== undefined &&
            coercedValue !== null &&
            coercedValue !== undefined &&
            String(coercedValue).length > column.maxLength
        ) {
            throw new ConstraintViolationError(`Column '${columnName}' exceeds maxLength ${column.maxLength}`);
        }

        this._validateUniqueness(columnName, coercedValue);
        this._validatePrimaryKeyIfNeeded(columnName, coercedValue);

        const currentValue = this._values[columnName];
        if (this._areEqual(currentValue, coercedValue)) {
            return;
        }

        if (this._rowState === DataRowState.UNCHANGED) {
            this._originalValues = cloneValues(this._values);
            this._rowState = DataRowState.MODIFIED;
        }

        this._values[columnName] = coercedValue;
    }

    toJSON() {
        return this._values;
    }

    toString() {
        return JSON.stringify(this._values);
    }

    // ===== ROWSTATE MANAGEMENT METHODS =====

    /**
     * Accepts all changes made to the row
     */
    acceptChanges() {
        if (this._rowState === DataRowState.DETACHED) {
            throw new InvalidRowStateError("Cannot accept changes on a DETACHED row");
        }

        if (this._rowState === DataRowState.DELETED) {
            this._detachFromTable();
            this._rowState = DataRowState.DETACHED;
            return;
        }

        this._originalValues = cloneValues(this._values);
        this._rowState = DataRowState.UNCHANGED;
    }

    /**
     * Rejects all changes and restores original values
     */
    rejectChanges() {
        if (this._rowState === DataRowState.MODIFIED) {
            this._values = cloneValues(this._originalValues);
            this._rowState = DataRowState.UNCHANGED;
            return;
        }

        if (this._rowState === DataRowState.DELETED) {
            this._values = cloneValues(this._originalValues);
            this._rowState = DataRowState.UNCHANGED;
            return;
        }

        if (this._rowState === DataRowState.ADDED) {
            this._detachFromTable();
            this._rowState = DataRowState.DETACHED;
            return;
        }
    }

    /**
     * Checks if the row has unsaved changes
     * @returns {boolean}
     */
    hasChanges() {
        return DataRowState.isChanged(this._rowState);
    }

    /**
     * Gets the current row state
     * @returns {string}
     */
    getRowState() {
        return this._rowState;
    }

    /**
     * Marks the row as deleted
     */
    delete() {
        if (this._rowState === DataRowState.DETACHED) {
            throw new InvalidRowStateError("Cannot delete a DETACHED row");
        }
        if (this._rowState === DataRowState.DELETED) {
            return;
        }
        if (this._rowState === DataRowState.UNCHANGED) {
            this._originalValues = cloneValues(this._values);
        }
        this._rowState = DataRowState.DELETED;
    }

    toObject() {
        return { ...this._values };
    }

    toDebugView() {
        return DebugTableSerializer.rowToDebugView(this);
    }

    [NodeInspectFormatter.customInspectSymbol]() {
        return NodeInspectFormatter.inspectDataRow(this);
    }

    get rowState() {
        return this._rowState;
    }

    get currentValues() {
        return this._values;
    }

    get originalValues() {
        return this._originalValues;
    }

    _setRowState(state) {
        this._rowState = state;
    }

    _attachToTable(table) {
        this._table = table;
        if (this._rowState === DataRowState.DETACHED) {
            this._rowState = DataRowState.ADDED;
        }
    }

    _detachFromTable() {
        if (!this._table || !this._table.rows || typeof this._table.rows.remove !== "function") {
            return;
        }
        this._table.rows.remove(this);
    }

    _initializeNewColumn(column) {
        const value = this._evaluateDefaultValue(column);
        this._values[column.columnName] = value;
        if (this._rowState === DataRowState.UNCHANGED) {
            this._originalValues[column.columnName] = value;
        }
    }

    _evaluateDefaultValue(column) {
        if (typeof column.defaultValue === "function") {
            return column.defaultValue();
        }
        return column.defaultValue;
    }

    _coerceValue(column, value) {
        if (value === null || value === undefined || !column.dataType) {
            return value;
        }

        switch (String(column.dataType).toLowerCase()) {
            case "number": {
                const num = Number(value);
                if (Number.isNaN(num)) {
                    throw new TypeMismatchError(
                        `Type mismatch for column '${column.columnName}': expected number`
                    );
                }
                return num;
            }
            case "integer": {
                const num = Number(value);
                if (Number.isNaN(num) || !Number.isInteger(num)) {
                    throw new TypeMismatchError(
                        `Type mismatch for column '${column.columnName}': expected integer`
                    );
                }
                return num;
            }
            case "bigint":
                try {
                    return typeof value === "bigint" ? value : BigInt(value);
                } catch (_) {
                    throw new TypeMismatchError(
                        `Type mismatch for column '${column.columnName}': expected bigint`
                    );
                }
            case "date": {
                if (value instanceof Date) {
                    if (Number.isNaN(value.getTime())) {
                        throw new TypeMismatchError(
                            `Type mismatch for column '${column.columnName}': invalid date`
                        );
                    }
                    return value;
                }
                const date = new Date(value);
                if (Number.isNaN(date.getTime())) {
                    throw new TypeMismatchError(
                        `Type mismatch for column '${column.columnName}': expected date`
                    );
                }
                return date;
            }
            case "string":
                return String(value);
            case "boolean": {
                if (typeof value === "boolean") return value;
                if (typeof value === "number") return value !== 0;
                if (typeof value === "string") {
                    const lower = value.trim().toLowerCase();
                    if (["true", "1", "yes", "y"].includes(lower)) return true;
                    if (["false", "0", "no", "n"].includes(lower)) return false;
                }
                throw new TypeMismatchError(
                    `Type mismatch for column '${column.columnName}': expected boolean`
                );
            }
            case "json":
                if (typeof value === "string") {
                    try {
                        return JSON.parse(value);
                    } catch (_) {
                        throw new TypeMismatchError(
                            `Type mismatch for column '${column.columnName}': expected json`
                        );
                    }
                }
                return value;
            case "object":
                if (value && typeof value === "object" && !Array.isArray(value)) return value;
                throw new TypeMismatchError(
                    `Type mismatch for column '${column.columnName}': expected object`
                );
            case "array":
                if (Array.isArray(value)) return value;
                throw new TypeMismatchError(
                    `Type mismatch for column '${column.columnName}': expected array`
                );
            case "buffer":
                if (typeof Buffer !== "undefined" && Buffer.isBuffer(value)) return value;
                if (typeof Buffer !== "undefined") return Buffer.from(value);
                return value;
            case "any":
            case "null":
            case "undefined":
                return value;
            default:
                return value;
        }
    }

    _areEqual(a, b) {
        if (a instanceof Date && b instanceof Date) {
            return a.getTime() === b.getTime();
        }
        return a === b;
    }

    _validateUniqueness(columnName, value) {
        const column = this._table.columns.get(columnName);
        if (!column.unique) {
            return;
        }
        const pk = this._table.columns.getPrimaryKey();
        if (pk.length > 1 && pk.includes(columnName)) {
            return;
        }
        if (value === null || value === undefined) {
            return;
        }
        for (const row of this._table.rows?._rows ?? []) {
            if (row === this) continue;
            if (typeof row.getRowState === "function" && row.getRowState() === DataRowState.DELETED) continue;
            if (this._areEqual(row.get(columnName), value)) {
                if (column.isPrimaryKey) {
                    throw new DuplicatePrimaryKeyError(`Duplicate primary key for column '${columnName}': ${value}`);
                }
                throw new ConstraintViolationError(`Constraint violation: duplicate value for unique column '${columnName}'`);
            }
        }
    }

    _validatePrimaryKeyIfNeeded(columnName, newValue) {
        const pk = this._table.columns.getPrimaryKey();
        if (pk.length === 0 || !pk.includes(columnName)) {
            return;
        }
        const keyValues = pk.map((name) => (name === columnName ? newValue : this._values[name]));
        if (keyValues.some((v) => v === null || v === undefined)) {
            return;
        }
        for (const row of this._table.rows?._rows ?? []) {
            if (row === this) continue;
            if (typeof row.getRowState === "function" && row.getRowState() === DataRowState.DELETED) continue;
            const otherKeyValues = pk.map((name) => row.get(name));
            if (otherKeyValues.some((v) => v === null || v === undefined)) continue;
            const same = keyValues.every((v, i) => this._areEqual(v, otherKeyValues[i]));
            if (same) {
                throw new DuplicatePrimaryKeyError(
                    `Duplicate primary key (${pk.join(",")}): ${keyValues.join(",")}`
                );
            }
        }
    }
}

module.exports = DataRow;
