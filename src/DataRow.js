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
        this._inEdit = false;
        this._proposedValues = null;

        for (const column of table.columns) {
            if (column.isComputed) {
                continue;
            }
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
     * @param {string} [version] - 'current' | 'original' | 'proposed'
     * @returns {*} The value stored at the specified index or column name
     * @throws {Error} If the column doesn't exist
     */
    get(index, version = undefined) {
        if (typeof index === 'number') {
            const column = this._table.columns.get(index);
            return this._getByName(column.columnName, normalizeRowVersion(version, undefined), new Set());
        }
        const column = this._table.columns.get(index);
        return this._getByName(column.columnName, normalizeRowVersion(version, undefined), new Set());
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

        if (column.isComputed) {
            throw new ReadOnlyColumnError(`Column '${columnName}' is read-only`);
        }

        if (column.readOnly && this._rowState !== DataRowState.DETACHED) {
            const current = this.get(columnName, 'current');
            const proposed = this._inEdit ? this.get(columnName, 'proposed') : current;
            if (!this._areEqual(proposed, value)) {
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

        const currentValue = this.get(columnName, this._inEdit ? 'proposed' : 'current');
        if (this._areEqual(currentValue, coercedValue)) {
            return;
        }

        if (this._inEdit) {
            if (!this._proposedValues) {
                this._proposedValues = cloneValues(this._values);
            }
            this._proposedValues[columnName] = coercedValue;
            return;
        }

        if (this._table && typeof this._table._emit === 'function') {
            this._table._emit('columnChanging', {
                row: this,
                columnName,
                oldValue: this._values[columnName],
                newValue: coercedValue
            });
        }

        const dataSet = this._table?._dataSet;
        if (dataSet && typeof dataSet._onRowValueChange === 'function') {
            dataSet._onRowValueChange(this, columnName, this._values[columnName], coercedValue);
        }

        if (this._table?.rows && typeof this._table.rows._onRowValueChange === 'function') {
            this._table.rows._onRowValueChange(this, columnName, this._values[columnName], coercedValue);
        }

        if (this._rowState === DataRowState.UNCHANGED) {
            this._originalValues = cloneValues(this._values);
            this._rowState = DataRowState.MODIFIED;
        }

        this._values[columnName] = coercedValue;

        if (this._table && typeof this._table._emit === 'function') {
            this._table._emit('columnChanged', {
                row: this,
                columnName,
                value: coercedValue
            });
        }
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
            const restored = cloneValues(this._originalValues);
            if (this._table?.rows && typeof this._table.rows._reindexRow === 'function') {
                this._table.rows._reindexRow(this, restored);
            }
            this._values = restored;
            this._rowState = DataRowState.UNCHANGED;
            this._inEdit = false;
            this._proposedValues = null;
            return;
        }

        if (this._rowState === DataRowState.DELETED) {
            this._values = cloneValues(this._originalValues);
            this._rowState = DataRowState.UNCHANGED;
            if (this._table?.rows && typeof this._table.rows._indexRow === 'function') {
                this._table.rows._indexRow(this);
            }
            this._inEdit = false;
            this._proposedValues = null;
            return;
        }

        if (this._rowState === DataRowState.ADDED) {
            this._detachFromTable();
            this._rowState = DataRowState.DETACHED;
            this._inEdit = false;
            this._proposedValues = null;
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
        if (this._table && typeof this._table._emit === 'function') {
            this._table._emit('rowDeleting', { row: this });
        }
        if (this._rowState === DataRowState.UNCHANGED) {
            this._originalValues = cloneValues(this._values);
        }
        const dataSet = this._table?._dataSet;
        if (dataSet && typeof dataSet._onRowDeleting === 'function') {
            dataSet._onRowDeleting(this);
        }
        if (this._table?.rows && typeof this._table.rows._unindexRow === 'function') {
            this._table.rows._unindexRow(this);
        }
        this._inEdit = false;
        this._proposedValues = null;
        this._rowState = DataRowState.DELETED;
        if (this._table && typeof this._table._emit === 'function') {
            this._table._emit('rowDeleted', { row: this });
        }
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

    get proposedValues() {
        return this._proposedValues;
    }

    beginEdit() {
        if (this._rowState === DataRowState.DELETED) {
            throw new InvalidRowStateError("Cannot edit a DELETED row");
        }
        if (!this._inEdit) {
            this._proposedValues = cloneValues(this._values);
            this._inEdit = true;
        }
        return this;
    }

    endEdit() {
        if (!this._inEdit) {
            return this;
        }
        const proposed = this._proposedValues ? cloneValues(this._proposedValues) : cloneValues(this._values);

        const wasInEdit = this._inEdit;
        this._inEdit = false;
        try {
            if (this._table?.rows && typeof this._table.rows._reindexRow === 'function') {
                this._table.rows._reindexRow(this, proposed);
            }
        } finally {
            this._inEdit = wasInEdit;
        }

        const before = this._values;
        this._values = proposed;
        this._proposedValues = null;
        this._inEdit = false;

        if (this._rowState === DataRowState.UNCHANGED) {
            const changed = Object.keys(this._values).some((key) => !this._areEqual(before[key], this._values[key]));
            if (changed) {
                this._originalValues = cloneValues(before);
                this._rowState = DataRowState.MODIFIED;
            }
        }

        return this;
    }

    cancelEdit() {
        this._inEdit = false;
        this._proposedValues = null;
        return this;
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
        if (column.isComputed) {
            return;
        }
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

    _getByName(columnName, version, evaluationStack) {
        const column = this._table.columns.get(columnName);
        const rowVersion = normalizeRowVersion(version, undefined);

        if (!column.isComputed) {
            if (rowVersion === 'original') {
                return this._originalValues[column.columnName];
            }
            if (rowVersion === 'current') {
                return this._values[column.columnName];
            }
            if (rowVersion === 'proposed') {
                if (this._inEdit && this._proposedValues) {
                    return this._proposedValues[column.columnName];
                }
                return this._values[column.columnName];
            }
            if (this._inEdit && this._proposedValues && Object.prototype.hasOwnProperty.call(this._proposedValues, column.columnName)) {
                return this._proposedValues[column.columnName];
            }
            return this._values[column.columnName];
        }

        if (evaluationStack.has(columnName)) {
            throw new Error(`Circular expression detected for column '${columnName}'`);
        }
        evaluationStack.add(columnName);

        try {
            const proxy = this._createExpressionProxy(evaluationStack, rowVersion);
            return column._expressionEvaluator(proxy, this, this._table, rowVersion);
        } finally {
            evaluationStack.delete(columnName);
        }
    }

    _createExpressionProxy(evaluationStack, rowVersion) {
        return new Proxy(this, {
            get: (target, prop) => {
                if (prop in target) {
                    return target[prop];
                }
                if (typeof prop === 'string' && target._table && target._table.columnExists(prop)) {
                    return target._getByName(prop, rowVersion, evaluationStack);
                }
                return undefined;
            }
        });
    }
}

function normalizeRowVersion(value, fallback = undefined) {
    if (value === null || value === undefined || value === '') {
        return fallback;
    }
    const text = String(value).toLowerCase();
    if (text === 'current') return 'current';
    if (text === 'original') return 'original';
    if (text === 'proposed') return 'proposed';
    return fallback;
}

module.exports = DataRow;
