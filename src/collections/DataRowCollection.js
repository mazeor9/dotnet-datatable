const DataRow = require('../DataRow');
const DataRowState = require('../enums/DataRowState');
const { DebugTableSerializer, NodeInspectFormatter } = require('../debug');
const {
    ColumnNotFoundError,
    ConstraintViolationError,
    DuplicatePrimaryKeyError,
    InvalidRowStateError
} = require('../errors');

class DataRowCollection {
    constructor(table) {
        this._table = table;
        this._rows = [];
        this._pkIndex = new Map();
        this._uniqueIndexes = new Map();
        this._uniqueConstraintIndexes = new Map();
         // direct access to index column
         const func = (index) => this._rows[index];
        
        // Copia tutte le proprietà e metodi nell'oggetto funzione
        Object.setPrototypeOf(func, DataRowCollection.prototype);
        Object.assign(func, this);
        func._rebuildIndexes();
        
        // Crea un proxy per gestire sia l'accesso via indice che via funzione
        return new Proxy(func, {
            get: (target, prop) => {
                if (typeof prop === 'string' && !isNaN(Number(prop))) {
                    return target._rows[prop];
                }
                return target[prop];
            },
            apply: (target, thisArg, [index]) => {
                return target._rows[index];
            }
        });
    }

    /**
     * @param {DataRow|Array|Object} row - Row to add: can be a DataRow instance, array of values, or object with column-value pairs
     * @returns {DataRow} The added row
     */
    add(row) {
        const dataRow = row instanceof DataRow ? row : this._table.newRow();

        if (dataRow._table !== this._table) {
            throw new InvalidRowStateError('Cannot add a DataRow created for a different table');
        }
        if (dataRow.getRowState() !== DataRowState.DETACHED) {
            throw new InvalidRowStateError(`Cannot add a row that is not DETACHED (state: ${dataRow.getRowState()})`);
        }

        if (!(row instanceof DataRow)) {
            if (Array.isArray(row)) {
                const columns = this._table.columns.toArray();
                for (let i = 0; i < columns.length; i++) {
                    if (row[i] !== undefined) {
                        dataRow.set(columns[i].columnName, row[i]);
                    }
                }
            } else if (row && typeof row === 'object') {
                for (const [key, value] of Object.entries(row)) {
                    if (!this._table.columns.contains(key)) {
                        throw new ColumnNotFoundError(`Column '${key}' does not exist`);
                    }
                    dataRow.set(key, value);
                }
            } else {
                throw new InvalidRowStateError('Invalid row payload');
            }
        }

        if (this._table && typeof this._table._emit === 'function') {
            this._table._emit('rowAdding', { row: dataRow });
        }
        this._validateRowConstraints(dataRow);
        dataRow._attachToTable(this._table);
        this._rows.push(dataRow);
        this._indexRow(dataRow);
        if (this._table && typeof this._table._emit === 'function') {
            this._table._emit('rowAdded', { row: dataRow });
        }
        return dataRow;
    }

    /**
     * @param {DataRow} row - The row instance to remove from the collection
     */
    remove(row) {
        const index = this._rows.indexOf(row);
        if (index !== -1) {
            if (this._table && typeof this._table._emit === 'function') {
                this._table._emit('rowRemoving', { row });
            }
            this._unindexRow(row);
            this._rows.splice(index, 1);
            if (row instanceof DataRow) {
                row._setRowState(DataRowState.DETACHED);
            }
            if (this._table && typeof this._table._emit === 'function') {
                this._table._emit('rowRemoved', { row });
            }
        }
    }

    /**
     * @param {number} index - The index of the row to remove
     */
    removeAt(index) {
        if (index >= 0 && index < this._rows.length) {
            const [row] = this._rows.splice(index, 1);
            if (this._table && typeof this._table._emit === 'function') {
                this._table._emit('rowRemoving', { row });
            }
            this._unindexRow(row);
            if (row instanceof DataRow) {
                row._setRowState(DataRowState.DETACHED);
            }
            if (this._table && typeof this._table._emit === 'function') {
                this._table._emit('rowRemoved', { row });
            }
        }
    }

    clear() {
        if (this._table && typeof this._table._emit === 'function') {
            this._table._emit('tableClearing', { table: this._table });
        }
        for (const row of this._rows) {
            if (row instanceof DataRow) {
                row._setRowState(DataRowState.DETACHED);
            }
        }
        this._rows = [];
        this._rebuildIndexes();
        if (this._table && typeof this._table._emit === 'function') {
            this._table._emit('tableCleared', { table: this._table });
        }
    }

    get count() {
        return this._rows.length;
    }

    countRows() {
        return this._rows.length;
    }

    toArray() {
        return [...this._rows];
    }

    toJSON() {
        return this._rows
            .filter(row => row.getRowState() !== DataRowState.DELETED)
            .map(row => row.toObject());
    }

    toDebugView(options = {}) {
        const columns = this._table ? this._table.columns.toArray() : [];
        const rows = this._rows
            .filter(row => options.includeDeleted === true || row.getRowState() !== DataRowState.DELETED)
            .map(row => DebugTableSerializer.rowToObject(row, {
                columns,
                serializeValues: true
            }));

        return {
            type: 'DataRowCollection',
            tableName: this._table ? this._table.tableName : undefined,
            rows,
            rowCount: rows.length
        };
    }

    [NodeInspectFormatter.customInspectSymbol](depth, options, inspect) {
        return NodeInspectFormatter.inspectDataRowCollection(this, depth, options, inspect);
    }

    find(key) {
        const pk = this._table.columns.getPrimaryKey();
        if (pk.length === 0) {
            throw new InvalidRowStateError('Cannot find() without a primary key');
        }

        let keyValues;
        if (pk.length === 1) {
            keyValues = [key];
        } else if (Array.isArray(key)) {
            keyValues = key;
        } else if (key && typeof key === 'object') {
            keyValues = pk.map((name) => key[name]);
        } else {
            throw new InvalidRowStateError('Invalid key for composite primary key');
        }

        if (keyValues.length !== pk.length) {
            throw new InvalidRowStateError(`Invalid key length for primary key (${pk.join(",")})`);
        }

        if (keyValues.some((v) => v === null || v === undefined)) {
            return null;
        }

        const keyString = this._serializeKeyValues(keyValues);
        return this._pkIndex.get(keyString) || null;
    }

    *[Symbol.iterator]() {
        yield* this._rows;
    }

    _addClonedRow(row) {
        if (!(row instanceof DataRow)) {
            throw new InvalidRowStateError('Invalid row payload');
        }
        if (row._table !== this._table) {
            row._table = this._table;
        }
        this._rows.push(row);
        this._indexRow(row);
    }

    _validateRowConstraints(row) {
        const columns = this._table.columns.toArray();
        const pk = this._table.columns.getPrimaryKey();

        for (const col of columns) {
            if (col.isComputed) {
                continue;
            }
            const value = row.get(col.columnName);
            if ((value === null || value === undefined) && col.allowNull === false) {
                throw new ConstraintViolationError(`Column '${col.columnName}' does not allow null values`);
            }
        }

        if (pk.length > 0) {
            const keyValues = pk.map((name) => row.get(name));
            if (keyValues.some((v) => v === null || v === undefined)) {
                throw new ConstraintViolationError(
                    `Primary key '${pk.join(",")}' cannot contain null values`
                );
            }
            const keyString = this._serializeKeyValues(keyValues);
            if (this._pkIndex.has(keyString)) {
                throw new DuplicatePrimaryKeyError(
                    `Duplicate primary key (${pk.join(",")}): ${keyValues.join(",")}`
                );
            }
        }

        for (const col of columns) {
            if (!col.unique) continue;
            if (col.isComputed) continue;
            if (pk.length > 1 && pk.includes(col.columnName)) continue;
            const value = row.get(col.columnName);
            if (value === null || value === undefined) continue;

            if (pk.length === 1 && pk[0] === col.columnName) {
                continue;
            }

            const index = this._uniqueIndexes.get(col.columnName);
            if (!index) {
                continue;
            }
            const valueKey = this._serializeIndexValue(value);
            if (valueKey === null) {
                continue;
            }
            if (index.has(valueKey)) {
                throw new ConstraintViolationError(
                    `Constraint violation: duplicate value for unique column '${col.columnName}'`
                );
            }
        }
    }

    _rebuildIndexes() {
        this._pkIndex = new Map();
        this._uniqueIndexes = new Map();
        this._uniqueConstraintIndexes = new Map();

        const pk = this._table?.columns?.getPrimaryKey?.() ?? [];
        const columns = this._table?.columns?.toArray?.() ?? [];

        for (const col of columns) {
            if (!col || !col.unique) continue;
            if (col.isComputed) continue;
            if (pk.length > 1 && pk.includes(col.columnName)) continue;
            if (pk.length === 1 && pk[0] === col.columnName) continue;
            this._uniqueIndexes.set(col.columnName, new Map());
        }

        for (const row of this._rows) {
            this._indexRow(row);
        }
    }

    _rebuildUniqueConstraintIndexes() {
        this._uniqueConstraintIndexes = new Map();
        const constraints = typeof this._table?.getUniqueConstraints === 'function'
            ? this._table.getUniqueConstraints()
            : [];
        for (const constraint of constraints) {
            if (!constraint || !Array.isArray(constraint.columns) || constraint.columns.length === 0) {
                continue;
            }
            this._uniqueConstraintIndexes.set(constraint.name, new Map());
        }
    }

    _serializeKeyValues(values) {
        return JSON.stringify(values.map((value) => this._serializeIndexValue(value)));
    }

    _serializeIndexValue(value) {
        if (value === null) return null;
        if (value === undefined) return null;
        if (value instanceof Date) return `d:${value.getTime()}`;
        if (typeof value === 'bigint') return `bi:${value.toString()}`;
        if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) return `buf:${value.toString('base64')}`;
        if (typeof value === 'number') return `n:${Object.is(value, -0) ? '-0' : String(value)}`;
        if (typeof value === 'boolean') return `b:${value ? '1' : '0'}`;
        if (typeof value === 'string') return `s:${value}`;
        try {
            return `j:${JSON.stringify(value, (_, item) => {
                if (item instanceof Date) return { $type: 'date', value: item.toISOString() };
                if (typeof item === 'bigint') return { $type: 'bigint', value: item.toString() };
                if (typeof Buffer !== 'undefined' && Buffer.isBuffer(item)) return { $type: 'buffer', value: item.toString('base64') };
                return item;
            })}`;
        } catch (_) {
            return `u:${String(value)}`;
        }
    }

    _indexRow(row) {
        if (!(row instanceof DataRow)) {
            return;
        }
        if (row.getRowState() === DataRowState.DELETED || row.getRowState() === DataRowState.DETACHED) {
            return;
        }

        const pk = this._table.columns.getPrimaryKey();
        if (pk.length > 0) {
            const keyValues = pk.map((name) => row.get(name));
            if (keyValues.some((v) => v === null || v === undefined)) {
                throw new ConstraintViolationError(
                    `Primary key '${pk.join(",")}' cannot contain null values`
                );
            }
            const keyString = this._serializeKeyValues(keyValues);
            const existing = this._pkIndex.get(keyString);
            if (existing && existing !== row) {
                throw new DuplicatePrimaryKeyError(
                    `Duplicate primary key (${pk.join(",")}): ${keyValues.join(",")}`
                );
            }
            this._pkIndex.set(keyString, row);
        }

        for (const [columnName, index] of this._uniqueIndexes.entries()) {
            const value = row.get(columnName);
            const valueKey = this._serializeIndexValue(value);
            if (valueKey === null) {
                continue;
            }
            const existing = index.get(valueKey);
            if (existing && existing !== row) {
                throw new ConstraintViolationError(
                    `Constraint violation: duplicate value for unique column '${columnName}'`
                );
            }
            index.set(valueKey, row);
        }

        this._indexUniqueConstraints(row);
        this._enforceCheckConstraints(row, row._values);
    }

    _unindexRow(row) {
        if (!(row instanceof DataRow)) {
            return;
        }

        const pk = this._table.columns.getPrimaryKey();
        if (pk.length > 0) {
            const keyValues = pk.map((name) => row.get(name));
            if (!keyValues.some((v) => v === null || v === undefined)) {
                const keyString = this._serializeKeyValues(keyValues);
                if (this._pkIndex.get(keyString) === row) {
                    this._pkIndex.delete(keyString);
                }
            }
        }

        for (const [columnName, index] of this._uniqueIndexes.entries()) {
            const value = row.get(columnName);
            const valueKey = this._serializeIndexValue(value);
            if (valueKey === null) {
                continue;
            }
            if (index.get(valueKey) === row) {
                index.delete(valueKey);
            }
        }

        this._unindexUniqueConstraints(row);
    }

    _onRowValueChange(row, columnName, oldValue, newValue) {
        if (!(row instanceof DataRow)) {
            return;
        }
        if (row.getRowState() === DataRowState.DELETED) {
            return;
        }

        const pk = this._table.columns.getPrimaryKey();
        const isDetached = row.getRowState() === DataRowState.DETACHED;
        const pkAffected = pk.includes(columnName);
        const uniqueIndex = this._uniqueIndexes.get(columnName) || null;

        if (pkAffected && pk.length > 0) {
            const keyValues = pk.map((name) => (name === columnName ? newValue : row.get(name)));
            if (keyValues.some((v) => v === null || v === undefined)) {
                if (!isDetached) {
                    throw new ConstraintViolationError(
                        `Primary key '${pk.join(",")}' cannot contain null values`
                    );
                }
                return;
            }
            const newKey = this._serializeKeyValues(keyValues);
            const existing = this._pkIndex.get(newKey);
            if (existing && existing !== row) {
                throw new DuplicatePrimaryKeyError(
                    `Duplicate primary key (${pk.join(",")}): ${keyValues.join(",")}`
                );
            }

            if (!isDetached) {
                const oldKeyValues = pk.map((name) => row.get(name));
                const oldKey = this._serializeKeyValues(oldKeyValues);
                if (oldKey !== newKey) {
                    if (this._pkIndex.get(oldKey) === row) {
                        this._pkIndex.delete(oldKey);
                    }
                    this._pkIndex.set(newKey, row);
                }
            }
        }

        if (uniqueIndex) {
            if (newValue !== null && newValue !== undefined) {
                const newValueKey = this._serializeIndexValue(newValue);
                const existing = uniqueIndex.get(newValueKey);
                if (existing && existing !== row) {
                    throw new ConstraintViolationError(
                        `Constraint violation: duplicate value for unique column '${columnName}'`
                    );
                }
            }

            if (!isDetached) {
                const oldValueKey = this._serializeIndexValue(oldValue);
                const newValueKey = this._serializeIndexValue(newValue);

                if (oldValueKey !== null && oldValueKey !== newValueKey && uniqueIndex.get(oldValueKey) === row) {
                    uniqueIndex.delete(oldValueKey);
                }
                if (newValueKey !== null && uniqueIndex.get(newValueKey) !== row) {
                    uniqueIndex.set(newValueKey, row);
                }
            }
        }

        const nextValues = { ...row._values, [columnName]: newValue };
        this._reindexUniqueConstraints(row, nextValues);
        this._enforceCheckConstraints(row, nextValues);
    }

    _reindexRow(row, nextValues) {
        if (!(row instanceof DataRow)) {
            return;
        }
        if (row.getRowState() === DataRowState.DELETED || row.getRowState() === DataRowState.DETACHED) {
            return;
        }

        const pk = this._table.columns.getPrimaryKey();
        if (pk.length > 0) {
            const newKeyValues = pk.map((name) => nextValues[name]);
            if (newKeyValues.some((v) => v === null || v === undefined)) {
                throw new ConstraintViolationError(
                    `Primary key '${pk.join(",")}' cannot contain null values`
                );
            }
            const newKey = this._serializeKeyValues(newKeyValues);
            const existing = this._pkIndex.get(newKey);
            if (existing && existing !== row) {
                throw new DuplicatePrimaryKeyError(
                    `Duplicate primary key (${pk.join(",")}): ${newKeyValues.join(",")}`
                );
            }
        }

        for (const [columnName, index] of this._uniqueIndexes.entries()) {
            const newValueKey = this._serializeIndexValue(nextValues[columnName]);
            if (newValueKey === null) {
                continue;
            }
            const existing = index.get(newValueKey);
            if (existing && existing !== row) {
                throw new ConstraintViolationError(
                    `Constraint violation: duplicate value for unique column '${columnName}'`
                );
            }
        }

        this._validateUniqueConstraintsForValues(row, nextValues);
        this._enforceCheckConstraints(row, nextValues);

        if (pk.length > 0) {
            const oldKeyValues = pk.map((name) => row.get(name));
            const oldKey = this._serializeKeyValues(oldKeyValues);
            const newKeyValues = pk.map((name) => nextValues[name]);
            const newKey = this._serializeKeyValues(newKeyValues);
            if (oldKey !== newKey) {
                if (this._pkIndex.get(oldKey) === row) {
                    this._pkIndex.delete(oldKey);
                }
                this._pkIndex.set(newKey, row);
            }
        }

        for (const [columnName, index] of this._uniqueIndexes.entries()) {
            const oldValueKey = this._serializeIndexValue(row.get(columnName));
            const newValueKey = this._serializeIndexValue(nextValues[columnName]);

            if (oldValueKey !== null && oldValueKey !== newValueKey && index.get(oldValueKey) === row) {
                index.delete(oldValueKey);
            }
            if (newValueKey !== null) {
                index.set(newValueKey, row);
            }
        }

        this._applyUniqueConstraintsIndexUpdate(row, nextValues);
    }

    _indexUniqueConstraints(row) {
        const constraints = typeof this._table?.getUniqueConstraints === 'function'
            ? this._table.getUniqueConstraints()
            : [];
        if (constraints.length === 0) {
            return;
        }
        if (this._uniqueConstraintIndexes.size === 0) {
            this._rebuildUniqueConstraintIndexes();
        }
        for (const constraint of constraints) {
            const index = this._uniqueConstraintIndexes.get(constraint.name);
            if (!index) continue;
            const keyValues = constraint.columns.map((name) => row.get(name));
            if (keyValues.some((v) => v === null || v === undefined)) {
                continue;
            }
            const key = this._serializeKeyValues(keyValues);
            const existing = index.get(key);
            if (existing && existing !== row) {
                throw new ConstraintViolationError(
                    `Constraint violation: duplicate key for unique constraint '${constraint.name}'`
                );
            }
            index.set(key, row);
        }
    }

    _unindexUniqueConstraints(row) {
        const constraints = typeof this._table?.getUniqueConstraints === 'function'
            ? this._table.getUniqueConstraints()
            : [];
        if (constraints.length === 0 || this._uniqueConstraintIndexes.size === 0) {
            return;
        }
        for (const constraint of constraints) {
            const index = this._uniqueConstraintIndexes.get(constraint.name);
            if (!index) continue;
            const keyValues = constraint.columns.map((name) => row.get(name));
            if (keyValues.some((v) => v === null || v === undefined)) {
                continue;
            }
            const key = this._serializeKeyValues(keyValues);
            if (index.get(key) === row) {
                index.delete(key);
            }
        }
    }

    _reindexUniqueConstraints(row, nextValues) {
        if (row.getRowState() === DataRowState.DETACHED) {
            return;
        }
        this._validateUniqueConstraintsForValues(row, nextValues);
        this._applyUniqueConstraintsIndexUpdate(row, nextValues);
    }

    _validateUniqueConstraintsForValues(row, nextValues) {
        const constraints = typeof this._table?.getUniqueConstraints === 'function'
            ? this._table.getUniqueConstraints()
            : [];
        if (constraints.length === 0) {
            return;
        }
        if (this._uniqueConstraintIndexes.size === 0) {
            this._rebuildUniqueConstraintIndexes();
            for (const r of this._rows) {
                this._indexUniqueConstraints(r);
            }
        }
        for (const constraint of constraints) {
            const index = this._uniqueConstraintIndexes.get(constraint.name);
            if (!index) continue;
            const keyValues = constraint.columns.map((name) => nextValues[name]);
            if (keyValues.some((v) => v === null || v === undefined)) {
                continue;
            }
            const key = this._serializeKeyValues(keyValues);
            const existing = index.get(key);
            if (existing && existing !== row) {
                throw new ConstraintViolationError(
                    `Constraint violation: duplicate key for unique constraint '${constraint.name}'`
                );
            }
        }
    }

    _applyUniqueConstraintsIndexUpdate(row, nextValues) {
        const constraints = typeof this._table?.getUniqueConstraints === 'function'
            ? this._table.getUniqueConstraints()
            : [];
        if (constraints.length === 0 || this._uniqueConstraintIndexes.size === 0) {
            return;
        }
        for (const constraint of constraints) {
            const index = this._uniqueConstraintIndexes.get(constraint.name);
            if (!index) continue;

            const oldKeyValues = constraint.columns.map((name) => row.get(name, 'current'));
            const newKeyValues = constraint.columns.map((name) => nextValues[name]);

            if (!oldKeyValues.some((v) => v === null || v === undefined)) {
                const oldKey = this._serializeKeyValues(oldKeyValues);
                if (index.get(oldKey) === row) {
                    index.delete(oldKey);
                }
            }
            if (!newKeyValues.some((v) => v === null || v === undefined)) {
                const newKey = this._serializeKeyValues(newKeyValues);
                index.set(newKey, row);
            }
        }
    }

    _enforceCheckConstraints(row, values) {
        if (!this._table || typeof this._table._shouldEnforceConstraints !== 'function' || this._table._shouldEnforceConstraints() !== true) {
            return;
        }
        if (typeof this._table._evaluateCheckConstraints !== 'function') {
            return;
        }
        this._table._evaluateCheckConstraints(row, values);
        const dataSet = this._table._dataSet;
        if (dataSet && typeof dataSet._evaluateForeignKeyConstraints === 'function') {
            dataSet._evaluateForeignKeyConstraints(row, values);
        }
    }
}

module.exports = DataRowCollection;
