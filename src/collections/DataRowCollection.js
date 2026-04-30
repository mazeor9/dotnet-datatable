const DataRow = require('../DataRow');
const DataRowState = require('../enums/DataRowState');
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
         // direct access to index column
         const func = (index) => this._rows[index];
        
        // Copia tutte le proprietà e metodi nell'oggetto funzione
        Object.setPrototypeOf(func, DataRowCollection.prototype);
        Object.assign(func, this);
        
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

        this._validateRowConstraints(dataRow);
        dataRow._attachToTable(this._table);
        this._rows.push(dataRow);
        return dataRow;
    }

    /**
     * @param {DataRow} row - The row instance to remove from the collection
     */
    remove(row) {
        const index = this._rows.indexOf(row);
        if (index !== -1) {
            this._rows.splice(index, 1);
            if (row instanceof DataRow) {
                row._setRowState(DataRowState.DETACHED);
            }
        }
    }

    /**
     * @param {number} index - The index of the row to remove
     */
    removeAt(index) {
        if (index >= 0 && index < this._rows.length) {
            const [row] = this._rows.splice(index, 1);
            if (row instanceof DataRow) {
                row._setRowState(DataRowState.DETACHED);
            }
        }
    }

    clear() {
        for (const row of this._rows) {
            if (row instanceof DataRow) {
                row._setRowState(DataRowState.DETACHED);
            }
        }
        this._rows = [];
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

        for (const row of this._rows) {
            if (row.getRowState() === DataRowState.DELETED) continue;
            const rowKeyValues = pk.map((name) => row.get(name));
            const same = rowKeyValues.every((v, i) => row._areEqual(v, keyValues[i]));
            if (same) return row;
        }
        return null;
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
    }

    _validateRowConstraints(row) {
        const columns = this._table.columns.toArray();
        const pk = this._table.columns.getPrimaryKey();

        for (const col of columns) {
            const value = row.get(col.columnName);
            if ((value === null || value === undefined) && col.allowNull === false) {
                throw new ConstraintViolationError(`Column '${col.columnName}' does not allow null values`);
            }
        }

        for (const col of columns) {
            if (!col.unique) continue;
            if (pk.length > 1 && pk.includes(col.columnName)) continue;
            const value = row.get(col.columnName);
            if (value === null || value === undefined) continue;
            for (const existing of this._rows) {
                if (existing.getRowState() === DataRowState.DELETED) continue;
                if (existing._areEqual(existing.get(col.columnName), value)) {
                    if (col.isPrimaryKey) {
                        throw new DuplicatePrimaryKeyError(`Duplicate primary key for column '${col.columnName}': ${value}`);
                    }
                    throw new ConstraintViolationError(
                        `Constraint violation: duplicate value for unique column '${col.columnName}'`
                    );
                }
            }
        }

        if (pk.length > 0) {
            const keyValues = pk.map((name) => row.get(name));
            if (keyValues.some((v) => v === null || v === undefined)) {
                throw new ConstraintViolationError(
                    `Primary key '${pk.join(",")}' cannot contain null values`
                );
            }
            for (const existing of this._rows) {
                if (existing.getRowState() === DataRowState.DELETED) continue;
                const other = pk.map((name) => existing.get(name));
                const same = other.every((v, i) => existing._areEqual(v, keyValues[i]));
                if (same) {
                    throw new DuplicatePrimaryKeyError(
                        `Duplicate primary key (${pk.join(",")}): ${keyValues.join(",")}`
                    );
                }
            }
        }
    }
}

module.exports = DataRowCollection;
