const DataRowState = require('../enums/DataRowState');
const { areValuesEqual, cloneValue, cloneValues } = require('../utils/typeUtils');

class DataTableChangeSet {
    constructor(tableName, primaryKey, changes = {}) {
        this.tableName = tableName || '';
        this.primaryKey = Array.isArray(primaryKey) ? [...primaryKey] : [];
        this.added = changes.added || [];
        this.modified = changes.modified || [];
        this.deleted = changes.deleted || [];
    }

    static fromTable(table, options = {}) {
        const primaryKey = typeof table.getPrimaryKey === 'function'
            ? table.getPrimaryKey()
            : [];
        const changes = {
            added: [],
            modified: [],
            deleted: []
        };

        for (const row of table.rows || []) {
            const state = typeof row.getRowState === 'function'
                ? row.getRowState()
                : DataRowState.UNCHANGED;

            if (state === DataRowState.ADDED) {
                changes.added.push(createAddedChange(table, row, primaryKey, options));
            } else if (state === DataRowState.MODIFIED) {
                changes.modified.push(createModifiedChange(table, row, primaryKey, options));
            } else if (state === DataRowState.DELETED) {
                changes.deleted.push(createDeletedChange(table, row, primaryKey, options));
            }
        }

        return new DataTableChangeSet(table.tableName, primaryKey, changes);
    }

    get count() {
        return this.added.length + this.modified.length + this.deleted.length;
    }

    get hasChanges() {
        return this.count > 0;
    }

    isEmpty() {
        return !this.hasChanges;
    }

    toObject() {
        return {
            tableName: this.tableName,
            primaryKey: [...this.primaryKey],
            added: this.added.map(cloneChange),
            modified: this.modified.map(cloneChange),
            deleted: this.deleted.map(cloneChange),
            count: this.count,
            hasChanges: this.hasChanges
        };
    }

    toJSON() {
        return this.toObject();
    }
}

class DataSetChangeSet {
    constructor(dataSetName, tables = []) {
        this.dataSetName = dataSetName || '';
        this.tables = tables;
    }

    static fromDataSet(dataSet, options = {}) {
        const tables = [];
        for (const table of dataSet.tables.values()) {
            const changeSet = DataTableChangeSet.fromTable(table, options);
            if (options.includeUnchangedTables === true || changeSet.hasChanges) {
                tables.push(changeSet);
            }
        }
        return new DataSetChangeSet(dataSet.dataSetName, tables);
    }

    get count() {
        return this.tables.reduce((total, table) => total + table.count, 0);
    }

    get hasChanges() {
        return this.count > 0;
    }

    isEmpty() {
        return !this.hasChanges;
    }

    table(tableName) {
        return this.tables.find(changeSet => changeSet.tableName === tableName) || null;
    }

    toObject() {
        return {
            dataSetName: this.dataSetName,
            tables: this.tables.map(table => table.toObject()),
            count: this.count,
            hasChanges: this.hasChanges
        };
    }

    toJSON() {
        return this.toObject();
    }
}

function createAddedChange(table, row, primaryKey, options) {
    const values = getRowValues(table, row, 'current', options);
    const keyValues = getRowValues(table, row, 'current', { ...options, includeColumns: null, excludeColumns: null });
    return {
        state: DataRowState.ADDED,
        tableName: table.tableName,
        key: pickKey(keyValues, primaryKey),
        values
    };
}

function createModifiedChange(table, row, primaryKey, options) {
    const values = getRowValues(table, row, 'current', options);
    const originalValues = getRowValues(table, row, 'original', options);
    const keyValues = getRowValues(table, row, 'current', { ...options, includeColumns: null, excludeColumns: null });
    const originalKeyValues = getRowValues(table, row, 'original', { ...options, includeColumns: null, excludeColumns: null });
    const changedColumns = [];

    for (const column of table.columns) {
        const columnName = column.columnName;
        if (!areValuesEqual(values[columnName], originalValues[columnName])) {
            changedColumns.push(columnName);
        }
    }

    return {
        state: DataRowState.MODIFIED,
        tableName: table.tableName,
        key: pickKey(keyValues, primaryKey),
        originalKey: pickKey(originalKeyValues, primaryKey),
        values,
        originalValues,
        changedColumns
    };
}

function createDeletedChange(table, row, primaryKey, options) {
    const values = getRowValues(table, row, 'current', options);
    const originalValues = getRowValues(table, row, 'original', options);
    const allValues = getRowValues(table, row, 'current', { ...options, includeColumns: null, excludeColumns: null });
    const allOriginalValues = getRowValues(table, row, 'original', { ...options, includeColumns: null, excludeColumns: null });
    const keySource = Object.keys(allOriginalValues).length > 0 ? allOriginalValues : allValues;

    return {
        state: DataRowState.DELETED,
        tableName: table.tableName,
        key: pickKey(keySource, primaryKey),
        values,
        originalValues
    };
}

function getRowValues(table, row, version, options) {
    const result = {};
    const source = version === 'original'
        ? row.originalValues || row._originalValues || {}
        : row.currentValues || row._values || {};

    for (const column of table.columns) {
        const columnName = column.columnName;
        if (!shouldIncludeColumn(columnName, options)) {
            continue;
        }
        const value = Object.prototype.hasOwnProperty.call(source, columnName)
            ? source[columnName]
            : undefined;
        result[columnName] = cloneValue(value);
    }

    return result;
}

function shouldIncludeColumn(columnName, options) {
    if (Array.isArray(options.includeColumns) && options.includeColumns.length > 0) {
        return options.includeColumns.includes(columnName);
    }
    if (Array.isArray(options.excludeColumns) && options.excludeColumns.includes(columnName)) {
        return false;
    }
    return true;
}

function pickKey(values, primaryKey) {
    if (!Array.isArray(primaryKey) || primaryKey.length === 0) {
        return null;
    }

    const key = {};
    for (const columnName of primaryKey) {
        key[columnName] = cloneValue(values[columnName]);
    }
    return key;
}

function cloneChange(change) {
    return cloneValues(change);
}

module.exports = {
    DataSetChangeSet,
    DataTableChangeSet
};
