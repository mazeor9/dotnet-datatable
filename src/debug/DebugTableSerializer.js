const DebugPreview = require('./DebugPreview');
const DebugSchemaSerializer = require('./DebugSchemaSerializer');

function tableToArray(table, options = {}) {
    return getTableRows(table)
        .filter(row => shouldIncludeRow(row, options))
        .map(row => rowToObject(row, {
            columns: getColumns(table),
            serializeValues: options.serializeValues === true
        }));
}

function viewToArray(view, options = {}) {
    const table = view._table;
    return view.getRows()
        .filter(row => shouldIncludeRow(row, options))
        .map(row => rowToObject(row, {
            columns: getColumns(table),
            serializeValues: options.serializeValues === true
        }));
}

function rowToObject(row, options = {}) {
    const columns = options.columns || getColumns(row._table);
    const output = {};

    if (columns.length > 0) {
        for (const column of columns) {
            output[column.columnName] = readRowValue(row, column.columnName, options.serializeValues);
        }
        return output;
    }

    for (const [key, value] of Object.entries(row._values || {})) {
        output[key] = options.serializeValues
            ? DebugSchemaSerializer.serializeDebugValue(value)
            : value;
    }
    return output;
}

function columnToDebugView(column) {
    return {
        type: 'DataColumn',
        ...DebugSchemaSerializer.serializeColumn(column),
        tableName: column.table ? column.table.tableName : undefined
    };
}

function rowToDebugView(row) {
    return {
        type: 'DataRow',
        tableName: row._table ? row._table.tableName : undefined,
        rowState: typeof row.getRowState === 'function' ? row.getRowState() : row._rowState,
        values: rowToObject(row, { serializeValues: true })
    };
}

function tableToDebugView(table, options = {}) {
    const maxRows = DebugPreview.normalizeMaxRows(options.maxRows);
    const schema = DebugSchemaSerializer.getTableSchema(table);
    const rows = tableToArray(table, {
        includeDeleted: options.includeDeleted === true,
        serializeValues: true
    });

    return {
        type: 'DataTable',
        name: table.tableName,
        tableName: table.tableName,
        columns: schema.columns,
        rows,
        rowCount: rows.length,
        columnCount: schema.columnCount,
        primaryKey: schema.primaryKey,
        preview: DebugPreview.limitRecords(rows, maxRows)
    };
}

function viewToDebugView(view, options = {}) {
    const maxRows = DebugPreview.normalizeMaxRows(options.maxRows);
    const rows = viewToArray(view, {
        includeDeleted: options.includeDeleted === true,
        serializeValues: true
    });

    return {
        type: 'DataView',
        sourceTable: view._table ? view._table.tableName : undefined,
        rows,
        rowCount: rows.length,
        sort: describeSorts(view._sorts),
        filter: describeFilters(view._filters),
        preview: DebugPreview.limitRecords(rows, maxRows)
    };
}

function dataSetToDebugView(dataSet, options = {}) {
    const tables = Array.from(dataSet.tables.values()).map(table => tableToDebugView(table, options));

    return {
        type: 'DataSet',
        name: dataSet.dataSetName,
        dataSetName: dataSet.dataSetName,
        tables,
        tableCount: tables.length,
        relations: (dataSet.relations || []).map(DebugSchemaSerializer.serializeRelation)
    };
}

function getTableRows(table) {
    if (!table || !table.rows) {
        return [];
    }
    if (Array.isArray(table.rows._rows)) {
        return [...table.rows._rows];
    }
    if (typeof table.rows.toArray === 'function') {
        return table.rows.toArray();
    }
    return Array.from(table.rows);
}

function getColumns(table) {
    if (!table || !table.columns) {
        return [];
    }
    if (typeof table.columns.toArray === 'function') {
        return table.columns.toArray();
    }
    if (table.columns._columns instanceof Map) {
        return Array.from(table.columns._columns.values());
    }
    return Array.from(table.columns);
}

function shouldIncludeRow(row, options) {
    if (options.includeDeleted === true || typeof row.getRowState !== 'function') {
        return true;
    }
    return row.getRowState() !== 'DELETED';
}

function readRowValue(row, columnName, serializeValues) {
    const value = typeof row.get === 'function'
        ? row.get(columnName)
        : row._values[columnName];

    return serializeValues
        ? DebugSchemaSerializer.serializeDebugValue(value)
        : value;
}

function describeFilters(filters) {
    if (!filters || filters.length === 0) {
        return undefined;
    }
    return filters.length === 1 ? '1 filter' : `${filters.length} filters`;
}

function describeSorts(sorts) {
    if (!sorts || sorts.length === 0) {
        return undefined;
    }

    return sorts.map(sort => {
        if (sort.comparer) {
            return '[comparer]';
        }
        if (sort.expression) {
            return `[expression] ${sort.direction || 'asc'}`;
        }
        return `${sort.columnName} ${sort.direction || 'asc'}`;
    }).join(', ');
}

module.exports = {
    columnToDebugView,
    dataSetToDebugView,
    rowToDebugView,
    rowToObject,
    tableToArray,
    tableToDebugView,
    viewToArray,
    viewToDebugView
};
