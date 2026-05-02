const { inspect } = require('node:util');
const DebugPreview = require('./DebugPreview');
const DebugSchemaSerializer = require('./DebugSchemaSerializer');
const DebugTableSerializer = require('./DebugTableSerializer');

function formatDataTable(table, options = {}) {
    const schema = DebugSchemaSerializer.getTableSchema(table);
    const preview = DebugPreview.getTablePreview(table, options.maxRows);
    const columns = schema.columns.map(column => `${column.name}:${column.dataType || 'any'}`);
    const lines = [
        `${formatName('DataTable', schema.name)} {`,
        `  Columns: [${columns.join(', ')}]`,
        `  Rows: ${previewSourceRowCount(table)}`
    ];

    if (schema.primaryKey.length > 0) {
        lines.push(`  PrimaryKey: [${schema.primaryKey.join(', ')}]`);
    }

    lines.push('  Preview:');
    lines.push(...indentLines(formatRecordTable(preview, schema.columns.map(column => column.name)).split('\n'), 2));
    lines.push('}');
    return lines.join('\n');
}

function formatDataRow(row) {
    const view = DebugTableSerializer.rowToDebugView(row);
    return [
        `${formatName('DataRow', view.tableName)} {`,
        `  RowState: ${view.rowState}`,
        `  Values: ${inspect(view.values, { depth: 3, colors: false, compact: true })}`,
        '}'
    ].join('\n');
}

function formatDataColumn(column) {
    const view = DebugTableSerializer.columnToDebugView(column);
    return [
        `${formatName('DataColumn', view.name)} {`,
        `  Type: ${view.dataType || 'any'}`,
        `  AllowNull: ${view.allowNull}`,
        `  ReadOnly: ${view.readOnly}`,
        `  Unique: ${view.unique}`,
        `  PrimaryKey: ${view.primaryKey}`,
        '}'
    ].join('\n');
}

function formatDataView(view, options = {}) {
    const debugView = DebugTableSerializer.viewToDebugView(view, { maxRows: options.maxRows });
    const sourceTable = view._table;
    const columns = sourceTable ? sourceTable.columns.toArray().map(column => column.columnName) : [];
    const lines = [
        `${formatName('DataView', debugView.sourceTable)} {`,
        `  Rows: ${debugView.rowCount}`
    ];

    if (debugView.filter) {
        lines.push(`  Filter: ${debugView.filter}`);
    }
    if (debugView.sort) {
        lines.push(`  Sort: ${debugView.sort}`);
    }

    lines.push('  Preview:');
    lines.push(...indentLines(formatRecordTable(debugView.preview, columns).split('\n'), 2));
    lines.push('}');
    return lines.join('\n');
}

function formatDataSet(dataSet) {
    const schema = DebugSchemaSerializer.getDataSetSchema(dataSet);
    const lines = [
        `${formatName('DataSet', schema.name)} {`,
        `  Tables: ${schema.tableCount}`,
        `  Relations: ${schema.relations.length}`
    ];

    if (schema.tables.length > 0) {
        lines.push('  TableList:');
        for (const table of schema.tables) {
            const rowCount = dataSet.table(table.name).rows.count;
            lines.push(`    - ${table.name || '(unnamed)'} (${rowCount} rows, ${table.columnCount} columns)`);
        }
    }

    lines.push('}');
    return lines.join('\n');
}

function formatDataColumnCollection(collection) {
    const columns = collection.toArray().map(column => `${column.columnName}:${column.dataType || 'any'}`);
    return `DataColumnCollection [${columns.join(', ')}]`;
}

function formatDataRowCollection(collection, options = {}) {
    const table = collection._table;
    const columns = table ? table.columns.toArray() : [];
    const columnNames = columns.map(column => column.columnName);
    const records = collection.toArray()
        .filter(row => typeof row.getRowState !== 'function' || row.getRowState() !== 'DELETED')
        .slice(0, DebugPreview.normalizeMaxRows(options.maxRows))
        .map(row => DebugTableSerializer.rowToObject(row, { columns, serializeValues: true }));

    return [
        'DataRowCollection {',
        `  Rows: ${collection.count}`,
        '  Preview:',
        ...indentLines(formatRecordTable(records, columnNames).split('\n'), 2),
        '}'
    ].join('\n');
}

function formatRecordTable(records, columns = []) {
    const orderedColumns = normalizeColumns(records, columns);
    if (orderedColumns.length === 0) {
        return records.length === 0 ? '(empty)' : '(no columns)';
    }
    if (records.length === 0) {
        return '(no rows)';
    }

    const headers = ['#', ...orderedColumns];
    const rows = records.map((record, index) => [
        String(index),
        ...orderedColumns.map(column => formatCell(record[column]))
    ]);
    const widths = headers.map((header, index) => {
        const rowWidths = rows.map(row => row[index].length);
        return Math.max(header.length, ...rowWidths);
    });

    const top = makeBorder('┌', '┬', '┐', widths);
    const header = makeRow(headers, widths);
    const separator = makeBorder('├', '┼', '┤', widths);
    const body = rows.map(row => makeRow(row, widths));
    const bottom = makeBorder('└', '┴', '┘', widths);

    return [top, header, separator, ...body, bottom].join('\n');
}

function normalizeColumns(records, columns) {
    const result = [];
    for (const column of columns || []) {
        if (column && !result.includes(column)) {
            result.push(column);
        }
    }
    for (const record of records) {
        for (const column of Object.keys(record)) {
            if (!result.includes(column)) {
                result.push(column);
            }
        }
    }
    return result;
}

function makeBorder(left, middle, right, widths) {
    return left + widths.map(width => '─'.repeat(width + 2)).join(middle) + right;
}

function makeRow(values, widths) {
    return '│ ' + values.map((value, index) => padRight(value, widths[index])).join(' │ ') + ' │';
}

function padRight(value, width) {
    return value + ' '.repeat(Math.max(0, width - value.length));
}

function formatCell(value) {
    if (value === null) {
        return 'null';
    }
    if (value === undefined) {
        return '';
    }
    if (typeof value === 'string') {
        return value;
    }
    if (typeof value === 'number' || typeof value === 'boolean' || typeof value === 'bigint') {
        return String(value);
    }
    return inspect(value, { depth: 2, colors: false, compact: true, breakLength: Infinity });
}

function previewSourceRowCount(table) {
    if (!table || !table.rows) {
        return 0;
    }
    return typeof table.rows.count === 'number' ? table.rows.count : table.rows.toArray().length;
}

function indentLines(lines, spaces) {
    const prefix = ' '.repeat(spaces);
    return lines.map(line => `${prefix}${line}`);
}

function formatName(type, name) {
    return name ? `${type} "${name}"` : type;
}

module.exports = {
    formatDataColumn,
    formatDataColumnCollection,
    formatDataRow,
    formatDataRowCollection,
    formatDataSet,
    formatDataTable,
    formatDataView,
    formatRecordTable
};
