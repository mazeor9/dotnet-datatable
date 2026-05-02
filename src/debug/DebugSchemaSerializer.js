function serializeColumn(column) {
    return {
        name: column.columnName,
        dataType: column.dataType,
        allowNull: column.allowNull,
        defaultValue: serializeDebugValue(column.defaultValue),
        readOnly: column.readOnly,
        unique: column.unique,
        primaryKey: column.isPrimaryKey,
        ordinal: column.ordinal,
        caption: column.caption,
        maxLength: column.maxLength,
        sourceColumn: column.sourceColumn,
        metadata: serializeDebugValue(column.metadata)
    };
}

function getTableSchema(table) {
    const columns = getColumns(table).map(serializeColumn);

    return {
        type: 'DataTableSchema',
        name: table.tableName,
        tableName: table.tableName,
        columns,
        columnCount: columns.length,
        primaryKey: typeof table.getPrimaryKey === 'function' ? table.getPrimaryKey() : [],
        caseSensitive: table.caseSensitive === true
    };
}

function getDataSetSchema(dataSet) {
    const tables = Array.from(dataSet.tables.values()).map(getTableSchema);

    return {
        type: 'DataSetSchema',
        name: dataSet.dataSetName,
        dataSetName: dataSet.dataSetName,
        tables,
        tableCount: tables.length,
        relations: (dataSet.relations || []).map(serializeRelation)
    };
}

function serializeRelation(relation) {
    return {
        name: relation.relationName,
        parentTable: relation.parentTable ? relation.parentTable.tableName : undefined,
        parentColumn: relation.parentColumn ? relation.parentColumn.columnName : undefined,
        childTable: relation.childTable ? relation.childTable.tableName : undefined,
        childColumn: relation.childColumn ? relation.childColumn.columnName : undefined
    };
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

function serializeDebugValue(value, seen = new WeakSet()) {
    if (value === null || value === undefined) {
        return value;
    }
    if (value instanceof Date) {
        return Number.isNaN(value.getTime()) ? null : value.toISOString();
    }
    if (typeof value === 'bigint') {
        return value.toString();
    }
    if (typeof value === 'function') {
        return `[Function${value.name ? `: ${value.name}` : ''}]`;
    }
    if (typeof value === 'symbol') {
        return value.toString();
    }
    if (typeof value !== 'object') {
        return value;
    }
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
        return {
            type: 'Buffer',
            length: value.length,
            preview: value.toString('hex', 0, Math.min(value.length, 16))
        };
    }
    if (seen.has(value)) {
        return '[Circular]';
    }

    seen.add(value);
    if (Array.isArray(value)) {
        const array = value.map(item => serializeDebugValue(item, seen));
        seen.delete(value);
        return array;
    }

    const output = {};
    for (const [key, entryValue] of Object.entries(value)) {
        output[key] = serializeDebugValue(entryValue, seen);
    }
    seen.delete(value);
    return output;
}

module.exports = {
    getDataSetSchema,
    getTableSchema,
    serializeColumn,
    serializeDebugValue,
    serializeRelation
};
