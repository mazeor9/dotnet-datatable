const TypeMapper = require('./TypeMapper');
const {
    createColumnNameResolver,
    shouldIncludeColumn
} = require('../utils/objectUtils');

class ColumnMetadataNormalizer {
    static normalize(fields, options = {}) {
        if (!Array.isArray(fields)) {
            return [];
        }

        const resolver = createColumnNameResolver(options);
        return fields
            .map(field => this.normalizeField(field, { ...options, columnNameResolver: resolver }))
            .filter(Boolean);
    }

    static normalizeField(field, options = {}) {
        if (!field || typeof field !== 'object') {
            return null;
        }

        const sourceName = field.name ||
            field.columnName ||
            field.column ||
            field.alias ||
            field.columnAlias;

        if (!sourceName) {
            return null;
        }

        const resolver = options.columnNameResolver || createColumnNameResolver(options);
        const name = resolver(sourceName);
        if (!shouldIncludeColumn(sourceName, name, options)) {
            return null;
        }

        const provider = options.provider || inferProvider(field);
        const type = TypeMapper.fromDatabaseType(
            field.dataType || field.typeName || field.databaseType,
            provider,
            field,
            options
        );

        return {
            name,
            columnName: name,
            type,
            dataType: type,
            allowNull: field.allowNull ?? field.nullable ?? true,
            maxLength: field.columnLength ?? field.length ?? field.maxLength,
            precision: field.precision,
            scale: field.decimals ?? field.scale,
            sourceColumn: sourceName,
            metadata: { ...field }
        };
    }
}

function inferProvider(field) {
    if (field.dataTypeID !== undefined || field.tableID !== undefined || field.columnID !== undefined) {
        return 'pg';
    }
    if (field.columnType !== undefined || field.flags !== undefined || field.decimals !== undefined) {
        return 'mysql2';
    }
    if (field.type && (field.type.name || field.type.declaration)) {
        return 'mssql';
    }
    return undefined;
}

module.exports = ColumnMetadataNormalizer;
