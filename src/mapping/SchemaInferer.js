const TypeMapper = require('./TypeMapper');
const { SchemaMismatchError } = require('../errors');
const {
    createColumnNameResolver,
    normalizeRecords,
    shouldIncludeColumn
} = require('../utils/objectUtils');

class SchemaInferer {
    static infer(rows, options = {}) {
        const records = options.recordsAlreadyNormalized
            ? rows
            : normalizeRecords(rows, options);
        const overrides = this.normalizeColumnDefinitions(options.columns, options);
        const map = new Map();

        for (const [columnName, definition] of overrides.entries()) {
            map.set(columnName, {
                name: columnName,
                types: [],
                presentCount: 0,
                definition
            });
        }

        for (const record of records) {
            for (const [columnName, value] of Object.entries(record)) {
                if (!map.has(columnName)) {
                    map.set(columnName, {
                        name: columnName,
                        types: [],
                        presentCount: 0,
                        definition: {}
                    });
                }
                const entry = map.get(columnName);
                entry.types.push(TypeMapper.inferValueType(value));
                entry.presentCount++;
            }
        }

        if (records.length === 0 && map.size === 0) {
            throw new SchemaMismatchError('Cannot infer schema from empty rows. Provide columns option.');
        }

        const primaryKey = normalizePrimaryKey(options.primaryKey);
        const columnDefs = [];

        for (const entry of map.values()) {
            const override = entry.definition || {};
            const type = TypeMapper.normalizeType(override.type || override.dataType || TypeMapper.reconcileTypes(entry.types));
            const allowNull = override.allowNull !== undefined
                ? override.allowNull
                : options.allowNull !== undefined
                    ? options.allowNull
                    : entry.types.includes('null') ||
                        entry.types.includes('undefined') ||
                        entry.presentCount < records.length;

            columnDefs.push({
                name: entry.name,
                columnName: entry.name,
                type,
                dataType: type,
                allowNull,
                defaultValue: override.defaultValue !== undefined ? override.defaultValue : null,
                maxLength: override.maxLength,
                unique: override.unique === true,
                primaryKey: override.primaryKey === true || primaryKey.includes(entry.name),
                readOnly: override.readOnly === true,
                caption: override.caption,
                sourceColumn: override.sourceColumn,
                metadata: override.metadata
            });
        }

        return columnDefs;
    }

    static normalizeColumnDefinitions(columns, options = {}) {
        const definitions = new Map();
        if (!columns) {
            return definitions;
        }

        const resolver = createColumnNameResolver(options);

        if (Array.isArray(columns)) {
            for (const item of columns) {
                if (typeof item === 'string') {
                    const name = resolver(item);
                    if (shouldIncludeColumn(item, name, options)) {
                        definitions.set(name, { sourceColumn: item });
                    }
                    continue;
                }

                if (item && typeof item === 'object') {
                    const sourceName = item.name || item.columnName || item.sourceColumn;
                    if (!sourceName) {
                        continue;
                    }
                    const name = resolver(sourceName);
                    if (shouldIncludeColumn(sourceName, name, options)) {
                        definitions.set(name, {
                            ...item,
                            sourceColumn: item.sourceColumn || sourceName
                        });
                    }
                }
            }
            return definitions;
        }

        for (const [sourceName, definition] of Object.entries(columns)) {
            const name = resolver(sourceName);
            if (!shouldIncludeColumn(sourceName, name, options)) {
                continue;
            }
            definitions.set(name, {
                ...(definition || {}),
                sourceColumn: definition?.sourceColumn || sourceName
            });
        }

        return definitions;
    }
}

function normalizePrimaryKey(primaryKey) {
    if (!primaryKey) {
        return [];
    }
    return Array.isArray(primaryKey) ? primaryKey : [primaryKey];
}

module.exports = SchemaInferer;
