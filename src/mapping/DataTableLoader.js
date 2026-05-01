const DataRowState = require('../enums/DataRowState');
const {
    ColumnNotFoundError,
    SchemaMismatchError,
    TypeMismatchError
} = require('../errors');
const ColumnMetadataNormalizer = require('./ColumnMetadataNormalizer');
const SchemaInferer = require('./SchemaInferer');
const TypeMapper = require('./TypeMapper');
const {
    createColumnNameResolver,
    normalizeRecord,
    normalizeRecords
} = require('../utils/objectUtils');
const {
    cloneValues,
    describeValueType,
    normalizeRowState
} = require('../utils/typeUtils');

class DataTableLoader {
    static load(table, rows, options = {}) {
        const opts = this._normalizeOptions(options);
        const records = normalizeRecords(this._ensureRows(rows), opts);

        if (opts.clearBeforeLoad || opts.append === false) {
            table.clear();
        }

        const metadataColumns = opts.useFieldMetadata && opts.columnMetadata
            ? ColumnMetadataNormalizer.normalize(opts.columnMetadata, opts)
            : [];

        const inferredColumns = opts.inferSchema !== false
            ? this._inferColumns(records, opts, table)
            : this._columnsFromOptions(opts);

        this._ensureColumns(table, [...metadataColumns, ...inferredColumns], records, opts);
        this._ensurePrimaryKey(table, opts.primaryKey);

        for (const record of records) {
            this.addRecord(table, record, { ...opts, recordAlreadyNormalized: true });
        }

        return table;
    }

    static addRecord(table, record, options = {}) {
        const opts = this._normalizeOptions(options);
        const normalized = opts.recordAlreadyNormalized
            ? record
            : normalizeRecord(record, opts);

        this._handleExtraColumns(table, normalized, opts);

        const row = table.newRow();
        for (const column of table.columns) {
            if (!Object.prototype.hasOwnProperty.call(normalized, column.columnName)) {
                continue;
            }
            this._setRowColumnValue(row, column, normalized[column.columnName], opts);
        }

        table.rows.add(row);
        row._setRowState(normalizeRowState(opts.rowState, DataRowState.UNCHANGED));

        if (opts.preserveOriginalValues !== false) {
            row._originalValues = cloneValues(row._values);
        }

        return row;
    }

    static normalizeRows(rows, options = {}) {
        return normalizeRecords(this._ensureRows(rows), this._normalizeOptions(options));
    }

    static _normalizeOptions(options = {}) {
        const opts = {
            tableName: '',
            primaryKey: null,
            columns: null,
            includeColumns: null,
            excludeColumns: null,
            renameColumns: null,
            columnNameTransform: 'none',
            inferSchema: true,
            useFieldMetadata: true,
            autoCreateColumns: true,
            validateSchema: true,
            convertTypes: true,
            rowState: DataRowState.UNCHANGED,
            preserveOriginalValues: true,
            allowNull: true,
            strict: false,
            clearBeforeLoad: false,
            append: true,
            ignoreExtraColumns: false,
            throwOnExtraColumns: false,
            columnMetadata: null,
            provider: null,
            ...options
        };
        opts.columnNameResolver = createColumnNameResolver(opts);
        return opts;
    }

    static _ensureRows(rows) {
        if (!Array.isArray(rows)) {
            throw new SchemaMismatchError('Rows must be an array of objects.');
        }
        return rows;
    }

    static _inferColumns(records, opts, table) {
        if (table.columns.count > 0 && opts.autoCreateColumns !== true) {
            return this._columnsFromOptions(opts);
        }
        if (records.length === 0 && !opts.columns) {
            if (table.columns.count > 0 || opts.columnMetadata) {
                return [];
            }
            throw new SchemaMismatchError('Cannot infer schema from empty rows. Provide columns option.');
        }
        return SchemaInferer.infer(records, {
            ...opts,
            recordsAlreadyNormalized: true
        });
    }

    static _columnsFromOptions(opts) {
        return Array.from(SchemaInferer.normalizeColumnDefinitions(opts.columns, opts).entries())
            .map(([name, definition]) => ({
                name,
                columnName: name,
                type: TypeMapper.normalizeType(definition.type || definition.dataType || 'any'),
                dataType: TypeMapper.normalizeType(definition.type || definition.dataType || 'any'),
                allowNull: definition.allowNull !== undefined ? definition.allowNull : opts.allowNull,
                defaultValue: definition.defaultValue !== undefined ? definition.defaultValue : null,
                maxLength: definition.maxLength,
                unique: definition.unique === true,
                primaryKey: definition.primaryKey === true,
                readOnly: definition.readOnly === true,
                caption: definition.caption,
                sourceColumn: definition.sourceColumn,
                metadata: definition.metadata
            }));
    }

    static _ensureColumns(table, columnDefs, records, opts) {
        const added = new Set();

        for (const definition of columnDefs) {
            if (!definition || !definition.name) {
                continue;
            }
            if (added.has(definition.name)) {
                this._applyColumnDefinition(table, definition);
                continue;
            }
            added.add(definition.name);
            if (!table.columnExists(definition.name)) {
                table.addColumn(definition.name, definition.type || definition.dataType || 'any', {
                    allowNull: definition.allowNull !== undefined ? definition.allowNull : opts.allowNull,
                    defaultValue: definition.defaultValue !== undefined ? definition.defaultValue : null,
                    maxLength: definition.maxLength,
                    unique: definition.unique === true,
                    primaryKey: definition.primaryKey === true,
                    readOnly: definition.readOnly === true,
                    caption: definition.caption,
                    sourceColumn: definition.sourceColumn,
                    metadata: definition.metadata
                });
            } else {
                this._applyColumnDefinition(table, definition);
            }
        }

        const primaryKeyFromDefinitions = [...new Set(columnDefs
            .filter(definition => definition && definition.primaryKey === true)
            .map(definition => definition.name))];
        if (primaryKeyFromDefinitions.length > 0) {
            table.setPrimaryKey(primaryKeyFromDefinitions);
        }

        if (opts.autoCreateColumns) {
            for (const record of records) {
                for (const [columnName, value] of Object.entries(record)) {
                    if (!table.columnExists(columnName)) {
                        const type = TypeMapper.reconcileTypes([TypeMapper.inferValueType(value)]);
                        table.addColumn(columnName, type, { allowNull: opts.allowNull });
                    }
                }
            }
        }
    }

    static _applyColumnDefinition(table, definition) {
        if (!definition || !definition.name || !table.columnExists(definition.name)) {
            return;
        }
        const column = table.columns.get(definition.name);
        if ((column.dataType === null || column.dataType === 'any') && (definition.type || definition.dataType)) {
            column.dataType = definition.type || definition.dataType;
        }
        if (definition.allowNull !== undefined) column.allowNull = definition.allowNull;
        if (definition.defaultValue !== undefined) column.defaultValue = definition.defaultValue;
        if (definition.maxLength !== undefined) column.maxLength = definition.maxLength;
        if (definition.unique !== undefined) column.unique = definition.unique;
        if (definition.readOnly !== undefined) column.readOnly = definition.readOnly;
        if (definition.caption !== undefined) column.caption = definition.caption;
        if (definition.sourceColumn !== undefined) column.sourceColumn = definition.sourceColumn;
        if (definition.metadata !== undefined) column.metadata = definition.metadata;
    }

    static _ensurePrimaryKey(table, primaryKey) {
        if (!primaryKey) {
            return;
        }
        const columns = Array.isArray(primaryKey) ? primaryKey : [primaryKey];
        for (const columnName of columns) {
            if (!table.columnExists(columnName)) {
                throw new ColumnNotFoundError(`Missing primary key column: "${columnName}"`);
            }
        }
        table.setPrimaryKey(columns);
    }

    static _handleExtraColumns(table, record, opts) {
        const extraColumns = Object.keys(record).filter(columnName => !table.columnExists(columnName));
        if (extraColumns.length === 0) {
            return;
        }

        if (opts.autoCreateColumns) {
            for (const columnName of extraColumns) {
                const value = record[columnName];
                const type = TypeMapper.reconcileTypes([TypeMapper.inferValueType(value)]);
                table.addColumn(columnName, type, { allowNull: opts.allowNull });
            }
            return;
        }

        if (opts.ignoreExtraColumns) {
            for (const columnName of extraColumns) {
                delete record[columnName];
            }
            return;
        }

        if (opts.throwOnExtraColumns || opts.strict) {
            throw new ColumnNotFoundError(`Column "${extraColumns[0]}" does not exist in DataTable.`);
        }

        for (const columnName of extraColumns) {
            delete record[columnName];
        }
    }

    static _setRowColumnValue(row, column, value, opts) {
        if (opts.validateSchema === false) {
            row._values[column.columnName] = value;
            return;
        }

        const targetType = column.dataType || 'any';
        const converted = opts.convertTypes === false
            ? value
            : TypeMapper.convertValue(value, targetType, { strict: opts.strict });

        try {
            row.set(column.columnName, converted);
        } catch (error) {
            if (error instanceof TypeMismatchError && opts.strict !== true) {
                column.dataType = 'any';
                row._values[column.columnName] = value;
                return;
            }

            if (error instanceof TypeMismatchError) {
                throw new TypeMismatchError(
                    `Invalid value for column "${column.columnName}": expected ${targetType}, got ${describeValueType(value)}.`
                );
            }

            throw error;
        }
    }
}

module.exports = DataTableLoader;
