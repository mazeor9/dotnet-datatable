const DataColumnCollection = require('./collections/DataColumnCollection');
const DataRowCollection = require('./collections/DataRowCollection');
const DataColumn = require('./DataColumn');
const DataRow = require('./DataRow');
const DataRowState = require('./enums/DataRowState');
const {
    ConstraintViolationError,
    SchemaMismatchError,
    TypeMismatchError
} = require('./errors');

class DataTable {
    /**
     * @param {string} [tableName=''] - Name of the table
     */
    constructor(tableName = '') {
        this.tableName = tableName;
        this.rows = new DataRowCollection(this);
        this.columns = new DataColumnCollection(this);
        this.caseSensitive = false;
    }

    /**
     * @param {string} columnName - Name of the column to add
     * @param {string|null} [dataType=null] - Data type of the column
     * @param {Object} [options] - Column options (allowNull, unique, readOnly, defaultValue, primaryKey, caption, expression)
     * @returns {DataColumn} The created column
     */
    addColumn(columnName, dataType = null, options = undefined) {
        const column = this.columns.add(columnName, dataType, options);
        if (options && typeof options === 'object') {
            if (options.caption !== undefined) column.caption = options.caption;
            if (options.expression !== undefined) column.expression = options.expression;
        }
        return column;
    }

    /**
     * @param {string} columnName - Name of the column to remove
     */
    removeColumn(columnName) {
        this.columns.remove(columnName);
    }

    /**
     * @param {string} columnName - Name of the column to check
     * @returns {boolean} True if the column exists, false otherwise
     */
    columnExists(columnName) {
        return this.columns.contains(columnName);
    }

    newRow() {
        return new DataRow(this, DataRowState.DETACHED);
    }

    /**
     * @param {Object} values - Values to insert in the row
     * @returns {DataRow} The added row
     */
    addRow(values) {
        return this.rows.add(values);
    }

    /**
     * @param {number} index - Index of the row to remove
     */
    removeRow(index) {
        this.rows.removeAt(index);
    }

    /**
     * @param {Function} filterExpression - Filter function to select rows
     * @returns {Array} Array of filtered rows
     */
    select(filterExpression) {
        return this.rows._rows.filter(row => filterExpression(row._values)).map(row => row._values);
    }

    /**
     * @param {string|Function} columnNameOrComparer - Column name or comparison function
     * @param {string} [order='asc'] - Sort order ('asc' or 'desc')
     * @returns {DataTable} The current table instance
     */
    sort(columnNameOrComparer, order = 'asc') {
        if (typeof columnNameOrComparer === 'function') {
            this.rows._rows.sort(columnNameOrComparer);
        } else {
            this.rows._rows.sort((a, b) => {
                const valueA = a.item(columnNameOrComparer);
                const valueB = b.item(columnNameOrComparer);

                // Gestione null/undefined
                if (valueA === valueB) return 0;
                if (valueA == null) return 1;
                if (valueB == null) return -1;

                // Gestione dei tipi
                const column = this.columns.get(columnNameOrComparer);
                let comparison = 0;

                switch (column.dataType?.toLowerCase()) {
                    case 'number':
                        comparison = Number(valueA) - Number(valueB);
                        break;
                    case 'date':
                        comparison = new Date(valueA) - new Date(valueB);
                        break;
                    case 'string':
                        comparison = String(valueA).localeCompare(String(valueB));
                        break;
                    default:
                        // Fallback
                        comparison = String(valueA).localeCompare(String(valueB));
                }

                return order === 'asc' ? comparison : -comparison;
            });
        }
        return this;
    }

    /**
     * @param {Function} expression - Expression function for sorting
     * @returns {DataTable} The current table instance
     */
    sortBy(expression) {
        this.rows._rows.sort((a, b) => {
            const valueA = expression(a);
            const valueB = expression(b);

            if (valueA === valueB) return 0;
            if (valueA === null) return 1;
            if (valueB === null) return -1;

            return valueA < valueB ? -1 : 1;
        });
        return this;
    }

    /**
     * @param {...Object} sortCriteria - Array of objects with column and order properties
     * @returns {DataTable} The current table instance
     */
    sortMultiple(...sortCriteria) {
        this.rows._rows.sort((a, b) => {
            for (const { column, order = 'asc' } of sortCriteria) {
                const valueA = a.item(column);
                const valueB = b.item(column);

                if (valueA === valueB) continue;
                if (valueA === null) return 1;
                if (valueB === null) return -1;

                const comparison = valueA < valueB ? -1 : 1;
                return order === 'asc' ? comparison : -comparison;
            }
            return 0;
        });
        return this;
    }

    clear() {
        this.rows.clear();
    }

    /**
     * Creates a deep copy of the DataTable including columns, rows and all properties
     * @returns {DataTable} A new instance of DataTable with the same structure and data
     */
    clone() {
        const newTable = new DataTable(this.tableName);

        // Clone columns
        const primaryKey = [];
        for (const col of this.columns) {
            const newColumn = newTable.addColumn(col.columnName, col.dataType, {
                allowNull: col.allowNull,
                unique: col.unique,
                readOnly: col.readOnly,
                defaultValue: col.defaultValue,
                expression: col.expression
            });
            newColumn.caption = col.caption;
            if (col.isPrimaryKey) {
                primaryKey.push(col.columnName);
            }
        }
        if (primaryKey.length > 0) {
            newTable.setPrimaryKey(primaryKey);
        }

        // Clone rows state
        for (const row of this.rows) {
            const newRow = newTable.newRow();
            for (const col of this.columns) {
                newRow.set(col.columnName, row.get(col.columnName));
            }
            newRow._originalValues = { ...row._originalValues };
            newRow._rowState = row._rowState;
            newTable.rows._addClonedRow(newRow);
        }

        // Clone other properties
        newTable.caseSensitive = this.caseSensitive;

        return newTable;
    }

    *[Symbol.iterator]() {
        yield* this.rows._rows;
    }

    /**
     * @param {Object|Function} criteria - Search criteria or filter function
     * @returns {Array<DataRow>} Array of rows that match the criteria
     */
    findRows(criteria) {
        if (typeof criteria === 'function') {
            return this.rows._rows.filter(row => criteria(row));
        }

        return this.rows._rows.filter(row => {
            return Object.entries(criteria).every(([key, value]) => {
                const rowValue = row._values[key];

                if (value instanceof RegExp) {
                    return value.test(String(rowValue));
                }

                if (typeof value === 'object' && value !== null) {
                    // support operators
                    if ('$gt' in value) return rowValue > value.$gt;
                    if ('$gte' in value) return rowValue >= value.$gte;
                    if ('$lt' in value) return rowValue < value.$lt;
                    if ('$lte' in value) return rowValue <= value.$lte;
                    if ('$ne' in value) return rowValue !== value.$ne;
                    if ('$in' in value) return value.$in.includes(rowValue);
                    if ('$contains' in value) return String(rowValue).includes(value.$contains);
                }

                return rowValue === value;
            });
        });
    }

    /**
     * @param {Object|Function} criteria - Search criteria or filter function
     * @returns {DataRow|null} First row that matches the criteria or null
     */
    findOne(criteria) {
        const results = this.findRows(criteria);
        return results.length > 0 ? results[0] : null;
    }

    /**
     * Loads the results of a query into the DataTable
     * @param {Array<Object>} queryResults - Array of objects resulting from the query
     */
    loadFromQuery(queryResults) {
        if (!Array.isArray(queryResults) || queryResults.length === 0) {
            return;
        }

        // Clear existing data
        this.clear();

        // Create columns from the first row
        const firstRow = queryResults[0];
        for (const columnName in firstRow) {
            const value = firstRow[columnName];
            let dataType = typeof value;
            if (value instanceof Date) dataType = 'date';
            if (value === null) dataType = 'string'; // default type for null values

            this.addColumn(columnName, dataType);
        }

        // Add all rows
        queryResults.forEach(row => {
            this.addRow(row);
        });
    }

    /**
     * Loads the results of an asynchronous query into the DataTable
     * @param {Promise<Array<Object>>} queryPromise - Promise that resolves with the query results
     */
    async loadFromQueryAsync(queryPromise) {
        const results = await queryPromise;
        this.loadFromQuery(results);
    }

    /**
 * Exports the schema definition of the table
 * @returns {Object} Schema definition object
 */
    exportSchema() {
        const schema = {
            tableName: this.tableName,
            caseSensitive: this.caseSensitive,
            columns: [],
            primaryKey: null,
            uniqueConstraints: []
        };

        // Export columns
        for (const column of this.columns) {
            schema.columns.push({
                name: column.columnName,
                dataType: column.dataType,
                allowNull: column.allowNull,
                defaultValue: column.defaultValue,
                expression: column.expression,
                readOnly: column.readOnly,
                unique: column.unique,
                ordinal: column.ordinal,
                caption: column.caption
            });

            // Add primary key info
            if (column.isPrimaryKey) {
                if (!schema.primaryKey) {
                    schema.primaryKey = [];
                }
                schema.primaryKey.push(column.columnName);
            }

            // Add unique constraints
            if (column.unique && !column.isPrimaryKey) {
                schema.uniqueConstraints.push({
                    columns: [column.columnName],
                    name: `UQ_${this.tableName}_${column.columnName}`
                });
            }
        }

        return schema;
    }

    /**
     * Creates a new DataTable from a schema definition
     * @param {Object} schema - Schema definition object
     * @returns {DataTable} A new DataTable instance configured with the schema
     */
    static importSchema(schema) {
        const table = new DataTable(schema.tableName);
        table.caseSensitive = schema.caseSensitive || false;

        // Import columns
        for (const columnDef of schema.columns) {
            const column = table.addColumn(columnDef.name, columnDef.dataType, {
                allowNull: columnDef.allowNull !== undefined ? columnDef.allowNull : true,
                defaultValue: columnDef.defaultValue,
                expression: columnDef.expression,
                readOnly: columnDef.readOnly || false,
                unique: columnDef.unique || false,
                caption: columnDef.caption || columnDef.name
            });
            column.caption = columnDef.caption || columnDef.name;
        }
        if (schema.primaryKey && schema.primaryKey.length > 0) {
            table.setPrimaryKey(schema.primaryKey);
        }

        return table;
    }

    /**
     * Compares the schema of this table with another
     * @param {DataTable} otherTable - Table to compare schema with
     * @returns {Object} Object containing differences between schemas
     */
    compareSchema(otherTable) {
        const differences = {
            missingColumns: [],
            extraColumns: [],
            typeMismatches: [],
            nullabilityDifferences: []
        };

        // Check for missing or type-mismatched columns
        for (const column of this.columns) {
            if (!otherTable.columnExists(column.columnName)) {
                differences.missingColumns.push(column.columnName);
            } else {
                const otherColumn = otherTable.columns.get(column.columnName);

                // Check for type mismatches
                if (column.dataType !== otherColumn.dataType) {
                    differences.typeMismatches.push({
                        column: column.columnName,
                        thisType: column.dataType,
                        otherType: otherColumn.dataType
                    });
                }

                // Check for nullability differences
                if (column.allowNull !== otherColumn.allowNull) {
                    differences.nullabilityDifferences.push({
                        column: column.columnName,
                        thisAllowNull: column.allowNull,
                        otherAllowNull: otherColumn.allowNull
                    });
                }
            }
        }

        // Check for extra columns in other table
        for (const column of otherTable.columns) {
            if (!this.columnExists(column.columnName)) {
                differences.extraColumns.push(column.columnName);
            }
        }

        return differences;
    }

    /**
     * Updates the schema of the table to match another table
     * @param {DataTable} sourceTable - Source table to copy schema from
     * @param {boolean} [addMissingColumns=true] - Whether to add missing columns
     * @param {boolean} [removeExtraColumns=false] - Whether to remove extra columns
     * @returns {Object} Result of the schema update operation
     */
    updateSchema(sourceTable, addMissingColumns = true, removeExtraColumns = false) {
        const result = {
            addedColumns: [],
            removedColumns: [],
            modifiedColumns: []
        };

        const differences = this.compareSchema(sourceTable);

        // Add missing columns
        if (addMissingColumns) {
            for (const columnName of differences.missingColumns) {
                const sourceColumn = sourceTable.columns.get(columnName);
                const newColumn = this.addColumn(columnName, sourceColumn.dataType, {
                    allowNull: sourceColumn.allowNull,
                    defaultValue: sourceColumn.defaultValue,
                    expression: sourceColumn.expression,
                    readOnly: sourceColumn.readOnly,
                    unique: sourceColumn.unique,
                    caption: sourceColumn.caption
                });
                newColumn.caption = sourceColumn.caption;

                result.addedColumns.push(columnName);
            }
        }

        // Remove extra columns
        if (removeExtraColumns) {
            for (const columnName of differences.extraColumns) {
                this.removeColumn(columnName);
                result.removedColumns.push(columnName);
            }
        }

        // Update column definitions
        for (const mismatch of differences.typeMismatches) {
            const column = this.columns.get(mismatch.column);
            column.dataType = sourceTable.columns.get(mismatch.column).dataType;
            result.modifiedColumns.push({
                column: mismatch.column,
                change: 'dataType',
                from: mismatch.thisType,
                to: mismatch.otherType
            });
        }

        for (const diff of differences.nullabilityDifferences) {
            const column = this.columns.get(diff.column);
            column.allowNull = sourceTable.columns.get(diff.column).allowNull;
            result.modifiedColumns.push({
                column: diff.column,
                change: 'allowNull',
                from: diff.thisAllowNull,
                to: diff.otherAllowNull
            });
        }

        return result;
    }

    /**
     * Merges rows and, optionally, schema from another DataTable into this table.
     * Existing rows are matched by primary key. Without a primary key, source rows are appended.
     * @param {DataTable} sourceTable - Source table to merge from
     * @param {Object} [options] - Merge options
     * @param {boolean} [options.preserveChanges=false] - Preserve local row/column changes
     * @param {'add'|'ignore'|'error'} [options.missingSchemaAction='error'] - How to handle source columns missing in this table
     * @returns {Object} Merge summary
     */
    merge(sourceTable, options = {}) {
        this._ensureMergeSource(sourceTable);

        const mergeOptions = this._normalizeMergeOptions(options);
        const result = {
            tableName: this.tableName,
            addedColumns: [],
            ignoredColumns: [],
            updatedRows: 0,
            insertedRows: 0,
            preservedRows: 0,
            skippedRows: 0,
            primaryKeyAdded: null
        };

        const sourceColumns = this._prepareMergeSchema(
            sourceTable,
            mergeOptions.missingSchemaAction,
            result
        );
        const primaryKey = this._prepareMergePrimaryKey(
            sourceTable,
            mergeOptions.missingSchemaAction,
            result
        );
        const mergeColumns = sourceColumns.filter((column) => this.columnExists(column.columnName));

        for (const sourceRow of sourceTable.rows) {
            if (typeof sourceRow.getRowState === 'function' && sourceRow.getRowState() === DataRowState.DELETED) {
                result.skippedRows++;
                continue;
            }

            const targetRow = primaryKey.length > 0
                ? this._findMergeTargetRow(sourceRow, primaryKey)
                : null;

            if (targetRow) {
                const outcome = this._mergeExistingRow(
                    targetRow,
                    sourceRow,
                    mergeColumns,
                    primaryKey,
                    mergeOptions.preserveChanges
                );

                if (outcome.updated) {
                    result.updatedRows++;
                } else if (outcome.preserved) {
                    result.preservedRows++;
                } else {
                    result.skippedRows++;
                }
                continue;
            }

            this.addRow(this._createMergedRowValues(sourceRow, mergeColumns));
            result.insertedRows++;
        }

        return result;
    }

    _ensureMergeSource(sourceTable) {
        if (!(sourceTable instanceof DataTable)) {
            throw new SchemaMismatchError('DataTable.merge() expects a DataTable source');
        }

        if (this.tableName && sourceTable.tableName && this.tableName !== sourceTable.tableName) {
            throw new SchemaMismatchError(
                `Cannot merge table '${sourceTable.tableName}' into '${this.tableName}'`
            );
        }
    }

    _normalizeMergeOptions(options) {
        const opts = options || {};
        const missingSchemaAction = String(opts.missingSchemaAction || 'error').toLowerCase();
        const allowedActions = ['add', 'ignore', 'error'];

        if (!allowedActions.includes(missingSchemaAction)) {
            throw new SchemaMismatchError(
                `Invalid missingSchemaAction '${opts.missingSchemaAction}'. Expected: ${allowedActions.join(', ')}`
            );
        }

        return {
            preserveChanges: opts.preserveChanges === true,
            missingSchemaAction
        };
    }

    _prepareMergeSchema(sourceTable, missingSchemaAction, result) {
        const sourceColumns = sourceTable.columns.toArray();

        for (const sourceColumn of sourceColumns) {
            if (!this.columnExists(sourceColumn.columnName)) {
                if (missingSchemaAction === 'add') {
                    this._addColumnFrom(sourceColumn);
                    result.addedColumns.push(sourceColumn.columnName);
                } else if (missingSchemaAction === 'ignore') {
                    result.ignoredColumns.push(sourceColumn.columnName);
                } else {
                    throw new SchemaMismatchError(
                        `Column '${sourceColumn.columnName}' does not exist in target table '${this.tableName}'`
                    );
                }
                continue;
            }

            const targetColumn = this.columns.get(sourceColumn.columnName);
            if (!this._areMergeTypesCompatible(targetColumn.dataType, sourceColumn.dataType)) {
                throw new SchemaMismatchError(
                    `Column '${sourceColumn.columnName}' type mismatch: target '${targetColumn.dataType}', source '${sourceColumn.dataType}'`
                );
            }
        }

        return sourceColumns;
    }

    _prepareMergePrimaryKey(sourceTable, missingSchemaAction, result) {
        let targetPrimaryKey = this.getPrimaryKey();
        const sourcePrimaryKey = sourceTable.getPrimaryKey();

        if (
            targetPrimaryKey.length > 0 &&
            sourcePrimaryKey.length > 0 &&
            !this._arePrimaryKeysEqual(targetPrimaryKey, sourcePrimaryKey)
        ) {
            throw new SchemaMismatchError(
                `Primary key mismatch: target '${targetPrimaryKey.join(',')}', source '${sourcePrimaryKey.join(',')}'`
            );
        }

        if (targetPrimaryKey.length === 0 && sourcePrimaryKey.length > 0 && missingSchemaAction === 'add') {
            const missingKeyColumns = sourcePrimaryKey.filter((columnName) => !this.columnExists(columnName));
            if (missingKeyColumns.length > 0) {
                throw new SchemaMismatchError(
                    `Cannot add primary key because columns are missing: ${missingKeyColumns.join(', ')}`
                );
            }

            this.setPrimaryKey(sourcePrimaryKey);
            targetPrimaryKey = this.getPrimaryKey();
            result.primaryKeyAdded = [...targetPrimaryKey];
        }

        if (targetPrimaryKey.length > 0) {
            const missingInSource = targetPrimaryKey.filter((columnName) => !sourceTable.columnExists(columnName));
            if (missingInSource.length > 0) {
                throw new SchemaMismatchError(
                    `Source table '${sourceTable.tableName}' is missing primary key columns: ${missingInSource.join(', ')}`
                );
            }
        }

        return targetPrimaryKey;
    }

    _addColumnFrom(sourceColumn) {
        const column = this.addColumn(sourceColumn.columnName, sourceColumn.dataType, {
            allowNull: sourceColumn.allowNull,
            defaultValue: sourceColumn.defaultValue,
            expression: sourceColumn.expression,
            readOnly: sourceColumn.readOnly,
            unique: sourceColumn.unique,
            caption: sourceColumn.caption
        });

        column.caption = sourceColumn.caption;
        return column;
    }

    _areMergeTypesCompatible(targetType, sourceType) {
        if (targetType === null || targetType === undefined || sourceType === null || sourceType === undefined) {
            return true;
        }
        return String(targetType).toLowerCase() === String(sourceType).toLowerCase();
    }

    _arePrimaryKeysEqual(targetPrimaryKey, sourcePrimaryKey) {
        return targetPrimaryKey.length === sourcePrimaryKey.length &&
            targetPrimaryKey.every((columnName, index) => columnName === sourcePrimaryKey[index]);
    }

    _findMergeTargetRow(sourceRow, primaryKey) {
        const keyValues = primaryKey.map((columnName) => {
            const value = sourceRow.get(columnName);
            if (value === null || value === undefined) {
                throw new ConstraintViolationError(
                    `Primary key '${primaryKey.join(',')}' cannot contain null values`
                );
            }
            return this._coerceMergeValue(this.columns.get(columnName), value);
        });

        return this.find(primaryKey.length === 1 ? keyValues[0] : keyValues);
    }

    _coerceMergeValue(column, value) {
        if (value === null || value === undefined || !column.dataType) {
            return value;
        }

        switch (String(column.dataType).toLowerCase()) {
            case 'number': {
                const num = Number(value);
                if (Number.isNaN(num)) {
                    throw new TypeMismatchError(
                        `Type mismatch for column '${column.columnName}': expected number`
                    );
                }
                return num;
            }
            case 'date': {
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
            case 'string':
                return String(value);
            case 'boolean':
                return typeof value === 'boolean' ? value : Boolean(value);
            default:
                return value;
        }
    }

    _mergeExistingRow(targetRow, sourceRow, mergeColumns, primaryKey, preserveChanges) {
        let updated = false;
        let preserved = false;
        const primaryKeyColumns = new Set(primaryKey);

        for (const sourceColumn of mergeColumns) {
            const columnName = sourceColumn.columnName;
            if (primaryKeyColumns.has(columnName)) {
                continue;
            }

            if (this._shouldPreserveColumn(targetRow, columnName, preserveChanges)) {
                preserved = true;
                continue;
            }

            const before = targetRow.get(columnName);
            targetRow.set(columnName, sourceRow.get(columnName));
            const after = targetRow.get(columnName);
            if (!targetRow._areEqual(before, after)) {
                updated = true;
            }
        }

        return { updated, preserved };
    }

    _shouldPreserveColumn(targetRow, columnName, preserveChanges) {
        if (!preserveChanges) {
            return false;
        }

        const state = targetRow.getRowState();
        if (state === DataRowState.ADDED || state === DataRowState.DELETED) {
            return true;
        }
        if (state !== DataRowState.MODIFIED) {
            return false;
        }
        if (!Object.prototype.hasOwnProperty.call(targetRow._originalValues || {}, columnName)) {
            return false;
        }

        return !targetRow._areEqual(targetRow.get(columnName), targetRow._originalValues[columnName]);
    }

    _createMergedRowValues(sourceRow, mergeColumns) {
        const values = {};
        for (const sourceColumn of mergeColumns) {
            values[sourceColumn.columnName] = sourceRow.get(sourceColumn.columnName);
        }
        return values;
    }

    /**
     * Serializes the table schema to JSON
     * @returns {string} JSON string of the schema
     */
    serializeSchema() {
        return JSON.stringify(this.exportSchema());
    }

    /**
     * Creates a DataTable from a serialized schema
     * @param {string} schemaJson - JSON string of the schema
     * @returns {DataTable} A new DataTable instance
     */
    static deserializeSchema(schemaJson) {
        const schema = JSON.parse(schemaJson);
        return DataTable.importSchema(schema);
    }

    // ===== ROWSTATE MANAGEMENT METHODS =====

    /**
     * Accepts changes for all rows in the table
     */
    acceptAllChanges() {
        for (const row of this.rows) {
            if (row.hasChanges()) {
                row.acceptChanges();
            }
        }
    }

    /**
     * Rejects changes for all modified rows in the table
     */
    rejectAllChanges() {
        const snapshot = [...this.rows._rows];
        for (const row of snapshot) {
            if (row.hasChanges()) {
                row.rejectChanges();
            }
        }
    }

    acceptChanges() {
        return this.acceptAllChanges();
    }

    rejectChanges() {
        return this.rejectAllChanges();
    }

    /**
     * Gets all rows that have changes
     * @returns {Array<DataRow>} Array of rows with changes
     */
    getChanges() {
        return this.rows._rows.filter(row => row.hasChanges());
    }

    /**
     * Gets rows by their state
     * @param {string} state - Row state to filter by
     * @returns {Array<DataRow>} Array of rows in the specified state
     */
    getRowsByState(state) {
        return this.rows._rows.filter(row => row.getRowState() === state);
    }

    /**
     * Checks if the table has any unsaved changes
     * @returns {boolean}
     */
    hasChanges() {
        return this.rows._rows.some(row => row.hasChanges());
    }

    /**
     * Gets a detailed summary of all changes in the table
     * @returns {Object} Summary object with counts and details
     */
    getChangesSummary() {
        const summary = {
            totalRows: this.rows._rows.length,
            addedCount: 0,
            modifiedCount: 0,
            deletedCount: 0,
            unchangedCount: 0,
            hasChanges: false,
            addedRows: [],
            modifiedRows: [],
            deletedRows: []
        };

        for (const row of this.rows._rows) {
            const state = row.getRowState();
            
            switch (state) {
                case 'ADDED':
                    summary.addedCount++;
                    summary.addedRows.push(row);
                    break;
                case 'MODIFIED':
                    summary.modifiedCount++;
                    summary.modifiedRows.push(row);
                    break;
                case 'DELETED':
                    summary.deletedCount++;
                    summary.deletedRows.push(row);
                    break;
                case 'UNCHANGED':
                    summary.unchangedCount++;
                    break;
            }
        }

        summary.hasChanges = summary.addedCount > 0 || summary.modifiedCount > 0 || summary.deletedCount > 0;
        
        return summary;
    }

    /**
     * Clears all row states without losing data (sets all rows to UNCHANGED)
     * This is useful when you want to reset tracking without accepting/rejecting changes
     */
    clearChanges() {
        for (const row of this.rows._rows) {
            if (row.getRowState() !== DataRowState.DELETED) {
                row._setRowState(DataRowState.UNCHANGED);
                // Clear original values tracking
                if (row._originalValues) {
                    row._originalValues = {};
                }
            }
        }
    }

    setPrimaryKey(columnNames) {
        this.columns.setPrimaryKey(columnNames);
    }

    getPrimaryKey() {
        return this.columns.getPrimaryKey();
    }

    find(key) {
        return this.rows.find(key);
    }

}

module.exports = DataTable;
