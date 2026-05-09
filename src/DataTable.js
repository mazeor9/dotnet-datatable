const DataColumnCollection = require('./collections/DataColumnCollection');
const DataRowCollection = require('./collections/DataRowCollection');
const DataColumn = require('./DataColumn');
const DataRow = require('./DataRow');
const DataView = require('./DataView');
const DataRowState = require('./enums/DataRowState');
const {
    DebugFormatter,
    DebugPreview,
    DebugSchemaSerializer,
    DebugTableSerializer,
    NodeInspectFormatter
} = require('./debug');
const {
    ConstraintViolationError,
    SchemaMismatchError,
    TypeMismatchError
} = require('./errors');
const { cloneValue, cloneValues, normalizeRowState } = require('./utils/typeUtils');

class DataTable {
    /**
     * @param {string} [tableName=''] - Name of the table
     */
    constructor(tableName = '') {
        this.tableName = tableName;
        this.rows = new DataRowCollection(this);
        this.columns = new DataColumnCollection(this);
        this._caseSensitive = false;
        this._uniqueConstraints = [];
        this._checkConstraints = [];
        this._inLoad = 0;
        this._dataSet = null;
        this._events = new Map();
    }

    get caseSensitive() {
        return this._caseSensitive === true;
    }

    set caseSensitive(value) {
        const next = value === true;
        const prev = this._caseSensitive === true;
        if (next === prev) {
            this._caseSensitive = next;
            return;
        }
        this._caseSensitive = next;
        if (this.columns && typeof this.columns._rebuildNameIndex === 'function') {
            this.columns._rebuildNameIndex();
        }
    }

    beginLoadData() {
        this._inLoad++;
        return this;
    }

    endLoadData() {
        this._inLoad = Math.max(0, this._inLoad - 1);
        if (this._inLoad === 0) {
            if (this.rows && typeof this.rows._rebuildIndexes === 'function') {
                this.rows._rebuildIndexes();
            }
        }
        return this;
    }

    _shouldEnforceConstraints() {
        if (this._inLoad > 0) {
            return false;
        }
        if (this._dataSet && this._dataSet.enforceConstraints === false) {
            return false;
        }
        return true;
    }

    on(eventName, handler) {
        const name = String(eventName);
        if (typeof handler !== 'function') {
            return this;
        }
        if (!this._events.has(name)) {
            this._events.set(name, new Set());
        }
        this._events.get(name).add(handler);
        return this;
    }

    off(eventName, handler) {
        const name = String(eventName);
        const set = this._events.get(name);
        if (!set) {
            return this;
        }
        set.delete(handler);
        if (set.size === 0) {
            this._events.delete(name);
        }
        return this;
    }

    _emit(eventName, payload) {
        if (this._inLoad > 0) {
            return;
        }
        const name = String(eventName);
        const set = this._events.get(name);
        if (!set || set.size === 0) {
            return;
        }
        for (const handler of [...set]) {
            handler(payload);
        }
    }

    addUniqueConstraint(columns, name = undefined) {
        const cols = Array.isArray(columns) ? columns : [columns];
        if (cols.length === 0) {
            throw new SchemaMismatchError('UniqueConstraint requires at least one column.');
        }
        const resolved = cols.map((col) => this.columns.get(col).columnName);
        const constraintName = name || `UQ_${this.tableName}_${resolved.join('_')}`;
        if (this._uniqueConstraints.some((c) => c.name === constraintName)) {
            throw new SchemaMismatchError(`UniqueConstraint '${constraintName}' already exists.`);
        }
        const constraint = { name: constraintName, columns: resolved };
        this._uniqueConstraints.push(constraint);
        if (this.rows && typeof this.rows._rebuildIndexes === 'function') {
            this.rows._rebuildIndexes();
        }
        return constraint;
    }

    getUniqueConstraints() {
        return [...this._uniqueConstraints];
    }

    addCheckConstraint(predicate, name = undefined) {
        if (typeof predicate !== 'function') {
            throw new SchemaMismatchError('CheckConstraint predicate must be a function.');
        }
        const constraintName = name || `CK_${this.tableName}_${this._checkConstraints.length + 1}`;
        if (this._checkConstraints.some((c) => c.name === constraintName)) {
            throw new SchemaMismatchError(`CheckConstraint '${constraintName}' already exists.`);
        }
        const constraint = { name: constraintName, predicate };
        this._checkConstraints.push(constraint);
        if (this.rows && typeof this.rows._rebuildIndexes === 'function') {
            this.rows._rebuildIndexes();
        }
        return constraint;
    }

    getCheckConstraints() {
        return [...this._checkConstraints];
    }

    _evaluateCheckConstraints(row, values) {
        const constraints = this._checkConstraints || [];
        if (constraints.length === 0) {
            return;
        }
        const proxy = createRowProxyForValues(this, row, values);
        for (const constraint of constraints) {
            const ok = Boolean(constraint.predicate(proxy, row, this));
            if (!ok) {
                throw new ConstraintViolationError(
                    `Constraint violation: check constraint '${constraint.name}' failed`
                );
            }
        }
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

    static fromObjects(objects, options = {}) {
        const table = new DataTable(options.tableName || options.name || '');
        const DataTableLoader = require('./mapping/DataTableLoader');
        table.beginLoadData();
        try {
            DataTableLoader.load(table, objects, {
                ...options,
                clearBeforeLoad: false,
                rowState: options.rowState || DataRowState.UNCHANGED,
                preserveOriginalValues: options.preserveOriginalValues !== false
            });
        } finally {
            table.endLoadData();
        }
        return table;
    }

    static fromRows(rows, options = {}) {
        return DataTable.fromObjects(rows, options);
    }

    static fromRecords(records, options = {}) {
        return DataTable.fromObjects(records, options);
    }

    static fromQueryResult(queryResult, options = {}) {
        const QueryResultMapper = require('./mapping/QueryResultMapper');
        const mapped = QueryResultMapper.map(queryResult, options);
        return DataTable.fromRows(mapped.rows, {
            ...options,
            provider: options.provider || mapped.provider,
            columnMetadata: options.columnMetadata || mapped.fields,
            rowState: options.rowState || DataRowState.UNCHANGED
        });
    }

    /**
     * @param {number} index - Index of the row to remove
     */
    removeRow(index) {
        this.rows.removeAt(index);
    }

    deleteRow(index) {
        const row = this.rows[index];
        if (row) {
            row.delete();
        }
    }

    /**
     * @param {Function} filterExpression - Filter function to select rows
     * @returns {Array} Array of filtered rows
     */
    select(filterExpression) {
        if (filterExpression === null || filterExpression === undefined || typeof filterExpression === 'string') {
            return this.selectRows(filterExpression, arguments[1]);
        }
        return this.rows._rows.filter(row => filterExpression(row._values)).map(row => row._values);
    }

    selectRows(filterExpression = null, sortExpression = null, rowStateFilter = null) {
        const view = this.createView({
            filter: filterExpression || undefined,
            sort: sortExpression || undefined
        });
        let rows = view.getRows();
        if (rowStateFilter) {
            const normalized = normalizeRowState(rowStateFilter, null);
            rows = rows.filter((row) => row.getRowState() === normalized);
        }
        return rows;
    }

    compute(aggregateExpression, filterExpression = null) {
        const parsed = parseAggregateExpression(aggregateExpression);
        const rows = this.selectRows(filterExpression, null)
            .filter((row) => row.getRowState() !== DataRowState.DELETED);

        if (parsed.fn === 'COUNT' && parsed.arg === '*') {
            return rows.length;
        }

        const values = rows
            .map((row) => row.get(parsed.arg))
            .filter((value) => value !== null && value !== undefined);

        if (parsed.fn === 'COUNT') {
            return values.length;
        }
        if (values.length === 0) {
            return null;
        }

        if (parsed.fn === 'MIN') {
            return values.reduce((min, v) => compareScalars(v, min) < 0 ? v : min, values[0]);
        }
        if (parsed.fn === 'MAX') {
            return values.reduce((max, v) => compareScalars(v, max) > 0 ? v : max, values[0]);
        }
        if (parsed.fn === 'SUM') {
            return values.reduce((sum, v) => sum + Number(v), 0);
        }
        if (parsed.fn === 'AVG') {
            return values.reduce((sum, v) => sum + Number(v), 0) / values.length;
        }

        throw new SchemaMismatchError(`Unsupported aggregate function '${parsed.fn}'`);
    }

    join(otherTable, options = {}) {
        if (!(otherTable instanceof DataTable)) {
            throw new SchemaMismatchError('join() requires a DataTable.');
        }
        const type = String(options.type || 'inner').toLowerCase();
        const leftKey = options.leftKey || options.on;
        const rightKey = options.rightKey || options.on;
        if (!leftKey || !rightKey) {
            throw new SchemaMismatchError('join() requires leftKey/rightKey (or on).');
        }
        const leftSelector = typeof leftKey === 'function' ? leftKey : (row) => row.get(leftKey);
        const rightSelector = typeof rightKey === 'function' ? rightKey : (row) => row.get(rightKey);
        const select = typeof options.select === 'function'
            ? options.select
            : (l, r) => ({ ...l.toObject(), ...(r ? r.toObject() : {}) });

        const index = new Map();
        for (const r of otherTable.rows._rows) {
            if (r.getRowState() === DataRowState.DELETED) continue;
            const key = serializeJoinKey(rightSelector(r));
            if (!index.has(key)) index.set(key, []);
            index.get(key).push(r);
        }

        const output = [];
        for (const l of this.rows._rows) {
            if (l.getRowState() === DataRowState.DELETED) continue;
            const key = serializeJoinKey(leftSelector(l));
            const matches = index.get(key) || [];
            if (matches.length === 0) {
                if (type === 'left') {
                    output.push(select(l, null));
                }
                continue;
            }
            for (const r of matches) {
                output.push(select(l, r));
            }
        }

        return DataTable.fromObjects(output, {
            tableName: options.tableName || `${this.tableName}_join_${otherTable.tableName}`,
            primaryKey: options.primaryKey,
            caseSensitive: options.caseSensitive ?? this.caseSensitive
        });
    }

    groupBy(keys, aggregations = {}) {
        const keyCols = Array.isArray(keys) ? keys : [keys];
        const groups = new Map();

        for (const row of this.rows._rows) {
            if (row.getRowState() === DataRowState.DELETED) continue;
            const keyValues = keyCols.map((k) => (typeof k === 'function' ? k(row) : row.get(k)));
            const key = JSON.stringify(keyValues.map(serializeJoinKey));
            if (!groups.has(key)) {
                const base = {};
                for (let i = 0; i < keyCols.length; i++) {
                    const name = typeof keyCols[i] === 'string' ? keyCols[i] : `key${i + 1}`;
                    base[name] = keyValues[i];
                }
                groups.set(key, { base, rows: [] });
            }
            groups.get(key).rows.push(row);
        }

        const result = [];
        for (const group of groups.values()) {
            const out = { ...group.base };
            for (const [outName, def] of Object.entries(aggregations)) {
                const fn = String(def.fn || def.function || def.aggregate || 'count').toUpperCase();
                const col = def.column || def.arg || '*';
                if (fn === 'COUNT' && col === '*') {
                    out[outName] = group.rows.length;
                    continue;
                }
                const vals = group.rows.map((r) => r.get(col)).filter((v) => v !== null && v !== undefined);
                if (fn === 'COUNT') out[outName] = vals.length;
                else if (fn === 'SUM') out[outName] = vals.reduce((s, v) => s + Number(v), 0);
                else if (fn === 'AVG') out[outName] = vals.length ? vals.reduce((s, v) => s + Number(v), 0) / vals.length : null;
                else if (fn === 'MIN') out[outName] = vals.length ? vals.reduce((m, v) => compareScalars(v, m) < 0 ? v : m, vals[0]) : null;
                else if (fn === 'MAX') out[outName] = vals.length ? vals.reduce((m, v) => compareScalars(v, m) > 0 ? v : m, vals[0]) : null;
                else throw new SchemaMismatchError(`Unsupported aggregate '${fn}' in groupBy()`);
            }
            result.push(out);
        }
        return result;
    }

    distinct(columns) {
        const cols = Array.isArray(columns) ? columns : [columns];
        const seen = new Set();
        const output = [];
        for (const row of this.rows._rows) {
            if (row.getRowState() === DataRowState.DELETED) continue;
            const keyValues = cols.map((c) => row.get(c));
            const key = JSON.stringify(keyValues.map(serializeJoinKey));
            if (seen.has(key)) continue;
            seen.add(key);
            output.push(row.toObject());
        }
        return DataTable.fromObjects(output, {
            tableName: `${this.tableName}_distinct`,
            primaryKey: this.getPrimaryKey(),
            caseSensitive: this.caseSensitive
        });
    }

    union(otherTable, options = {}) {
        if (!(otherTable instanceof DataTable)) {
            throw new SchemaMismatchError('union() requires a DataTable.');
        }
        const columns = this.columns.toArray().map((c) => c.columnName);
        for (const col of columns) {
            if (!otherTable.columnExists(col)) {
                throw new SchemaMismatchError(`union() schema mismatch: missing column '${col}'`);
            }
        }
        const all = [];
        for (const row of this.rows._rows) {
            if (row.getRowState() === DataRowState.DELETED) continue;
            all.push(row.toObject());
        }
        for (const row of otherTable.rows._rows) {
            if (row.getRowState() === DataRowState.DELETED) continue;
            all.push(row.toObject());
        }
        return DataTable.fromObjects(all, {
            tableName: options.tableName || `${this.tableName}_union_${otherTable.tableName}`,
            primaryKey: options.primaryKey || this.getPrimaryKey(),
            caseSensitive: options.caseSensitive ?? this.caseSensitive
        });
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
                expression: col.expression,
                maxLength: col.maxLength,
                sourceColumn: col.sourceColumn,
                metadata: col.metadata
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
                if (col.isComputed) {
                    continue;
                }
                newRow.set(col.columnName, row.get(col.columnName));
            }
            newRow._originalValues = cloneValues(row._originalValues);
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
            return this;
        }

        return this.loadRows(queryResults, {
            clearBeforeLoad: true,
            inferSchema: true,
            autoCreateColumns: true,
            rowState: DataRowState.UNCHANGED,
            preserveOriginalValues: true
        });
    }

    /**
     * Loads the results of an asynchronous query into the DataTable
     * @param {Promise<Array<Object>>} queryPromise - Promise that resolves with the query results
     */
    async loadFromQueryAsync(queryPromise) {
        const results = await queryPromise;
        return this.loadFromQuery(results);
    }

    loadRows(rows, options = {}) {
        const DataTableLoader = require('./mapping/DataTableLoader');
        this.beginLoadData();
        try {
            return DataTableLoader.load(this, rows, options);
        } finally {
            this.endLoadData();
        }
    }

    mergeRows(rows, options = {}) {
        const DataTableLoader = require('./mapping/DataTableLoader');
        const primaryKey = normalizePrimaryKey(options.primaryKey || this.getPrimaryKey());
        if (primaryKey.length === 0) {
            throw new SchemaMismatchError('mergeRows() requires a primary key.');
        }

        for (const columnName of primaryKey) {
            if (!this.columnExists(columnName)) {
                throw new SchemaMismatchError(`Missing primary key column: "${columnName}"`);
            }
        }

        const opts = {
            updateExisting: options.updateExisting !== false,
            addMissing: options.addMissing !== false,
            markModified: options.markModified === true,
            autoCreateColumns: options.autoCreateColumns === true,
            strict: options.strict === true,
            convertTypes: options.convertTypes !== false,
            validateSchema: options.validateSchema !== false,
            ...options
        };
        const normalizedRows = DataTableLoader.normalizeRows(rows, {
            ...opts,
            columnNameResolver: createMergeRowColumnNameResolver(this, opts.columnNameResolver)
        });
        const result = {
            updatedRows: 0,
            insertedRows: 0,
            skippedRows: 0
        };

        if (opts.autoCreateColumns) {
            this.loadRows([], {
                ...opts,
                columns: null,
                clearBeforeLoad: false
            });
        }

        for (const record of normalizedRows) {
            for (const columnName of primaryKey) {
                if (!Object.prototype.hasOwnProperty.call(record, columnName)) {
                    throw new SchemaMismatchError(`Missing primary key column: "${columnName}"`);
                }
            }

            const key = primaryKey.length === 1
                ? record[primaryKey[0]]
                : primaryKey.map(columnName => record[columnName]);
            let existing = this.find(key);

            if (existing) {
                if (!opts.updateExisting) {
                    result.skippedRows++;
                    continue;
                }

                const before = cloneValues(existing._values);
                for (const [columnName, value] of Object.entries(record)) {
                    if (primaryKey.includes(columnName)) {
                        continue;
                    }
                    if (!this.columnExists(columnName)) {
                        if (opts.autoCreateColumns) {
                            this.addColumn(columnName, 'any');
                        } else if (opts.ignoreExtraColumns) {
                            continue;
                        } else {
                            throw new SchemaMismatchError(`Column "${columnName}" does not exist in DataTable.`);
                        }
                    }
                    existing.set(columnName, value);
                }

                if (opts.markModified) {
                    if (existing.getRowState() === DataRowState.UNCHANGED) {
                        existing._originalValues = before;
                        existing._setRowState(DataRowState.MODIFIED);
                    }
                } else {
                    existing._originalValues = cloneValues(existing._values);
                    existing._setRowState(DataRowState.UNCHANGED);
                }
                result.updatedRows++;
                continue;
            }

            if (!opts.addMissing) {
                result.skippedRows++;
                continue;
            }

            DataTableLoader.addRecord(this, record, {
                ...opts,
                recordAlreadyNormalized: true,
                rowState: DataRowState.UNCHANGED,
                preserveOriginalValues: true
            });
            result.insertedRows++;
        }

        return result;
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
                expression: typeof column.expression === 'string' ? column.expression : null,
                readOnly: column.readOnly,
                unique: column.unique,
                ordinal: column.ordinal,
                caption: column.caption,
                isPrimaryKey: column.isPrimaryKey,
                maxLength: column.maxLength,
                sourceColumn: column.sourceColumn,
                metadata: column.metadata
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

        for (const constraint of this._uniqueConstraints || []) {
            if (!constraint || !Array.isArray(constraint.columns) || constraint.columns.length < 2) {
                continue;
            }
            schema.uniqueConstraints.push({
                columns: [...constraint.columns],
                name: constraint.name
            });
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
            const columnName = columnDef.name || columnDef.columnName;
            const column = table.addColumn(columnName, columnDef.dataType || columnDef.type, {
                allowNull: columnDef.allowNull !== undefined ? columnDef.allowNull : true,
                defaultValue: columnDef.defaultValue,
                expression: columnDef.expression,
                readOnly: columnDef.readOnly || false,
                unique: columnDef.unique || false,
                caption: columnDef.caption || columnName,
                maxLength: columnDef.maxLength,
                sourceColumn: columnDef.sourceColumn,
                metadata: columnDef.metadata
            });
            column.caption = columnDef.caption || columnName;
        }
        if (schema.primaryKey && schema.primaryKey.length > 0) {
            table.setPrimaryKey(schema.primaryKey);
        }

        if (Array.isArray(schema.uniqueConstraints)) {
            for (const constraint of schema.uniqueConstraints) {
                const cols = constraint && Array.isArray(constraint.columns) ? constraint.columns : [];
                if (cols.length > 1) {
                    table.addUniqueConstraint(cols, constraint.name);
                }
            }
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
            caption: sourceColumn.caption,
            maxLength: sourceColumn.maxLength,
            sourceColumn: sourceColumn.sourceColumn,
            metadata: sourceColumn.metadata
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
            case 'integer': {
                const num = Number(value);
                if (Number.isNaN(num) || !Number.isInteger(num)) {
                    throw new TypeMismatchError(
                        `Type mismatch for column '${column.columnName}': expected integer`
                    );
                }
                return num;
            }
            case 'bigint':
                try {
                    return typeof value === 'bigint' ? value : BigInt(value);
                } catch (_) {
                    throw new TypeMismatchError(
                        `Type mismatch for column '${column.columnName}': expected bigint`
                    );
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
                if (typeof value === 'boolean') return value;
                if (typeof value === 'number') return value !== 0;
                if (typeof value === 'string') {
                    const lower = value.trim().toLowerCase();
                    if (['true', '1', 'yes', 'y'].includes(lower)) return true;
                    if (['false', '0', 'no', 'n'].includes(lower)) return false;
                }
                throw new TypeMismatchError(
                    `Type mismatch for column '${column.columnName}': expected boolean`
                );
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

    createView(options = {}) {
        const view = new DataView(this);
        if (options && options.filter !== undefined) {
            view.filter(options.filter);
        }
        if (options && options.sort !== undefined) {
            view.sort(options.sort);
        }
        return view;
    }

    get defaultView() {
        return this.createView();
    }

    toObjects(options = {}) {
        const {
            includeDeleted = false,
            includeRowState = false,
            includeOriginalValues = false,
            onlyChanged = false,
            columnNameMapping = null,
            dateMode = 'date',
            bigIntMode = 'bigint'
        } = options;

        const rows = [];
        for (const row of this.rows._rows) {
            const state = row.getRowState();
            if (!includeDeleted && state === DataRowState.DELETED) {
                continue;
            }
            if (onlyChanged && !row.hasChanges()) {
                continue;
            }

            const output = {};
            for (const column of this.columns) {
                const targetName = mapOutputColumnName(column.columnName, columnNameMapping);
                output[targetName] = serializeOutputValue(row.get(column.columnName), dateMode, bigIntMode);
            }

            if (includeRowState) {
                output.rowState = state;
            }
            if (includeOriginalValues) {
                output.originalValues = {};
                for (const column of this.columns) {
                    const targetName = mapOutputColumnName(column.columnName, columnNameMapping);
                    output.originalValues[targetName] = serializeOutputValue(row._originalValues[column.columnName], dateMode, bigIntMode);
                }
            }
            rows.push(output);
        }
        return rows;
    }

    toArray(options = {}) {
        return this.toObjects(options);
    }

    getSchema() {
        return DebugSchemaSerializer.getTableSchema(this);
    }

    getPreview(maxRows = DebugPreview.DEFAULT_MAX_ROWS) {
        return DebugPreview.getTablePreview(this, maxRows);
    }

    toConsoleTable() {
        return DebugTableSerializer.tableToArray(this, { serializeValues: true });
    }

    toDebugView(options = {}) {
        return DebugTableSerializer.tableToDebugView(this, options);
    }

    toDebugString(options = {}) {
        return DebugFormatter.formatDataTable(this, options);
    }

    toJSON() {
        return {
            tableName: this.tableName,
            columns: this.columns.toArray().map(column => ({
                name: column.columnName,
                dataType: column.dataType,
                allowNull: column.allowNull,
                primaryKey: column.isPrimaryKey,
                unique: column.unique,
                readOnly: column.readOnly,
                maxLength: column.maxLength,
                sourceColumn: column.sourceColumn
            })),
            rows: this.toObjects({ dateMode: 'iso-string', bigIntMode: 'string' })
        };
    }

    serialize(options = {}) {
        const payload = {
            schema: this.exportSchema(),
            rows: this.rows._rows.map((row) => ({
                rowState: row.getRowState(),
                values: cloneValues(row._values),
                originalValues: cloneValues(row._originalValues),
                proposedValues: row._proposedValues ? cloneValues(row._proposedValues) : null
            }))
        };
        return options.asObject === true ? payload : JSON.stringify(payload);
    }

    static deserialize(input) {
        const payload = typeof input === 'string' ? JSON.parse(input) : input;
        if (!payload || typeof payload !== 'object' || !payload.schema) {
            throw new SchemaMismatchError('Invalid DataTable serialized payload.');
        }
        const table = DataTable.importSchema(payload.schema);
        table.beginLoadData();
        try {
            const rows = Array.isArray(payload.rows) ? payload.rows : [];
            for (const item of rows) {
                const row = table.newRow();
                row._values = cloneValues(item.values || {});
                row._originalValues = cloneValues(item.originalValues || item.values || {});
                row._proposedValues = item.proposedValues ? cloneValues(item.proposedValues) : null;
                row._inEdit = Boolean(row._proposedValues);
                row._table = table;
                row._rowState = item.rowState || DataRowState.UNCHANGED;

                table.rows._rows.push(row);
                if (row._rowState !== DataRowState.DELETED && row._rowState !== DataRowState.DETACHED) {
                    table.rows._indexRow(row);
                }
            }
        } finally {
            table.endLoadData();
        }
        return table;
    }

    [NodeInspectFormatter.customInspectSymbol](depth, options, inspect) {
        return NodeInspectFormatter.inspectDataTable(this, depth, options, inspect);
    }

    // ===== ROWSTATE MANAGEMENT METHODS =====

    /**
     * Accepts changes for all rows in the table
     */
    acceptAllChanges() {
        const snapshot = [...this.rows._rows];
        for (const row of snapshot) {
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
    getChanges(rowState = null) {
        if (!rowState) {
            return this.rows._rows.filter(row => row.hasChanges());
        }
        const normalized = normalizeRowState(rowState, null);
        return this.rows._rows.filter(row => row.getRowState() === normalized);
    }

    getChangeSet(options = {}) {
        const { DataTableChangeSet } = require('./changeTracking');
        return DataTableChangeSet.fromTable(this, options);
    }

    getCommands(options = {}) {
        const changeSet = this.getChangeSet(options);
        return {
            tableName: this.tableName,
            primaryKey: [...changeSet.primaryKey],
            inserts: changeSet.added.map((c) => ({
                tableName: c.tableName,
                key: c.key,
                values: c.values
            })),
            updates: changeSet.modified.map((c) => ({
                tableName: c.tableName,
                key: c.key,
                originalKey: c.originalKey,
                values: c.values,
                originalValues: c.originalValues,
                changedColumns: c.changedColumns
            })),
            deletes: changeSet.deleted.map((c) => ({
                tableName: c.tableName,
                key: c.key,
                originalValues: c.originalValues
            }))
        };
    }

    applyChangeSet(changeSet, options = {}) {
        const opts = normalizeApplyChangeSetOptions(options);
        const normalized = normalizeDataTableChangeSet(changeSet);
        const tablePk = this.getPrimaryKey();

        if (opts.strict === true) {
            if (normalized.tableName && this.tableName && normalized.tableName !== this.tableName) {
                throw new SchemaMismatchError(
                    `applyChangeSet() tableName mismatch: '${normalized.tableName}' -> '${this.tableName}'`
                );
            }
            if (Array.isArray(normalized.primaryKey) && normalized.primaryKey.length > 0 && tablePk.length > 0) {
                if (!areStringArraysEqual(normalized.primaryKey, tablePk)) {
                    throw new SchemaMismatchError(
                        `applyChangeSet() primaryKey mismatch: '${normalized.primaryKey.join(",")}' -> '${tablePk.join(",")}'`
                    );
                }
            }
        }

        if (tablePk.length === 0) {
            throw new SchemaMismatchError('applyChangeSet() requires a primary key on the target DataTable.');
        }

        const summary = {
            tableName: this.tableName,
            appliedAdded: 0,
            appliedModified: 0,
            appliedDeleted: 0,
            skipped: 0
        };

        for (const change of normalized.added) {
            const outcome = applyRowChange(this, change, tablePk, opts);
            if (outcome === 'applied') summary.appliedAdded++;
            else summary.skipped++;
        }
        for (const change of normalized.modified) {
            const outcome = applyRowChange(this, change, tablePk, opts);
            if (outcome === 'applied') summary.appliedModified++;
            else summary.skipped++;
        }
        for (const change of normalized.deleted) {
            const outcome = applyRowChange(this, change, tablePk, opts);
            if (outcome === 'applied') summary.appliedDeleted++;
            else summary.skipped++;
        }

        return summary;
    }

    /**
     * Gets rows by their state
     * @param {string} state - Row state to filter by
     * @returns {Array<DataRow>} Array of rows in the specified state
     */
    getRowsByState(state) {
        const normalized = normalizeRowState(state, state);
        return this.rows._rows.filter(row => row.getRowState() === normalized);
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
                row._originalValues = cloneValues(row._values);
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

    findByPrimaryKey(key) {
        return this.find(key);
    }

}

function createMergeRowColumnNameResolver(table, baseResolver) {
    const resolver = typeof baseResolver === 'function' ? baseResolver : (name) => String(name);
    if (!table || table.caseSensitive === true) {
        return resolver;
    }
    const known = new Map();
    return function resolveColumnName(sourceName) {
        const resolved = resolver(sourceName);
        if (table.columns && typeof table.columns.resolveName === 'function') {
            const canonical = table.columns.resolveName(resolved) || table.columns.resolveName(sourceName);
            if (canonical) {
                return canonical;
            }
        }
        const lower = String(resolved).toLowerCase();
        if (known.has(lower)) {
            return known.get(lower);
        }
        known.set(lower, String(resolved));
        return String(resolved);
    };
}

function normalizePrimaryKey(primaryKey) {
    if (!primaryKey) {
        return [];
    }
    return Array.isArray(primaryKey) ? primaryKey : [primaryKey];
}

function normalizeApplyChangeSetOptions(options) {
    const opts = options || {};
    const missingRowAction = String(opts.missingRowAction || 'ignore').toLowerCase();
    const conflictPolicy = String(opts.conflictPolicy || 'overwrite').toLowerCase();
    const allowedMissing = ['ignore', 'add', 'error'];
    const allowedConflict = ['overwrite', 'preserve', 'error'];

    if (!allowedMissing.includes(missingRowAction)) {
        throw new SchemaMismatchError(
            `Invalid missingRowAction '${opts.missingRowAction}'. Expected: ${allowedMissing.join(', ')}`
        );
    }
    if (!allowedConflict.includes(conflictPolicy)) {
        throw new SchemaMismatchError(
            `Invalid conflictPolicy '${opts.conflictPolicy}'. Expected: ${allowedConflict.join(', ')}`
        );
    }

    return {
        missingRowAction,
        conflictPolicy,
        strict: opts.strict === true
    };
}

function normalizeDataTableChangeSet(changeSet) {
    const raw = changeSet && typeof changeSet.toObject === 'function'
        ? changeSet.toObject()
        : changeSet;
    if (!raw || typeof raw !== 'object') {
        throw new SchemaMismatchError('Invalid changeSet for applyChangeSet().');
    }
    return {
        tableName: raw.tableName || '',
        primaryKey: Array.isArray(raw.primaryKey) ? raw.primaryKey.map(String) : [],
        added: Array.isArray(raw.added) ? raw.added : [],
        modified: Array.isArray(raw.modified) ? raw.modified : [],
        deleted: Array.isArray(raw.deleted) ? raw.deleted : []
    };
}

function areStringArraysEqual(a, b) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
        if (String(a[i]) !== String(b[i])) return false;
    }
    return true;
}

function applyRowChange(table, change, primaryKey, options) {
    const state = String(change?.state || '').toUpperCase();
    if (![DataRowState.ADDED, DataRowState.MODIFIED, DataRowState.DELETED].includes(state)) {
        if (options.strict === true) {
            throw new SchemaMismatchError(`Invalid change state '${change?.state}'`);
        }
        return 'skipped';
    }

    const key = change && change.key && typeof change.key === 'object' ? change.key : null;
    const originalKey = change && change.originalKey && typeof change.originalKey === 'object' ? change.originalKey : null;
    const values = change && change.values && typeof change.values === 'object' ? change.values : {};
    const originalValues = change && change.originalValues && typeof change.originalValues === 'object' ? change.originalValues : null;

    let row = null;
    if (key) {
        row = findByKeyObject(table, primaryKey, key);
    }
    if (!row && originalKey) {
        row = findByKeyObject(table, primaryKey, originalKey);
    }

    if (!row) {
        if (state === DataRowState.ADDED) {
            const inserted = table.newRow();
            applyValuesToRow(table, inserted, values, options);
            table.rows.add(inserted);
            return 'applied';
        }
        if (state === DataRowState.DELETED && options.missingRowAction === 'ignore') {
            return 'skipped';
        }
        if (options.missingRowAction === 'error') {
            throw new SchemaMismatchError(`Missing target row for ${state} change.`);
        }
        if (options.missingRowAction !== 'add') {
            return 'skipped';
        }

        const inserted = table.newRow();
        applyValuesToRow(table, inserted, values, options);
        table.rows.add(inserted);

        if (state === DataRowState.DELETED) {
            inserted.delete();
            return 'applied';
        }

        if (state === DataRowState.MODIFIED) {
            if (originalValues) {
                inserted._originalValues = cloneValues(originalValues);
            } else {
                inserted._originalValues = cloneValues(inserted._values);
            }
            inserted._setRowState(DataRowState.MODIFIED);
            return 'applied';
        }

        return 'applied';
    }

    if (options.conflictPolicy === 'preserve' && row.hasChanges()) {
        return 'skipped';
    }
    if (options.conflictPolicy === 'error' && row.hasChanges()) {
        throw new SchemaMismatchError('Row has local changes.');
    }

    if (state === DataRowState.DELETED) {
        row.delete();
        return 'applied';
    }

    const beforeState = row.getRowState();
    applyValuesToRow(table, row, values, options);

    if (state === DataRowState.MODIFIED) {
        if (originalValues && beforeState === DataRowState.UNCHANGED) {
            row._originalValues = cloneValues(originalValues);
            row._setRowState(DataRowState.MODIFIED);
        }
    }

    return 'applied';
}

function findByKeyObject(table, primaryKey, keyObject) {
    const keyValues = primaryKey.map((name) => keyObject[name]);
    if (keyValues.some((v) => v === null || v === undefined)) {
        return null;
    }
    const key = primaryKey.length === 1 ? keyValues[0] : keyValues;
    return table.find(key);
}

function applyValuesToRow(table, row, values, options) {
    for (const [sourceName, value] of Object.entries(values)) {
        if (!table.columnExists(sourceName)) {
            if (options.strict === true) {
                throw new SchemaMismatchError(`Column '${sourceName}' does not exist in target DataTable.`);
            }
            continue;
        }
        const column = table.columns.get(sourceName);
        if (column.isComputed) {
            continue;
        }
        row.set(column.columnName, value);
    }
}

function parseAggregateExpression(expression) {
    const match = String(expression || '').trim().match(/^([A-Za-z_][\w]*)\s*\(\s*(\*|[A-Za-z_][\w.]*)\s*\)$/);
    if (!match) {
        throw new SchemaMismatchError(`Invalid aggregate expression '${expression}'`);
    }
    return {
        fn: match[1].toUpperCase(),
        arg: match[2]
    };
}

function compareScalars(a, b) {
    if (a === b) return 0;
    if (a === null || a === undefined) return 1;
    if (b === null || b === undefined) return -1;
    if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime();
    if (typeof a === 'number' && typeof b === 'number') return a - b;
    return String(a).localeCompare(String(b));
}

function serializeJoinKey(value) {
    if (value === null || value === undefined) return null;
    if (value instanceof Date) return `d:${value.getTime()}`;
    if (typeof value === 'bigint') return `bi:${value.toString()}`;
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) return `buf:${value.toString('base64')}`;
    if (typeof value === 'number') return `n:${Object.is(value, -0) ? '-0' : String(value)}`;
    if (typeof value === 'boolean') return `b:${value ? '1' : '0'}`;
    if (typeof value === 'string') return `s:${value}`;
    try {
        return `j:${JSON.stringify(value)}`;
    } catch (_) {
        return `u:${String(value)}`;
    }
}

function createRowProxyForValues(table, row, values) {
    const resolvedValues = values && typeof values === 'object' ? values : {};
    return new Proxy(row, {
        get(target, prop) {
            if (prop in target) {
                return target[prop];
            }
            if (typeof prop === 'string' && table && table.columnExists(prop)) {
                const canonical = table.columns.get(prop).columnName;
                if (Object.prototype.hasOwnProperty.call(resolvedValues, canonical)) {
                    return resolvedValues[canonical];
                }
                if (Object.prototype.hasOwnProperty.call(resolvedValues, prop)) {
                    return resolvedValues[prop];
                }
                return target.get(canonical, 'current');
            }
            return undefined;
        }
    });
}

function mapOutputColumnName(columnName, mapping) {
    if (typeof mapping === 'function') {
        return mapping(columnName);
    }
    if (mapping && Object.prototype.hasOwnProperty.call(mapping, columnName)) {
        return mapping[columnName];
    }
    return columnName;
}

function serializeOutputValue(value, dateMode, bigIntMode = 'bigint') {
    if (value instanceof Date) {
        return dateMode === 'iso-string' ? value.toISOString() : cloneValue(value);
    }
    if (typeof value === 'bigint') {
        return bigIntMode === 'string' ? value.toString() : value;
    }
    return cloneValue(value);
}

module.exports = DataTable;
