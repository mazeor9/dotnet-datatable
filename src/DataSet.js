const DataTable = require('./DataTable');
const DataRelation = require('./DataRelation');
const { DebugSchemaSerializer, DebugTableSerializer, NodeInspectFormatter } = require('./debug');
const { ConstraintViolationError, SchemaMismatchError } = require('./errors');

class DataSet {
    constructor(dataSetName = '') {
        this.dataSetName = dataSetName;
        this.tables = new Map();
        this.relations = [];
        this.enforceConstraints = true;
        this._foreignKeyConstraints = [];
        this._inConstraintAction = false;
    }

    static fromRecordsets(recordsets, options = {}) {
        const DataSetLoader = require('./mapping/DataSetLoader');
        return DataSetLoader.fromRecordsets(recordsets, options);
    }

    static fromQueryResult(queryResult, options = {}) {
        const DataSetLoader = require('./mapping/DataSetLoader');
        return DataSetLoader.fromQueryResult(queryResult, options);
    }

    /**
     * @param {string|DataTable} tableNameOrTable - Name of the table to create or DataTable instance to add
     * @returns {DataTable} The added table
     */
    addTable(tableNameOrTable) {
        let table;
        
        if (tableNameOrTable instanceof DataTable) {
            table = tableNameOrTable;
        } else {
            table = new DataTable(tableNameOrTable);
        }
        
        if (this.tables.has(table.tableName)) {
            throw new Error(`Table '${table.tableName}' already exists in the DataSet`);
        }
        
        table._dataSet = this;
        this.tables.set(table.tableName, table);
        return table;
    }

    /**
     * @param {string} tableName - Name of the table to remove
     */
    removeTable(tableName) {
        if (!this.tables.has(tableName)) {
            throw new Error(`Table '${tableName}' does not exist in the DataSet`);
        }
        
        // Remove any relations involving this table
        this.relations = this.relations.filter(rel => 
            rel.parentTable.tableName !== tableName && 
            rel.childTable.tableName !== tableName
        );
        
        const table = this.tables.get(tableName);
        if (table) {
            table._dataSet = null;
        }
        this.tables.delete(tableName);
    }

    /**
     * @param {string} tableName - Name of the table to retrieve
     * @returns {DataTable} The requested table
     * @throws {Error} If the table doesn't exist
     */
    table(tableName) {
        if (!this.tables.has(tableName)) {
            throw new Error(`Table '${tableName}' does not exist in the DataSet`);
        }
        
        return this.tables.get(tableName);
    }

    /**
     * @param {string} relationName - Name of the relation
     * @param {string|DataColumn} parentTableOrColumn - Parent table name or column
     * @param {string|DataColumn} childTableOrColumn - Child table name or column
     * @param {string} [parentColumnName] - Name of the parent column if parentTableOrColumn is a table name
     * @param {string} [childColumnName] - Name of the child column if childTableOrColumn is a table name
     * @returns {DataRelation} The created relation
     */
    addRelation(relationName, parentTableOrColumn, childTableOrColumn, parentColumnName, childColumnName) {
        let parentColumn, childColumn;
        
        if (typeof parentTableOrColumn === 'string' && parentColumnName) {
            const parentTable = this.table(parentTableOrColumn);
            parentColumn = parentTable.columns.get(parentColumnName);
            if (!parentColumn) {
                throw new Error(`Column '${parentColumnName}' does not exist in table '${parentTableOrColumn}'`);
            }
        } else {
            parentColumn = parentTableOrColumn;
        }
        
        if (typeof childTableOrColumn === 'string' && childColumnName) {
            const childTable = this.table(childTableOrColumn);
            childColumn = childTable.columns.get(childColumnName);
            if (!childColumn) {
                throw new Error(`Column '${childColumnName}' does not exist in table '${childTableOrColumn}'`);
            }
        } else {
            childColumn = childTableOrColumn;
        }
        
        const relation = new DataRelation(relationName, parentColumn, childColumn);
        this.relations.push(relation);
        
        return relation;
    }

    addForeignKeyConstraint(relationName, options = {}) {
        const relation = this.getRelation(relationName);
        const deleteRule = normalizeFkRule(options.deleteRule || options.onDelete || 'restrict');
        const updateRule = normalizeFkRule(options.updateRule || options.onUpdate || 'restrict');
        const name = options.name || `FK_${relation.parentTable.tableName}_${relation.childTable.tableName}_${relationName}`;

        if (this._foreignKeyConstraints.some((c) => c.name === name)) {
            throw new SchemaMismatchError(`ForeignKeyConstraint '${name}' already exists.`);
        }

        const constraint = {
            name,
            relationName,
            relation,
            deleteRule,
            updateRule
        };
        this._foreignKeyConstraints.push(constraint);
        return constraint;
    }

    getForeignKeyConstraints() {
        return [...this._foreignKeyConstraints];
    }

    _evaluateForeignKeyConstraints(row, values) {
        if (this.enforceConstraints !== true || this._inConstraintAction === true) {
            return;
        }
        const constraints = this._foreignKeyConstraints || [];
        if (constraints.length === 0) {
            return;
        }
        const table = row?._table;
        for (const constraint of constraints) {
            const rel = constraint.relation;
            if (!rel || rel.childTable !== table) {
                continue;
            }
            const childColumnName = rel.childColumn.columnName;
            const parentColumnName = rel.parentColumn.columnName;
            const fkValue = values && Object.prototype.hasOwnProperty.call(values, childColumnName)
                ? values[childColumnName]
                : row.get(childColumnName, 'current');

            if (fkValue === null || fkValue === undefined) {
                continue;
            }

            const parentTable = rel.parentTable;
            const parentPk = typeof parentTable.getPrimaryKey === 'function' ? parentTable.getPrimaryKey() : [];
            let parentRow = null;
            if (parentPk.length === 1 && parentPk[0] === parentColumnName) {
                parentRow = parentTable.find(fkValue);
            } else {
                parentRow = parentTable.findOne({ [parentColumnName]: fkValue });
            }
            if (!parentRow || parentRow.getRowState?.() === 'DELETED') {
                throw new ConstraintViolationError(
                    `Constraint violation: foreign key '${constraint.name}' has no parent row (${parentTable.tableName}.${parentColumnName}=${fkValue})`
                );
            }
        }
    }

    _onRowDeleting(row) {
        if (this.enforceConstraints !== true || this._inConstraintAction === true) {
            return;
        }
        const table = row?._table;
        const constraints = this._foreignKeyConstraints || [];
        for (const constraint of constraints) {
            const rel = constraint.relation;
            if (!rel || rel.parentTable !== table) {
                continue;
            }
            const parentValue = row.get(rel.parentColumn.columnName, 'current');
            const children = rel.getChildRows(row)
                .filter((child) => child.getRowState?.() !== 'DELETED');
            if (children.length === 0) {
                continue;
            }
            if (constraint.deleteRule === 'restrict') {
                throw new ConstraintViolationError(
                    `Constraint violation: cannot delete parent row due to foreign key '${constraint.name}'`
                );
            }
            this._inConstraintAction = true;
            try {
                if (constraint.deleteRule === 'cascade') {
                    for (const child of children) {
                        child.delete();
                    }
                } else if (constraint.deleteRule === 'setnull') {
                    for (const child of children) {
                        child.set(rel.childColumn.columnName, null);
                    }
                }
            } finally {
                this._inConstraintAction = false;
            }
        }
    }

    _onRowValueChange(row, columnName, oldValue, newValue) {
        if (this.enforceConstraints !== true || this._inConstraintAction === true) {
            return;
        }
        const table = row?._table;
        const constraints = this._foreignKeyConstraints || [];
        for (const constraint of constraints) {
            const rel = constraint.relation;
            if (!rel) continue;

            if (rel.childTable === table && rel.childColumn.columnName === columnName) {
                this._evaluateForeignKeyConstraints(row, { ...row._values, [columnName]: newValue });
                continue;
            }

            if (rel.parentTable === table && rel.parentColumn.columnName === columnName) {
                if (oldValue === newValue) {
                    continue;
                }
                const children = rel.getChildRows(row)
                    .filter((child) => child.getRowState?.() !== 'DELETED');
                if (children.length === 0) {
                    continue;
                }
                if (constraint.updateRule === 'restrict') {
                    throw new ConstraintViolationError(
                        `Constraint violation: cannot update parent key due to foreign key '${constraint.name}'`
                    );
                }
                this._inConstraintAction = true;
                try {
                    if (constraint.updateRule === 'cascade') {
                        for (const child of children) {
                            child.set(rel.childColumn.columnName, newValue);
                        }
                    } else if (constraint.updateRule === 'setnull') {
                        for (const child of children) {
                            child.set(rel.childColumn.columnName, null);
                        }
                    }
                } finally {
                    this._inConstraintAction = false;
                }
            }
        }
    }

    /**
     * @param {string} relationName - Name of the relation to remove
     */
    removeRelation(relationName) {
        const index = this.relations.findIndex(rel => rel.relationName === relationName);
        if (index !== -1) {
            this.relations.splice(index, 1);
        }
    }

    /**
     * @param {string} tableName - Name of the table to check
     * @returns {boolean} True if the table exists, false otherwise
     */
    hasTable(tableName) {
        return this.tables.has(tableName);
    }

    /**
     * Get relations for a specific table
     * @param {string} tableName - Name of the table to find relations for
     * @returns {Array<DataRelation>} Array of relations involving the table
     */
    getRelations(tableName) {
        return this.relations.filter(rel => 
            rel.parentTable.tableName === tableName || 
            rel.childTable.tableName === tableName
        );
    }

    getRelation(relationName) {
        const relation = this.relations.find(rel => rel.relationName === relationName);
        if (!relation) {
            throw new Error(`Relation '${relationName}' does not exist`);
        }
        return relation;
    }

    /**
     * Get child rows for a parent row
     * @param {DataRow} parentRow - Parent row
     * @param {string} relationName - Name of the relation
     * @returns {Array<DataRow>} Array of child rows
     */
    getChildRows(parentRow, relationName) {
        return this.getRelation(relationName).getChildRows(parentRow);
    }

    /**
     * Get parent row for a child row
     * @param {DataRow} childRow - Child row
     * @param {string} relationName - Name of the relation
     * @returns {DataRow} Parent row
     */
    getParentRow(childRow, relationName) {
        return this.getRelation(relationName).getParentRow(childRow);
    }

    /**
     * Clears all data from all tables while maintaining structure
     */
    clear() {
        for (const table of this.tables.values()) {
            table.clear();
        }
    }

    getChangeSet(options = {}) {
        const { DataSetChangeSet } = require('./changeTracking');
        return DataSetChangeSet.fromDataSet(this, options);
    }

    getCommands(options = {}) {
        const commands = [];
        for (const table of this.tables.values()) {
            if (typeof table.getCommands === 'function') {
                commands.push(table.getCommands(options));
            }
        }
        return {
            dataSetName: this.dataSetName,
            tables: commands
        };
    }

    applyChangeSet(changeSet, options = {}) {
        const opts = normalizeApplyDataSetChangeSetOptions(options);
        const normalized = normalizeDataSetChangeSet(changeSet);

        if (opts.strict === true) {
            if (normalized.dataSetName && this.dataSetName && normalized.dataSetName !== this.dataSetName) {
                throw new SchemaMismatchError(
                    `applyChangeSet() dataSetName mismatch: '${normalized.dataSetName}' -> '${this.dataSetName}'`
                );
            }
        }

        const result = {
            dataSetName: this.dataSetName,
            appliedTables: [],
            skippedTables: []
        };

        for (const tableChangeSet of normalized.tables) {
            const tableName = tableChangeSet.tableName || '';
            if (!this.hasTable(tableName)) {
                if (opts.missingTableAction === 'error') {
                    throw new SchemaMismatchError(`Missing target table '${tableName}'.`);
                }
                result.skippedTables.push(tableName);
                continue;
            }
            const table = this.table(tableName);
            const summary = table.applyChangeSet(tableChangeSet, opts);
            result.appliedTables.push({
                tableName,
                summary
            });
        }

        return result;
    }

    /**
     * Merges another DataSet, or a single DataTable, into this DataSet.
     * Existing tables are merged by table name; missing tables follow missingSchemaAction.
     * @param {DataSet|DataTable} source - Source dataset or table
     * @param {Object} [options] - Merge options passed to DataTable.merge()
     * @returns {Object} Merge summary
     */
    merge(source, options = {}) {
        const mergeOptions = this._normalizeMergeOptions(options);
        const result = {
            dataSetName: this.dataSetName,
            mergedTables: [],
            addedTables: [],
            ignoredTables: [],
            relationsAdded: []
        };

        if (source instanceof DataTable) {
            this._mergeSourceTable(source, mergeOptions, result);
            return result;
        }

        if (!(source instanceof DataSet)) {
            throw new SchemaMismatchError('DataSet.merge() expects a DataSet or DataTable source');
        }

        for (const sourceTable of source.tables.values()) {
            this._mergeSourceTable(sourceTable, mergeOptions, result);
        }

        if (mergeOptions.missingSchemaAction === 'add') {
            this._mergeRelationsFrom(source, result);
        }

        return result;
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

    _mergeSourceTable(sourceTable, mergeOptions, result) {
        if (this.hasTable(sourceTable.tableName)) {
            const mergeResult = this.table(sourceTable.tableName).merge(sourceTable, mergeOptions);
            result.mergedTables.push({
                tableName: sourceTable.tableName,
                result: mergeResult
            });
            return;
        }

        if (mergeOptions.missingSchemaAction === 'add') {
            this.addTable(sourceTable.clone());
            result.addedTables.push(sourceTable.tableName);
            return;
        }

        if (mergeOptions.missingSchemaAction === 'ignore') {
            result.ignoredTables.push(sourceTable.tableName);
            return;
        }

        throw new SchemaMismatchError(
            `Table '${sourceTable.tableName}' does not exist in the DataSet`
        );
    }

    _mergeRelationsFrom(sourceDataSet, result) {
        for (const relation of sourceDataSet.relations) {
            const existing = this.relations.find((rel) => rel.relationName === relation.relationName);
            if (existing) {
                if (!this._isSameRelation(existing, relation)) {
                    throw new SchemaMismatchError(`Relation '${relation.relationName}' already exists with a different definition`);
                }
                continue;
            }

            const parentTableName = relation.parentTable.tableName;
            const childTableName = relation.childTable.tableName;
            const parentColumnName = relation.parentColumn.columnName;
            const childColumnName = relation.childColumn.columnName;

            if (!this.hasTable(parentTableName) || !this.hasTable(childTableName)) {
                continue;
            }

            const parentTable = this.table(parentTableName);
            const childTable = this.table(childTableName);
            if (!parentTable.columnExists(parentColumnName) || !childTable.columnExists(childColumnName)) {
                continue;
            }

            this.addRelation(
                relation.relationName,
                parentTable.columns.get(parentColumnName),
                childTable.columns.get(childColumnName)
            );
            result.relationsAdded.push(relation.relationName);
        }
    }

    _isSameRelation(targetRelation, sourceRelation) {
        return targetRelation.parentTable.tableName === sourceRelation.parentTable.tableName &&
            targetRelation.childTable.tableName === sourceRelation.childTable.tableName &&
            targetRelation.parentColumn.columnName === sourceRelation.parentColumn.columnName &&
            targetRelation.childColumn.columnName === sourceRelation.childColumn.columnName;
    }

    toJSON() {
        return {
            dataSetName: this.dataSetName,
            tables: Array.from(this.tables.values()).map(table => table.toJSON()),
            relations: (this.relations || []).map(DebugSchemaSerializer.serializeRelation)
        };
    }

    serialize(options = {}) {
        const payload = {
            dataSetName: this.dataSetName,
            enforceConstraints: this.enforceConstraints === true,
            tables: Array.from(this.tables.values()).map((table) => ({
                tableName: table.tableName,
                payload: table.serialize({ asObject: true })
            })),
            relations: (this.relations || []).map((rel) => ({
                relationName: rel.relationName,
                parentTable: rel.parentTable.tableName,
                parentColumn: rel.parentColumn.columnName,
                childTable: rel.childTable.tableName,
                childColumn: rel.childColumn.columnName
            })),
            foreignKeyConstraints: (this._foreignKeyConstraints || []).map((c) => ({
                name: c.name,
                relationName: c.relationName,
                deleteRule: c.deleteRule,
                updateRule: c.updateRule
            }))
        };
        return options.asObject === true ? payload : JSON.stringify(payload);
    }

    static deserialize(input) {
        const payload = typeof input === 'string' ? JSON.parse(input) : input;
        if (!payload || typeof payload !== 'object') {
            throw new SchemaMismatchError('Invalid DataSet serialized payload.');
        }
        const ds = new DataSet(payload.dataSetName || '');
        ds.enforceConstraints = payload.enforceConstraints !== false;

        const tables = Array.isArray(payload.tables) ? payload.tables : [];
        for (const item of tables) {
            const tablePayload = item?.payload;
            const table = DataTable.deserialize(tablePayload);
            table.tableName = item.tableName || table.tableName;
            ds.addTable(table);
        }

        const relations = Array.isArray(payload.relations) ? payload.relations : [];
        for (const rel of relations) {
            if (!rel) continue;
            ds.addRelation(
                rel.relationName,
                rel.parentTable,
                rel.childTable,
                rel.parentColumn,
                rel.childColumn
            );
        }

        const fks = Array.isArray(payload.foreignKeyConstraints) ? payload.foreignKeyConstraints : [];
        for (const fk of fks) {
            if (!fk) continue;
            ds.addForeignKeyConstraint(fk.relationName, {
                name: fk.name,
                deleteRule: fk.deleteRule,
                updateRule: fk.updateRule
            });
        }

        return ds;
    }

    toDebugView(options = {}) {
        return DebugTableSerializer.dataSetToDebugView(this, options);
    }

    getSchema() {
        return DebugSchemaSerializer.getDataSetSchema(this);
    }

    [NodeInspectFormatter.customInspectSymbol]() {
        return NodeInspectFormatter.inspectDataSet(this);
    }

    /**
     * Creates a deep copy of the DataSet
     * @returns {DataSet} A new instance of DataSet with the same structure and data
     */
    clone() {
        const newDataSet = new DataSet(this.dataSetName);
        
        // Clone tables
        for (const [name, table] of this.tables.entries()) {
            newDataSet.addTable(table.clone());
        }
        
        // Clone relations
        for (const relation of this.relations) {
            const parentTable = newDataSet.table(relation.parentTable.tableName);
            const childTable = newDataSet.table(relation.childTable.tableName);
            
            const parentColumn = parentTable.columns.get(relation.parentColumn.columnName);
            const childColumn = childTable.columns.get(relation.childColumn.columnName);
            
            newDataSet.addRelation(
                relation.relationName,
                parentColumn,
                childColumn
            );
        }
        
        return newDataSet;
    }
}

function normalizeApplyDataSetChangeSetOptions(options) {
    const opts = options || {};
    const missingTableAction = String(opts.missingTableAction || 'ignore').toLowerCase();
    const allowed = ['ignore', 'error'];
    if (!allowed.includes(missingTableAction)) {
        throw new SchemaMismatchError(
            `Invalid missingTableAction '${opts.missingTableAction}'. Expected: ${allowed.join(', ')}`
        );
    }
    return {
        missingTableAction,
        missingRowAction: opts.missingRowAction,
        conflictPolicy: opts.conflictPolicy,
        strict: opts.strict === true
    };
}

function normalizeDataSetChangeSet(changeSet) {
    const raw = changeSet && typeof changeSet.toObject === 'function'
        ? changeSet.toObject()
        : changeSet;
    if (!raw || typeof raw !== 'object') {
        throw new SchemaMismatchError('Invalid changeSet for applyChangeSet().');
    }
    return {
        dataSetName: raw.dataSetName || '',
        tables: Array.isArray(raw.tables) ? raw.tables : []
    };
}

function normalizeFkRule(value) {
    const rule = String(value || 'restrict').toLowerCase();
    if (rule === 'cascade') return 'cascade';
    if (rule === 'setnull' || rule === 'set_null' || rule === 'set null') return 'setnull';
    return 'restrict';
}

module.exports = DataSet;
