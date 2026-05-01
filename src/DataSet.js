const DataTable = require('./DataTable');
const DataRelation = require('./DataRelation');
const { SchemaMismatchError } = require('./errors');

class DataSet {
    constructor(dataSetName = '') {
        this.dataSetName = dataSetName;
        this.tables = new Map();
        this.relations = [];
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

module.exports = DataSet;
