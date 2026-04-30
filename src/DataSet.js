const DataTable = require('./DataTable');
const DataRelation = require('./DataRelation');

class DataSet {
    constructor(dataSetName = '') {
        this.dataSetName = dataSetName;
        this.tables = new Map();
        this.relations = [];
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

    /**
     * Get child rows for a parent row
     * @param {DataRow} parentRow - Parent row
     * @param {string} relationName - Name of the relation
     * @returns {Array<DataRow>} Array of child rows
     */
    getChildRows(parentRow, relationName) {
        const relation = this.relations.find(rel => rel.relationName === relationName);
        if (!relation) {
            throw new Error(`Relation '${relationName}' does not exist`);
        }
        
        const parentValue = parentRow.get(relation.parentColumn.columnName);
        const childTable = relation.childTable;
        
        return childTable.findRows(row => 
            row.get(relation.childColumn.columnName) === parentValue
        );
    }

    /**
     * Get parent row for a child row
     * @param {DataRow} childRow - Child row
     * @param {string} relationName - Name of the relation
     * @returns {DataRow} Parent row
     */
    getParentRow(childRow, relationName) {
        const relation = this.relations.find(rel => rel.relationName === relationName);
        if (!relation) {
            throw new Error(`Relation '${relationName}' does not exist`);
        }
        
        const childValue = childRow.get(relation.childColumn.columnName);
        const parentTable = relation.parentTable;
        
        return parentTable.findOne(row => 
            row.get(relation.parentColumn.columnName) === childValue
        );
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
