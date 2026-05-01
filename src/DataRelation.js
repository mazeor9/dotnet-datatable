class DataRelation {
    /**
     * @param {string} relationName - Name of the relation
     * @param {DataColumn} parentColumn - Parent column
     * @param {DataColumn} childColumn - Child column
     */
    constructor(relationName, parentColumn, childColumn) {
        this.relationName = relationName;
        this.parentColumn = parentColumn;
        this.childColumn = childColumn;
        this.parentTable = parentColumn.table;
        this.childTable = childColumn.table;
    }

    /**
     * Check if a relationship between a parent and child row is valid
     * @param {DataRow} parentRow - Parent row
     * @param {DataRow} childRow - Child row
     * @returns {boolean} True if the relationship is valid
     */
    isValid(parentRow, childRow) {
        const parentValue = parentRow.get(this.parentColumn.columnName);
        const childValue = childRow.get(this.childColumn.columnName);
        
        return parentValue === childValue;
    }

    getChildRows(parentRow) {
        const parentValue = parentRow.get(this.parentColumn.columnName);
        return this.childTable.findRows(row =>
            row.get(this.childColumn.columnName) === parentValue
        );
    }

    getParentRow(childRow) {
        const childValue = childRow.get(this.childColumn.columnName);
        return this.parentTable.findOne(row =>
            row.get(this.parentColumn.columnName) === childValue
        );
    }

    /**
     * Returns a string representation of the relation
     * @returns {string} String representation
     */
    toString() {
        return `${this.relationName}: ${this.parentTable.tableName}.${this.parentColumn.columnName} -> ${this.childTable.tableName}.${this.childColumn.columnName}`;
    }
}

module.exports = DataRelation;
