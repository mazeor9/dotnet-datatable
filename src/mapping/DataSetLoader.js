const DataSet = require('../DataSet');
const DataTable = require('../DataTable');
const QueryResultMapper = require('./QueryResultMapper');

class DataSetLoader {
    static fromRecordsets(recordsets, options = {}) {
        if (!Array.isArray(recordsets)) {
            throw new Error('DataSet.fromRecordsets() expects an array of recordsets.');
        }

        const dataSet = new DataSet(options.dataSetName || options.name || '');
        const tableNames = options.tableNames || options.tables || [];

        recordsets.forEach((recordset, index) => {
            const tableOptions = resolveTableOptions(options, tableNames, index);
            const tableName = tableOptions.tableName || tableOptions.name || `Table${index + 1}`;
            const table = DataTable.fromRows(recordset || [], {
                ...options,
                ...tableOptions,
                tableName
            });
            dataSet.addTable(table);
        });

        addRelations(dataSet, options.relations || []);
        return dataSet;
    }

    static fromQueryResult(queryResult, options = {}) {
        const mapped = QueryResultMapper.map(queryResult, options);
        return this.fromRecordsets(mapped.recordsets || [mapped.rows], {
            ...options,
            provider: mapped.provider
        });
    }
}

function resolveTableOptions(options, tableNames, index) {
    const tableOption = tableNames[index];
    if (typeof tableOption === 'string') {
        return { tableName: tableOption };
    }
    if (tableOption && typeof tableOption === 'object') {
        return tableOption;
    }

    if (Array.isArray(options.tableOptions)) {
        return options.tableOptions[index] || {};
    }

    return {};
}

function addRelations(dataSet, relations) {
    for (const relation of relations) {
        dataSet.addRelation(
            relation.name || relation.relationName,
            relation.parentTable,
            relation.childTable,
            relation.parentColumn,
            relation.childColumn
        );
    }
}

module.exports = DataSetLoader;
