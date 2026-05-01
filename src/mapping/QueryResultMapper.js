const { SchemaMismatchError } = require('../errors');
const { getPath } = require('../utils/objectUtils');

class QueryResultMapper {
    static map(queryResult, options = {}) {
        if (options.rowsPath) {
            const rows = getPath(queryResult, options.rowsPath);
            if (!Array.isArray(rows)) {
                throw new SchemaMismatchError(`Unsupported query result shape. rowsPath '${options.rowsPath}' did not resolve to an array.`);
            }
            return {
                rows,
                fields: options.fieldsPath ? getPath(queryResult, options.fieldsPath) : options.fields,
                provider: options.provider || 'custom',
                recordsets: [rows]
            };
        }

        if (Array.isArray(queryResult)) {
            if (this._looksLikeMySqlTuple(queryResult)) {
                return {
                    rows: queryResult[0],
                    fields: queryResult[1],
                    provider: options.provider || 'mysql2',
                    recordsets: [queryResult[0]]
                };
            }

            return {
                rows: queryResult,
                fields: options.fields,
                provider: options.provider || 'array',
                recordsets: [queryResult]
            };
        }

        if (!queryResult || typeof queryResult !== 'object') {
            throw new SchemaMismatchError('Unsupported query result shape. Use rowsPath option.');
        }

        if (Array.isArray(queryResult.rows)) {
            return {
                rows: queryResult.rows,
                fields: queryResult.fields,
                provider: options.provider || 'pg',
                recordsets: [queryResult.rows]
            };
        }

        if (Array.isArray(queryResult.recordset)) {
            return {
                rows: queryResult.recordset,
                fields: queryResult.fields || queryResult.columns || queryResult.recordset.columns,
                provider: options.provider || 'mssql',
                recordsets: Array.isArray(queryResult.recordsets) ? queryResult.recordsets : [queryResult.recordset]
            };
        }

        if (Array.isArray(queryResult.recordsets)) {
            const first = queryResult.recordsets[options.recordsetIndex || 0] || [];
            return {
                rows: first,
                fields: queryResult.fields || queryResult.columns,
                provider: options.provider || 'mssql',
                recordsets: queryResult.recordsets
            };
        }

        for (const key of ['data', 'result', 'items', 'records', 'values']) {
            if (Array.isArray(queryResult[key])) {
                return {
                    rows: queryResult[key],
                    fields: options.fields,
                    provider: options.provider || 'custom',
                    recordsets: [queryResult[key]]
                };
            }
        }

        throw new SchemaMismatchError('Unsupported query result shape. Use rowsPath option.');
    }

    static extractRows(queryResult, options = {}) {
        return this.map(queryResult, options).rows;
    }

    static extractRecordsets(queryResult, options = {}) {
        return this.map(queryResult, options).recordsets;
    }

    static _looksLikeMySqlTuple(value) {
        if (value.length !== 2) {
            return false;
        }
        if (!Array.isArray(value[0]) || !Array.isArray(value[1])) {
            return false;
        }
        if (value[1].length === 0) {
            return true;
        }
        return value[1].every(field => field && typeof field === 'object' && (
            Object.prototype.hasOwnProperty.call(field, 'name') ||
            Object.prototype.hasOwnProperty.call(field, 'columnType') ||
            Object.prototype.hasOwnProperty.call(field, 'columnLength')
        ));
    }
}

module.exports = QueryResultMapper;
