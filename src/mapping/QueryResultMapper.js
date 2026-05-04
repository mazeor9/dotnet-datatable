const { SchemaMismatchError } = require('../errors');
const { getPath } = require('../utils/objectUtils');

class QueryResultMapper {
    static map(queryResult, options = {}) {
        if (options.rowsPath) {
            const rows = getPath(queryResult, options.rowsPath);
            if (!Array.isArray(rows)) {
                throw new SchemaMismatchError(`Unsupported query result shape. rowsPath '${options.rowsPath}' did not resolve to an array.`);
            }
            return this._createResult({
                rows,
                fields: options.fieldsPath ? getPath(queryResult, options.fieldsPath) : options.fields,
                provider: options.provider || 'custom',
                recordsets: [rows]
            }, options);
        }

        if (Array.isArray(queryResult)) {
            if (this._looksLikeMySqlTuple(queryResult)) {
                return this._createResult({
                    rows: queryResult[0],
                    fields: queryResult[1],
                    provider: options.provider || 'mysql2',
                    recordsets: [queryResult[0]]
                }, options);
            }

            const fields = options.fields ||
                queryResult.fields ||
                queryResult.columns ||
                queryResult.columnMetadata ||
                queryResult.metaData;

            return this._createResult({
                rows: queryResult,
                fields,
                provider: options.provider || inferProviderFromFields(fields) || 'array',
                recordsets: [queryResult]
            }, options);
        }

        if (!queryResult || typeof queryResult !== 'object') {
            throw new SchemaMismatchError('Unsupported query result shape. Use rowsPath option.');
        }

        if (Array.isArray(queryResult.rows)) {
            const fields = queryResult.fields ||
                queryResult.columns ||
                queryResult.columnMetadata ||
                queryResult.metaData ||
                queryResult.meta;

            return this._createResult({
                rows: queryResult.rows,
                fields,
                provider: options.provider || inferProviderFromResult(queryResult, fields) || 'custom',
                recordsets: [queryResult.rows]
            }, options);
        }

        if (Array.isArray(queryResult.recordset)) {
            return this._createResult({
                rows: queryResult.recordset,
                fields: queryResult.fields || queryResult.columns || queryResult.recordset.columns,
                provider: options.provider || 'mssql',
                recordsets: Array.isArray(queryResult.recordsets) ? queryResult.recordsets : [queryResult.recordset]
            }, options);
        }

        if (Array.isArray(queryResult.recordsets)) {
            const first = queryResult.recordsets[options.recordsetIndex || 0] || [];
            return this._createResult({
                rows: first,
                fields: queryResult.fields || queryResult.columns,
                provider: options.provider || 'mssql',
                recordsets: queryResult.recordsets
            }, options);
        }

        for (const key of ['data', 'result', 'items', 'records', 'values', 'documents', 'docs']) {
            if (Array.isArray(queryResult[key])) {
                const fields = options.fields ||
                    queryResult.fields ||
                    queryResult.columns ||
                    queryResult.columnMetadata ||
                    queryResult.metaData;

                return this._createResult({
                    rows: queryResult[key],
                    fields,
                    provider: options.provider || inferProviderFromResult(queryResult, fields) || 'custom',
                    recordsets: [queryResult[key]]
                }, options);
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

    static _createResult(result, options = {}) {
        const fields = normalizeFields(result.fields);
        const rows = mapRows(result.rows, fields);
        const recordsets = Array.isArray(result.recordsets)
            ? result.recordsets.map((recordset, index) => {
                if (index === 0) {
                    return rows;
                }
                const recordsetFields = normalizeFields(recordset?.fields || recordset?.columns || fields);
                return mapRows(recordset || [], recordsetFields);
            })
            : [rows];

        return {
            rows,
            fields,
            provider: options.provider || result.provider || inferProviderFromFields(fields) || 'custom',
            recordsets
        };
    }
}

function mapRows(rows, fields) {
    if (!Array.isArray(rows)) {
        return [];
    }
    if (!rows.some(Array.isArray)) {
        return rows;
    }

    const names = getFieldNames(fields);
    if (names.length === 0) {
        return rows;
    }

    return rows.map(row => {
        if (!Array.isArray(row)) {
            return row;
        }
        const record = {};
        for (let index = 0; index < names.length; index++) {
            record[names[index]] = row[index];
        }
        return record;
    });
}

function normalizeFields(fields) {
    if (!fields) {
        return undefined;
    }
    if (Array.isArray(fields)) {
        return fields.map((field, index) => normalizeField(field, index)).filter(Boolean);
    }
    if (fields && typeof fields === 'object') {
        return Object.entries(fields).map(([name, field]) => {
            if (field && typeof field === 'object') {
                return { name, ...field };
            }
            return { name, type: field };
        });
    }
    return fields;
}

function normalizeField(field, index) {
    if (typeof field === 'string') {
        return { name: field, ordinal: index };
    }
    if (field && typeof field === 'object') {
        return field;
    }
    return null;
}

function getFieldNames(fields) {
    if (!Array.isArray(fields)) {
        return [];
    }
    return fields
        .map(field => {
            if (typeof field === 'string') {
                return field;
            }
            return field && (
                field.name ||
                field.columnName ||
                field.column ||
                field.alias ||
                field.columnAlias
            );
        })
        .filter(Boolean);
}

function inferProviderFromResult(result, fields) {
    if (result.metaData) {
        return 'oracledb';
    }
    if (result.rows && result.columns && !result.fields) {
        return 'sqlite';
    }
    return inferProviderFromFields(fields);
}

function inferProviderFromFields(fields) {
    const list = normalizeFields(fields);
    if (!Array.isArray(list)) {
        return null;
    }
    if (list.some(field => field && (field.dataTypeID !== undefined || field.tableID !== undefined))) {
        return 'pg';
    }
    if (list.some(field => field && (field.columnType !== undefined || field.flags !== undefined || field.decimals !== undefined))) {
        return 'mysql2';
    }
    if (list.some(field => field && (field.dbTypeName !== undefined || field.fetchType !== undefined))) {
        return 'oracledb';
    }
    return null;
}

module.exports = QueryResultMapper;
