const { TypeMismatchError } = require('../errors');
const { describeValueType, isBuffer } = require('../utils/typeUtils');

const POSTGRES_TYPES = {
    16: 'boolean',
    17: 'buffer',
    18: 'string',
    20: 'bigint',
    21: 'integer',
    23: 'integer',
    25: 'string',
    700: 'number',
    701: 'number',
    1042: 'string',
    1043: 'string',
    1082: 'date',
    1114: 'date',
    1184: 'date',
    114: 'json',
    1700: 'number',
    2950: 'string',
    3802: 'json'
};

const MYSQL_TYPES = {
    0: 'number',
    1: 'integer',
    2: 'integer',
    3: 'integer',
    4: 'number',
    5: 'number',
    6: 'null',
    7: 'date',
    8: 'bigint',
    9: 'integer',
    10: 'date',
    11: 'string',
    12: 'date',
    13: 'integer',
    14: 'date',
    15: 'string',
    16: 'buffer',
    245: 'json',
    246: 'number',
    247: 'string',
    248: 'string',
    249: 'buffer',
    250: 'buffer',
    251: 'buffer',
    252: 'buffer',
    253: 'string',
    254: 'string',
    255: 'object'
};

const SQLSERVER_TYPES = {
    bigint: 'bigint',
    binary: 'buffer',
    bit: 'boolean',
    char: 'string',
    date: 'date',
    datetime: 'date',
    datetime2: 'date',
    datetimeoffset: 'date',
    decimal: 'number',
    float: 'number',
    image: 'buffer',
    int: 'integer',
    money: 'number',
    nchar: 'string',
    ntext: 'string',
    numeric: 'number',
    nvarchar: 'string',
    real: 'number',
    smalldatetime: 'date',
    smallint: 'integer',
    smallmoney: 'number',
    text: 'string',
    time: 'string',
    timestamp: 'buffer',
    tinyint: 'integer',
    uniqueidentifier: 'string',
    varbinary: 'buffer',
    varchar: 'string',
    xml: 'string'
};

const SQLITE_TYPES = {
    integer: 'integer',
    int: 'integer',
    real: 'number',
    double: 'number',
    float: 'number',
    numeric: 'number',
    decimal: 'number',
    text: 'string',
    varchar: 'string',
    char: 'string',
    clob: 'string',
    blob: 'buffer',
    boolean: 'boolean',
    date: 'date',
    datetime: 'date',
    json: 'json'
};

const ORACLE_TYPES = {
    bfile: 'buffer',
    binary_double: 'number',
    binary_float: 'number',
    blob: 'buffer',
    char: 'string',
    clob: 'string',
    date: 'date',
    float: 'number',
    integer: 'integer',
    json: 'json',
    long: 'string',
    nchar: 'string',
    nclob: 'string',
    number: 'number',
    nvarchar2: 'string',
    raw: 'buffer',
    rowid: 'string',
    timestamp: 'date',
    varchar: 'string',
    varchar2: 'string'
};

const TYPE_ALIASES = {
    bool: 'boolean',
    int: 'integer',
    int2: 'integer',
    int4: 'integer',
    int8: 'bigint',
    long: 'bigint',
    double: 'number',
    decimal: 'number',
    numeric: 'number',
    float: 'number',
    datetime: 'date',
    timestamp: 'date',
    timestamptz: 'date',
    varchar: 'string',
    nvarchar: 'string',
    text: 'string',
    char: 'string',
    blob: 'buffer',
    bytea: 'buffer'
};

class TypeMapper {
    static normalizeType(type) {
        if (type === null || type === undefined || type === '') {
            return 'any';
        }
        const lower = String(type).toLowerCase();
        return TYPE_ALIASES[lower] || lower;
    }

    static inferValueType(value) {
        const type = describeValueType(value);
        if (type === 'integer') return 'integer';
        if (type === 'object') return 'object';
        return type;
    }

    static reconcileTypes(types) {
        const meaningful = [...new Set(types.filter(type => type && type !== 'null' && type !== 'undefined'))];
        if (meaningful.length === 0) {
            return 'any';
        }
        if (meaningful.length === 1) {
            return meaningful[0];
        }
        if (meaningful.every(type => type === 'integer' || type === 'number')) {
            return 'number';
        }
        if (meaningful.every(type => type === 'object' || type === 'json')) {
            return 'json';
        }
        return 'any';
    }

    static fromPostgresDataTypeID(dataTypeID) {
        return POSTGRES_TYPES[Number(dataTypeID)] || 'any';
    }

    static fromMySqlColumnType(columnType, metadata = {}, options = {}) {
        const numericType = Number(columnType);
        if (
            options.tinyIntOneIsBoolean !== false &&
            numericType === 1 &&
            Number(metadata.columnLength ?? metadata.length) === 1
        ) {
            return 'boolean';
        }
        return MYSQL_TYPES[numericType] || 'any';
    }

    static fromSqlServerType(type) {
        const typeName = typeof type === 'string'
            ? type
            : type && (type.name || type.declaration || type.type);
        if (!typeName) {
            return 'any';
        }
        return SQLSERVER_TYPES[String(typeName).toLowerCase()] || 'any';
    }

    static fromSqliteType(type) {
        if (!type) {
            return 'any';
        }
        const lower = String(type).toLowerCase();
        for (const [key, mapped] of Object.entries(SQLITE_TYPES)) {
            if (lower.includes(key)) {
                return mapped;
            }
        }
        return 'any';
    }

    static fromOracleType(type, metadata = {}) {
        const typeName = type ||
            metadata.dbTypeName ||
            metadata.databaseType ||
            metadata.typeName ||
            metadata.fetchType;
        if (!typeName) {
            return 'any';
        }
        const lower = String(typeName).toLowerCase();
        for (const [key, mapped] of Object.entries(ORACLE_TYPES)) {
            if (lower.includes(key)) {
                return mapped;
            }
        }
        return 'any';
    }

    static fromDatabaseType(type, provider, metadata = {}, options = {}) {
        const providerName = String(provider || '').toLowerCase();
        if (providerName === 'postgres' || providerName === 'postgresql' || providerName === 'pg') {
            if (metadata.dataTypeID !== undefined) {
                return this.fromPostgresDataTypeID(metadata.dataTypeID);
            }
        }
        if (providerName === 'mysql' || providerName === 'mysql2') {
            if (metadata.columnType !== undefined || metadata.type !== undefined) {
                return this.fromMySqlColumnType(metadata.columnType ?? metadata.type, metadata, options);
            }
        }
        if (providerName === 'mssql' || providerName === 'sqlserver' || providerName === 'sql-server') {
            return this.fromSqlServerType(type || metadata.type);
        }
        if (providerName === 'sqlite') {
            return this.fromSqliteType(type || metadata.type);
        }
        if (providerName === 'oracle' || providerName === 'oracledb') {
            return this.fromOracleType(type, metadata);
        }

        if (metadata.dataTypeID !== undefined) {
            return this.fromPostgresDataTypeID(metadata.dataTypeID);
        }
        if (metadata.columnType !== undefined) {
            return this.fromMySqlColumnType(metadata.columnType, metadata, options);
        }
        if (metadata.type) {
            const sqlServerType = this.fromSqlServerType(metadata.type);
            if (sqlServerType !== 'any') {
                return sqlServerType;
            }
            return this.fromSqliteType(metadata.type);
        }
        if (metadata.dbTypeName !== undefined) {
            return this.fromOracleType(metadata.dbTypeName, metadata);
        }

        return this.normalizeType(type);
    }

    static convertValue(value, targetType, options = {}) {
        const type = this.normalizeType(targetType);
        const strict = options.strict === true;

        if (value === null || value === undefined || type === 'any' || type === 'null' || type === 'undefined') {
            return value;
        }

        try {
            switch (type) {
                case 'number':
                    return this._toNumber(value);
                case 'integer':
                    return this._toInteger(value);
                case 'bigint':
                    return typeof value === 'bigint' ? value : BigInt(value);
                case 'boolean':
                    return this._toBoolean(value);
                case 'date':
                    return this._toDate(value);
                case 'string':
                    return String(value);
                case 'json':
                    return this._toJson(value);
                case 'object':
                    return typeof value === 'object' && !Array.isArray(value) ? value : this._toJson(value);
                case 'array':
                    if (Array.isArray(value)) return value;
                    if (typeof value === 'string') {
                        const parsed = JSON.parse(value);
                        if (Array.isArray(parsed)) return parsed;
                    }
                    throw new Error('expected array');
                case 'buffer':
                    if (isBuffer(value)) return value;
                    if (typeof Buffer !== 'undefined') return Buffer.from(value);
                    return value;
                default:
                    return value;
            }
        } catch (_) {
            if (!strict) {
                return value;
            }
            throw new TypeMismatchError(
                `Invalid value: expected ${type}, got ${describeValueType(value)}`
            );
        }
    }

    static _toNumber(value) {
        if (typeof value === 'number') {
            if (Number.isNaN(value)) throw new Error('NaN');
            return value;
        }
        if (typeof value === 'bigint') {
            return Number(value);
        }
        if (typeof value === 'string' && value.trim() !== '') {
            const num = Number(value);
            if (!Number.isNaN(num)) return num;
        }
        throw new Error('expected number');
    }

    static _toInteger(value) {
        const num = this._toNumber(value);
        if (!Number.isInteger(num)) {
            throw new Error('expected integer');
        }
        return num;
    }

    static _toBoolean(value) {
        if (typeof value === 'boolean') {
            return value;
        }
        if (typeof value === 'number') {
            return value !== 0;
        }
        if (typeof value === 'string') {
            const lower = value.trim().toLowerCase();
            if (['true', '1', 'yes', 'y'].includes(lower)) return true;
            if (['false', '0', 'no', 'n'].includes(lower)) return false;
        }
        throw new Error('expected boolean');
    }

    static _toDate(value) {
        if (value instanceof Date) {
            if (Number.isNaN(value.getTime())) throw new Error('invalid date');
            return value;
        }
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
            throw new Error('expected date');
        }
        return date;
    }

    static _toJson(value) {
        if (typeof value === 'string') {
            return JSON.parse(value);
        }
        if (value && typeof value === 'object') {
            return value;
        }
        throw new Error('expected json');
    }
}

module.exports = TypeMapper;
