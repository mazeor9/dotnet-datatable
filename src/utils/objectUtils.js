function isPlainObject(value) {
    if (!value || typeof value !== 'object') {
        return false;
    }
    const proto = Object.getPrototypeOf(value);
    return proto === Object.prototype || proto === null;
}

function getPath(source, path) {
    if (!path) {
        return source;
    }
    const parts = Array.isArray(path) ? path : String(path).split('.');
    let current = source;
    for (const part of parts) {
        if (current === null || current === undefined) {
            return undefined;
        }
        current = current[part];
    }
    return current;
}

function toCamelCase(value) {
    const text = String(value).trim();
    const normalized = /^[A-Z0-9_\-\s]+$/.test(text) ? text.toLowerCase() : text;
    return normalized
        .replace(/[-_\s]+(.)?/g, (_, chr) => chr ? chr.toUpperCase() : '')
        .replace(/^[A-Z]/, chr => chr.toLowerCase());
}

function toSnakeCase(value) {
    return String(value)
        .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
        .replace(/[-\s]+/g, '_')
        .toLowerCase();
}

function transformColumnName(name, transform) {
    if (typeof transform === 'function') {
        return transform(name);
    }

    switch (String(transform || 'none').toLowerCase()) {
        case 'camelcase':
        case 'camel_case':
        case 'camel':
            return toCamelCase(name);
        case 'snake_case':
        case 'snakecase':
        case 'snake':
            return toSnakeCase(name);
        case 'none':
        default:
            return String(name);
    }
}

function createColumnNameResolver(options = {}) {
    const renameColumns = options.renameColumns || {};
    const transform = options.columnNameTransform || 'none';

    return function resolveColumnName(sourceName) {
        const originalName = String(sourceName);
        const renamed = typeof renameColumns === 'function'
            ? renameColumns(originalName)
            : Object.prototype.hasOwnProperty.call(renameColumns, originalName)
                ? renameColumns[originalName]
                : originalName;

        return transformColumnName(renamed, transform);
    };
}

function shouldIncludeColumn(sourceName, mappedName, options = {}) {
    const includeColumns = options.includeColumns || null;
    const excludeColumns = options.excludeColumns || null;

    if (Array.isArray(includeColumns) && includeColumns.length > 0) {
        if (!includeColumns.includes(sourceName) && !includeColumns.includes(mappedName)) {
            return false;
        }
    }

    if (Array.isArray(excludeColumns) && excludeColumns.length > 0) {
        if (excludeColumns.includes(sourceName) || excludeColumns.includes(mappedName)) {
            return false;
        }
    }

    return true;
}

function toPlainObject(record) {
    if (record === null || record === undefined) {
        return {};
    }

    if (record._values && typeof record.getRowState === 'function') {
        return { ...record._values };
    }

    if (typeof record.get === 'function' && record.constructor && record.constructor.name !== 'DataRow') {
        try {
            const plain = record.get({ plain: true });
            if (plain && typeof plain === 'object') {
                return { ...plain };
            }
        } catch (_) {
            // Not a Sequelize-style model instance; fall through.
        }
    }

    if (
        typeof record.toJSON === 'function' &&
        !(record instanceof Date) &&
        !Array.isArray(record)
    ) {
        try {
            const json = record.toJSON();
            if (json && typeof json === 'object') {
                return { ...json };
            }
        } catch (_) {
            // Some model instances expose toJSON but can throw when detached.
        }
    }

    if (isPlainObject(record)) {
        return { ...record };
    }

    if (record && typeof record === 'object') {
        const plain = {};
        for (const key of Object.keys(record)) {
            plain[key] = record[key];
        }
        return plain;
    }

    return {};
}

function normalizeRecord(record, options = {}) {
    const resolver = options.columnNameResolver || createColumnNameResolver(options);
    const plain = toPlainObject(record);
    const normalized = {};

    for (const [sourceName, value] of Object.entries(plain)) {
        const mappedName = resolver(sourceName);
        if (shouldIncludeColumn(sourceName, mappedName, options)) {
            normalized[mappedName] = value;
        }
    }

    return normalized;
}

function normalizeRecords(records, options = {}) {
    if (!Array.isArray(records)) {
        return [];
    }
    const resolver = options.columnNameResolver || createColumnNameResolver(options);
    return records.map(record => normalizeRecord(record, { ...options, columnNameResolver: resolver }));
}

module.exports = {
    createColumnNameResolver,
    getPath,
    isPlainObject,
    normalizeRecord,
    normalizeRecords,
    shouldIncludeColumn,
    toCamelCase,
    toPlainObject,
    toSnakeCase,
    transformColumnName
};
