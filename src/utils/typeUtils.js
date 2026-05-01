const DataRowState = require('../enums/DataRowState');

function isBuffer(value) {
    return typeof Buffer !== 'undefined' && Buffer.isBuffer(value);
}

function cloneValue(value) {
    if (value instanceof Date) {
        return new Date(value.getTime());
    }
    if (isBuffer(value)) {
        return Buffer.from(value);
    }
    if (Array.isArray(value)) {
        return value.map(cloneValue);
    }
    if (value && typeof value === 'object') {
        const copy = {};
        for (const [key, item] of Object.entries(value)) {
            copy[key] = cloneValue(item);
        }
        return copy;
    }
    return value;
}

function cloneValues(values) {
    const copy = {};
    for (const [key, value] of Object.entries(values || {})) {
        copy[key] = cloneValue(value);
    }
    return copy;
}

function areValuesEqual(a, b) {
    if (a instanceof Date && b instanceof Date) {
        return a.getTime() === b.getTime();
    }
    if (isBuffer(a) && isBuffer(b)) {
        return a.equals(b);
    }
    return a === b;
}

function normalizeRowState(state, fallback = DataRowState.UNCHANGED) {
    if (!state) {
        return fallback;
    }

    const value = String(state).toUpperCase();
    switch (value) {
        case DataRowState.DETACHED:
        case 'DETACHED':
            return DataRowState.DETACHED;
        case DataRowState.ADDED:
        case 'ADDED':
            return DataRowState.ADDED;
        case DataRowState.MODIFIED:
        case 'MODIFIED':
            return DataRowState.MODIFIED;
        case DataRowState.DELETED:
        case 'DELETED':
            return DataRowState.DELETED;
        case DataRowState.UNCHANGED:
        case 'UNCHANGED':
            return DataRowState.UNCHANGED;
        default:
            return fallback;
    }
}

function describeValueType(value) {
    if (value === null) return 'null';
    if (value === undefined) return 'undefined';
    if (value instanceof Date) return 'date';
    if (isBuffer(value)) return 'buffer';
    if (Array.isArray(value)) return 'array';
    if (typeof value === 'bigint') return 'bigint';
    if (typeof value === 'number' && Number.isInteger(value)) return 'integer';
    return typeof value;
}

module.exports = {
    areValuesEqual,
    cloneValue,
    cloneValues,
    describeValueType,
    isBuffer,
    normalizeRowState
};
