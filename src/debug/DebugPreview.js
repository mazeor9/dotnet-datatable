const DEFAULT_MAX_ROWS = 10;

function normalizeMaxRows(maxRows, fallback = DEFAULT_MAX_ROWS) {
    if (maxRows === undefined || maxRows === null) {
        return fallback;
    }

    const value = Number(maxRows);
    if (!Number.isFinite(value)) {
        return fallback;
    }

    return Math.max(0, Math.floor(value));
}

function limitRecords(records, maxRows = DEFAULT_MAX_ROWS) {
    return records.slice(0, normalizeMaxRows(maxRows));
}

function getTablePreview(table, maxRows = DEFAULT_MAX_ROWS, options = {}) {
    const { tableToArray } = require('./DebugTableSerializer');
    return limitRecords(tableToArray(table, { serializeValues: true, ...options }), maxRows);
}

function getViewPreview(view, maxRows = DEFAULT_MAX_ROWS, options = {}) {
    const { viewToArray } = require('./DebugTableSerializer');
    return limitRecords(viewToArray(view, { serializeValues: true, ...options }), maxRows);
}

module.exports = {
    DEFAULT_MAX_ROWS,
    getTablePreview,
    getViewPreview,
    limitRecords,
    normalizeMaxRows
};
