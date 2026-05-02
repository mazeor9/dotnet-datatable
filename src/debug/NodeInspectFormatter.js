const util = require('node:util');
const DebugFormatter = require('./DebugFormatter');
const { DEFAULT_MAX_ROWS } = require('./DebugPreview');

const customInspectSymbol = util.inspect.custom || Symbol.for('nodejs.util.inspect.custom');

function inspectDataTable(value, depth, options) {
    return DebugFormatter.formatDataTable(value, { maxRows: resolveMaxRows(options) });
}

function inspectDataRow(value) {
    return DebugFormatter.formatDataRow(value);
}

function inspectDataColumn(value) {
    return DebugFormatter.formatDataColumn(value);
}

function inspectDataView(value, depth, options) {
    return DebugFormatter.formatDataView(value, { maxRows: resolveMaxRows(options) });
}

function inspectDataSet(value) {
    return DebugFormatter.formatDataSet(value);
}

function inspectDataColumnCollection(value) {
    return DebugFormatter.formatDataColumnCollection(value);
}

function inspectDataRowCollection(value, depth, options) {
    return DebugFormatter.formatDataRowCollection(value, { maxRows: resolveMaxRows(options) });
}

function resolveMaxRows(options) {
    if (!options || typeof options.maxArrayLength !== 'number' || options.maxArrayLength < 0) {
        return DEFAULT_MAX_ROWS;
    }
    return Math.min(DEFAULT_MAX_ROWS, options.maxArrayLength);
}

module.exports = {
    customInspectSymbol,
    inspectDataColumn,
    inspectDataColumnCollection,
    inspectDataRow,
    inspectDataRowCollection,
    inspectDataSet,
    inspectDataTable,
    inspectDataView
};
