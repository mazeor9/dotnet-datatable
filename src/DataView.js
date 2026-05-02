const DataRowState = require('./enums/DataRowState');
const { cloneValues } = require('./utils/typeUtils');
const { DebugPreview, DebugTableSerializer, NodeInspectFormatter } = require('./debug');

class DataView {
    /**
     * @param {DataTable} table - Table to create view for
     * @param {Function|Object|string} [rowFilter=null] - Filter function, criteria object or simple string expression
     * @param {string|Function} [sort=null] - Sort expression or function
     * @param {string} [sortOrder='asc'] - Sort order ('asc' or 'desc')
     */
    constructor(table, rowFilter = null, sort = null, sortOrder = 'asc') {
        this._table = table;
        this._filters = [];
        this._sorts = [];
        this._skip = 0;
        this._take = null;

        if (rowFilter) {
            this.setFilter(rowFilter);
        }
        if (sort) {
            this.setSort(sort, sortOrder);
        }
    }

    setFilter(filter) {
        this._filters = [normalizeFilter(filter)];
        return this;
    }

    filter(filter) {
        return this.setFilter(filter);
    }

    where(columnName, operator, value) {
        this._filters.push(row => compareValues(row.get(columnName), operator, value));
        return this;
    }

    setSort(sort, order = 'asc') {
        this._sorts = normalizeSort(sort, order);
        return this;
    }

    sort(sort, order = 'asc') {
        return this.setSort(sort, order);
    }

    orderBy(columnName, direction = 'asc') {
        this._sorts = [{ columnName, direction: normalizeDirection(direction) }];
        return this;
    }

    take(count) {
        this._take = Math.max(0, Number(count) || 0);
        return this;
    }

    skip(count) {
        this._skip = Math.max(0, Number(count) || 0);
        return this;
    }

    getRows() {
        let rows = [...this._table.rows._rows];

        for (const filter of this._filters) {
            rows = rows.filter(row => filter(createRowProxy(row), row));
        }

        if (this._sorts.length > 0) {
            rows.sort((a, b) => compareRows(a, b, this._sorts));
        }

        if (this._skip > 0) {
            rows = rows.slice(this._skip);
        }
        if (this._take !== null) {
            rows = rows.slice(0, this._take);
        }

        return rows;
    }

    toTable() {
        const newTable = this._table.clone();
        newTable.clear();

        for (const sourceRow of this.getRows()) {
            const row = newTable.newRow();
            for (const column of this._table.columns) {
                row.set(column.columnName, sourceRow.get(column.columnName));
            }
            newTable.rows.add(row);
            row._setRowState(sourceRow.getRowState());
            row._originalValues = cloneValues(sourceRow._originalValues);
        }

        return newTable;
    }

    toDataTable() {
        return this.toTable();
    }

    toArray() {
        return this.toObjects();
    }

    toJSON() {
        return this.toArray();
    }

    toObjects(options = {}) {
        return this.getRows()
            .filter(row => options.includeDeleted === true || row.getRowState() !== DataRowState.DELETED)
            .map(row => {
                const result = {};
                for (const column of this._table.columns) {
                    result[column.columnName] = row.get(column.columnName);
                }
                return result;
            });
    }

    getPreview(maxRows = DebugPreview.DEFAULT_MAX_ROWS) {
        return DebugPreview.getViewPreview(this, maxRows);
    }

    toDebugView(options = {}) {
        return DebugTableSerializer.viewToDebugView(this, options);
    }

    [NodeInspectFormatter.customInspectSymbol](depth, options, inspect) {
        return NodeInspectFormatter.inspectDataView(this, depth, options, inspect);
    }

    get count() {
        return this.getRows().length;
    }

    get firstRow() {
        const rows = this.getRows();
        return rows.length > 0 ? rows[0] : null;
    }

    *[Symbol.iterator]() {
        yield* this.getRows();
    }

    row(index) {
        const rows = this.getRows();
        if (index < 0 || index >= rows.length) {
            throw new Error(`Index ${index} out of range [0, ${rows.length - 1}]`);
        }
        return rows[index];
    }
}

function createRowProxy(row) {
    return new Proxy(row, {
        get(target, prop) {
            if (prop in target) {
                return target[prop];
            }
            if (typeof prop === 'string' && target._table.columnExists(prop)) {
                return target.get(prop);
            }
            return undefined;
        }
    });
}

function normalizeFilter(filter) {
    if (typeof filter === 'function') {
        return (proxy, row) => filter(proxy, row);
    }
    if (typeof filter === 'string') {
        const predicates = parseFilterString(filter);
        return row => predicates.every(predicate => predicate(row));
    }
    if (filter && typeof filter === 'object') {
        return row => Object.entries(filter).every(([columnName, expected]) => {
            const current = row.get(columnName);
            if (expected && typeof expected === 'object' && !(expected instanceof Date) && !Array.isArray(expected)) {
                return Object.entries(expected).every(([operator, value]) => compareValues(current, operator, value));
            }
            return current === expected;
        });
    }
    return () => true;
}

function parseFilterString(expression) {
    return String(expression)
        .split(/\s+AND\s+/i)
        .map(part => part.trim())
        .filter(Boolean)
        .map(part => {
            const match = part.match(/^([A-Za-z_][\w.]*)\s*(===|!==|>=|<=|<>|!=|=|>|<)\s*(.+)$/);
            if (!match) {
                throw new Error('Only simple DataView filter expressions joined by AND are supported.');
            }
            const [, columnName, operator, rawValue] = match;
            const expected = parseLiteral(rawValue);
            return row => compareValues(row.get(columnName), operator, expected);
        });
}

function parseLiteral(value) {
    const trimmed = String(value).trim();
    if (/^'.*'$/.test(trimmed) || /^".*"$/.test(trimmed)) {
        return trimmed.slice(1, -1);
    }
    if (/^(true|false)$/i.test(trimmed)) {
        return trimmed.toLowerCase() === 'true';
    }
    if (/^null$/i.test(trimmed)) {
        return null;
    }
    if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
        return Number(trimmed);
    }
    return trimmed;
}

function normalizeSort(sort, order) {
    if (typeof sort === 'function') {
        return sort.length >= 2 ? [{ comparer: sort }] : [{ expression: sort, direction: normalizeDirection(order) }];
    }

    return String(sort)
        .split(',')
        .map(part => part.trim())
        .filter(Boolean)
        .map(part => {
            const [columnName, direction] = part.split(/\s+/);
            return {
                columnName,
                direction: normalizeDirection(direction || order)
            };
        });
}

function normalizeDirection(direction) {
    return String(direction || 'asc').toLowerCase() === 'desc' ? 'desc' : 'asc';
}

function compareRows(a, b, sorts) {
    for (const sort of sorts) {
        let result;
        if (sort.comparer) {
            result = sort.comparer(a, b);
        } else if (sort.expression) {
            result = compareScalar(sort.expression(a), sort.expression(b));
            if (sort.direction === 'desc') {
                result = -result;
            }
        } else {
            result = compareScalar(a.get(sort.columnName), b.get(sort.columnName));
            if (sort.direction === 'desc') {
                result = -result;
            }
        }
        if (result !== 0) {
            return result;
        }
    }
    return 0;
}

function compareScalar(a, b) {
    if (a === b) return 0;
    if (a === null || a === undefined) return 1;
    if (b === null || b === undefined) return -1;
    if (a instanceof Date && b instanceof Date) return a - b;
    if (typeof a === 'number' && typeof b === 'number') return a - b;
    return String(a).localeCompare(String(b));
}

function compareValues(current, operator, expected) {
    const op = normalizeOperator(operator);
    switch (op) {
        case '=':
            return current === expected;
        case '!=':
            return current !== expected;
        case '>':
            return current > expected;
        case '>=':
            return current >= expected;
        case '<':
            return current < expected;
        case '<=':
            return current <= expected;
        case 'contains':
            return String(current).includes(String(expected));
        case 'in':
            return Array.isArray(expected) && expected.includes(current);
        case 'startswith':
            return String(current).startsWith(String(expected));
        case 'endswith':
            return String(current).endsWith(String(expected));
        default:
            return false;
    }
}

function normalizeOperator(operator) {
    switch (String(operator).toLowerCase()) {
        case '==':
        case '===':
        case '=':
        case '$eq':
            return '=';
        case '!=':
        case '!==':
        case '<>':
        case '$ne':
            return '!=';
        case '$gt':
            return '>';
        case '$gte':
            return '>=';
        case '$lt':
            return '<';
        case '$lte':
            return '<=';
        case '$contains':
            return 'contains';
        case '$in':
            return 'in';
        default:
            return String(operator).toLowerCase();
    }
}

module.exports = DataView;
