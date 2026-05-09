const { DebugTableSerializer, NodeInspectFormatter } = require('./debug');
const { compileExpression } = require('./utils/expressionUtils');

class DataColumn {
    constructor(columnName, dataType = null, allowNullOrOptions = undefined, defaultValue = undefined) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.ordinal = -1;
        const options =
            typeof allowNullOrOptions === 'object' && allowNullOrOptions !== null
                ? allowNullOrOptions
                : {};
        const allowNull =
            typeof allowNullOrOptions === 'boolean'
                ? allowNullOrOptions
                : options.allowNull !== undefined
                    ? options.allowNull
                    : true;
        const resolvedDefaultValue =
            typeof allowNullOrOptions === 'boolean'
                ? defaultValue
                : options.defaultValue !== undefined
                    ? options.defaultValue
                    : null;

        this.allowNull = allowNull;
        this.defaultValue = resolvedDefaultValue;
        this.caption = columnName;
        this.expression = options.expression ?? null;
        if (typeof this.expression === 'string' && this.expression.trim() === '') {
            this.expression = null;
        }
        this._expressionEvaluator = null;
        if (typeof this.expression === 'function') {
            this._expressionEvaluator = this.expression;
        } else if (typeof this.expression === 'string') {
            this._expressionEvaluator = compileExpression(this.expression);
        }
        this.readOnly = options.readOnly ?? false;
        this.unique = options.unique ?? false;
        this.isPrimaryKey = options.primaryKey ?? options.isPrimaryKey ?? false;
        this.maxLength = options.maxLength ?? null;
        this.sourceColumn = options.sourceColumn ?? null;
        this.metadata = options.metadata ?? null;
        this._table = null;

        if (typeof this._expressionEvaluator === 'function') {
            this.readOnly = true;
        }

        if (this.isPrimaryKey) {
            this.unique = true;
            this.allowNull = false;
        }
    }

    get table() {
        return this._table;
    }

    get isComputed() {
        return typeof this._expressionEvaluator === 'function';
    }

    toJSON() {
        const { type, tableName, ...json } = DebugTableSerializer.columnToDebugView(this);
        return json;
    }

    toDebugView() {
        return DebugTableSerializer.columnToDebugView(this);
    }

    [NodeInspectFormatter.customInspectSymbol]() {
        return NodeInspectFormatter.inspectDataColumn(this);
    }
}

module.exports = DataColumn;
