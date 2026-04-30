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
        this.readOnly = options.readOnly ?? false;
        this.unique = options.unique ?? false;
        this.isPrimaryKey = options.primaryKey ?? options.isPrimaryKey ?? false;
        this._table = null;

        if (this.isPrimaryKey) {
            this.unique = true;
            this.allowNull = false;
        }
    }

    get table() {
        return this._table;
    }
}

module.exports = DataColumn;
