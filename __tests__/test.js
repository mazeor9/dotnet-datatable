const assert = require('node:assert/strict');
const test = require('node:test');
const util = require('node:util');
const {
    DataSet,
    DataSetChangeSet,
    DataTable,
    DataTableChangeSet,
    DataRowState
} = require('../src');
const {
    ConstraintViolationError,
    DuplicatePrimaryKeyError,
    ReadOnlyColumnError,
    SchemaMismatchError
} = require('../src/errors');

function createUsersTable() {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { primaryKey: true });
    users.addColumn('name', 'string');
    users.addColumn('email', 'string');
    return users;
}

test('row lifecycle: DETACHED -> ADDED -> UNCHANGED -> MODIFIED -> DELETED', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { allowNull: false });

    const row = users.newRow();
    assert.equal(row.getRowState(), DataRowState.DETACHED);

    row.set('id', 1);
    assert.equal(row.getRowState(), DataRowState.DETACHED);

    users.rows.add(row);
    assert.equal(row.getRowState(), DataRowState.ADDED);

    row.acceptChanges();
    assert.equal(row.getRowState(), DataRowState.UNCHANGED);

    row.set('id', 2);
    assert.equal(row.getRowState(), DataRowState.MODIFIED);

    row.delete();
    assert.equal(row.getRowState(), DataRowState.DELETED);
});

test('acceptChanges: ADDED -> UNCHANGED', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { allowNull: false });

    const row = users.addRow({ id: 1 });
    assert.equal(row.getRowState(), DataRowState.ADDED);

    users.acceptChanges();
    assert.equal(row.getRowState(), DataRowState.UNCHANGED);
});

test('rejectChanges: MODIFIED restores original values', () => {
    const users = new DataTable('Users');
    users.addColumn('name', 'string', { allowNull: false });

    const row = users.addRow({ name: 'Alice' });
    users.acceptChanges();
    assert.equal(row.getRowState(), DataRowState.UNCHANGED);

    row.set('name', 'Bob');
    assert.equal(row.getRowState(), DataRowState.MODIFIED);

    row.rejectChanges();
    assert.equal(row.get('name'), 'Alice');
    assert.equal(row.getRowState(), DataRowState.UNCHANGED);
});

test('rejectChanges: ADDED detaches/removes the row', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { allowNull: false });

    const row = users.addRow({ id: 1 });
    assert.equal(users.rows.count, 1);

    row.rejectChanges();
    assert.equal(row.getRowState(), DataRowState.DETACHED);
    assert.equal(users.rows.count, 0);
});

test('allowNull: false throws on null', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { allowNull: false });

    assert.throws(() => users.addRow({ id: null }), ConstraintViolationError);
});

test('defaultValue applies to newRow (static)', () => {
    const users = new DataTable('Users');
    users.addColumn('age', 'number', { defaultValue: 10 });

    const row = users.newRow();
    assert.equal(row.get('age'), 10);
});

test('defaultValue applies to newRow (function)', () => {
    const users = new DataTable('Users');
    users.addColumn('createdAt', 'date', { defaultValue: () => new Date() });

    const a = users.newRow();
    const b = users.newRow();

    assert.ok(a.get('createdAt') instanceof Date);
    assert.ok(b.get('createdAt') instanceof Date);
    assert.notStrictEqual(a.get('createdAt'), b.get('createdAt'));
});

test('readOnly: true allows setting while DETACHED, blocks after add', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { readOnly: true, allowNull: false, defaultValue: 0 });

    const row = users.newRow();
    row.set('id', 1);
    users.rows.add(row);

    assert.throws(() => row.set('id', 2), ReadOnlyColumnError);
});

test('unique: true prevents duplicates', () => {
    const users = new DataTable('Users');
    users.addColumn('email', 'string', { unique: true });

    users.addRow({ email: 'a@test.com' });
    assert.throws(() => users.addRow({ email: 'a@test.com' }), ConstraintViolationError);
});

test('unique index ignores DELETED rows (reuse unique values after delete)', () => {
    const users = new DataTable('Users');
    users.addColumn('email', 'string', { unique: true });

    const row = users.addRow({ email: 'a@test.com' });
    users.acceptChanges();
    row.delete();

    users.addRow({ email: 'a@test.com' });
    assert.equal(users.rows.count, 2);
});

test('primary key index updates when changing PK values', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { primaryKey: true });
    users.addColumn('email', 'string');

    const a = users.addRow({ id: 1, email: 'a@test.com' });
    const b = users.addRow({ id: 2, email: 'b@test.com' });
    users.acceptChanges();

    assert.throws(() => b.set('id', 1), DuplicatePrimaryKeyError);
    assert.equal(a.get('id'), 1);
    assert.equal(b.get('id'), 2);
});

test('computed columns (expression function) are evaluated and read-only', () => {
    const table = new DataTable('T');
    table.addColumn('a', 'number');
    table.addColumn('b', 'number');
    table.addColumn('sum', 'number', {
        expression: (row) => (row.a ?? 0) + (row.b ?? 0)
    });

    const row = table.newRow();
    row.set('a', 1);
    row.set('b', 2);
    assert.equal(row.get('sum'), 3);
    assert.throws(() => row.set('sum', 10), ReadOnlyColumnError);

    table.rows.add(row);
    assert.equal(table.rows[0].get('sum'), 3);

    const objects = table.toObjects();
    assert.deepEqual(objects, [{ a: 1, b: 2, sum: 3 }]);
});

test('caseSensitive: false resolves column names case-insensitively (get/contains/loadRows)', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { primaryKey: true });
    users.addColumn('name', 'string');

    assert.equal(users.columnExists('ID'), true);
    assert.equal(users.columns.get('ID').columnName, 'id');

    users.loadRows([{ ID: 1, NAME: 'Mario' }], {
        clearBeforeLoad: true,
        autoCreateColumns: false,
        inferSchema: false
    });

    const row = users.find(1);
    assert.notEqual(row, null);
    assert.equal(row.get('name'), 'Mario');
    assert.equal(row.get('NAME'), 'Mario');
});

test('caseSensitive: true makes column lookup case-sensitive and allows same-name-different-case', () => {
    const table = new DataTable('T');
    table.caseSensitive = true;
    table.addColumn('Name', 'string');
    table.addColumn('name', 'string');

    assert.equal(table.columnExists('Name'), true);
    assert.equal(table.columnExists('name'), true);
});

test('caseSensitive toggle rebuilds column name index', () => {
    const table = new DataTable('T');
    table.addColumn('ID', 'number');

    assert.equal(table.columnExists('id'), true);

    table.caseSensitive = true;
    assert.equal(table.columnExists('id'), false);
    assert.equal(table.columnExists('ID'), true);

    table.caseSensitive = false;
    assert.equal(table.columnExists('id'), true);
});

test('DataTable.applyChangeSet applies added/modified/deleted by primary key', () => {
    const target = DataTable.fromObjects([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' }
    ], { tableName: 'Users', primaryKey: 'id' });

    const source = target.clone();
    source.find(1).set('name', 'Alice Updated');
    source.find(2).delete();
    source.addRow({ id: 3, name: 'Cara' });

    const changeSet = source.getChangeSet();
    const result = target.applyChangeSet(changeSet, { strict: true, missingRowAction: 'error', conflictPolicy: 'overwrite' });

    assert.equal(result.appliedAdded, 1);
    assert.equal(result.appliedModified, 1);
    assert.equal(result.appliedDeleted, 1);
    assert.equal(target.find(1).get('name'), 'Alice Updated');
    assert.equal(target.find(2), null);
    assert.equal(target.find(3).get('name'), 'Cara');
});

test('DataTable.applyChangeSet supports primary key changes via originalKey', () => {
    const target = DataTable.fromObjects([
        { id: 1, name: 'Alice' }
    ], { tableName: 'Users', primaryKey: 'id' });

    const source = target.clone();
    source.find(1).set('id', 10);
    source.find(10).set('name', 'Alice Moved');

    const changeSet = source.getChangeSet();
    target.applyChangeSet(changeSet, { strict: true, missingRowAction: 'error', conflictPolicy: 'overwrite' });

    assert.equal(target.find(1), null);
    assert.equal(target.find(10).get('name'), 'Alice Moved');
});

test('DataSet.applyChangeSet applies table changesets', () => {
    const target = new DataSet('CRM');
    const users = target.addTable('Users');
    users.addColumn('id', 'number', { primaryKey: true });
    users.addColumn('name', 'string');
    users.addRow({ id: 1, name: 'Mario' });
    users.acceptChanges();

    const source = target.clone();
    source.table('Users').find(1).set('name', 'Mario Updated');
    source.table('Users').addRow({ id: 2, name: 'Laura' });

    const changeSet = source.getChangeSet();
    const result = target.applyChangeSet(changeSet, { strict: true, missingTableAction: 'error', missingRowAction: 'error' });

    assert.equal(result.appliedTables.length, 1);
    assert.equal(target.table('Users').find(1).get('name'), 'Mario Updated');
    assert.equal(target.table('Users').find(2).get('name'), 'Laura');
});

test('primary key (single) prevents duplicates and supports find()', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { primaryKey: true });
    users.addColumn('email', 'string');

    users.addRow({ id: 1, email: 'a@test.com' });
    assert.throws(() => users.addRow({ id: 1, email: 'b@test.com' }), DuplicatePrimaryKeyError);

    const found = users.find(1);
    assert.notEqual(found, null);
    assert.equal(found.get('email'), 'a@test.com');
});

test('primary key (composite) prevents duplicates and supports find()', () => {
    const table = new DataTable('T');
    table.addColumn('a', 'number', { allowNull: false });
    table.addColumn('b', 'number', { allowNull: false });
    table.addColumn('v', 'string');
    table.setPrimaryKey(['a', 'b']);

    table.addRow({ a: 1, b: 1, v: 'x' });
    table.addRow({ a: 1, b: 2, v: 'y' });
    assert.throws(() => table.addRow({ a: 1, b: 1, v: 'z' }), DuplicatePrimaryKeyError);

    const found = table.find([1, 2]);
    assert.notEqual(found, null);
    assert.equal(found.get('v'), 'y');
});

test('find() ignores DELETED rows', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { primaryKey: true });
    users.addRow({ id: 1 });

    const row = users.find(1);
    row.delete();

    assert.equal(users.find(1), null);
});

test('addRow(object) and addRow(array)', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { allowNull: false });
    users.addColumn('name', 'string');

    const a = users.addRow({ id: 1, name: 'Alice' });
    const b = users.addRow([2, 'Bob']);

    assert.equal(users.rows.count, 2);
    assert.equal(a.get('name'), 'Alice');
    assert.equal(b.get('id'), 2);
    assert.equal(b.getRowState(), DataRowState.ADDED);
});

test('clone() copies schema including options and primary key', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'number', { allowNull: false, readOnly: true, defaultValue: 0, caption: 'ID' });
    users.addColumn('email', 'string', { unique: true, defaultValue: '' });
    users.addColumn('emailUpper', 'string', { expression: 'UPPER(email)' });
    users.setPrimaryKey(['id']);
    users.addRow({ id: 1, email: 'a@test.com' });

    const copy = users.clone();
    assert.equal(copy.tableName, 'Users');

    const idCol = copy.columns.get('id');
    assert.equal(idCol.allowNull, false);
    assert.equal(idCol.readOnly, true);
    assert.equal(idCol.defaultValue, 0);
    assert.equal(idCol.caption, 'ID');
    assert.equal(idCol.expression, null);

    const emailCol = copy.columns.get('email');
    assert.equal(emailCol.unique, true);
    assert.deepEqual(copy.getPrimaryKey(), ['id']);

    assert.equal(copy.rows.count, 1);
    assert.equal(copy.rows[0].get('email'), 'a@test.com');
    assert.equal(copy.rows[0].get('emailUpper'), 'A@TEST.COM');
});

test('DataTable.merge updates by primary key, inserts missing rows, and adds missing schema', () => {
    const users = createUsersTable();
    users.addRow({ id: 1, name: 'Alice', email: 'alice@old.test' });
    users.addRow({ id: 2, name: 'Bob', email: 'bob@old.test' });
    users.acceptChanges();
    users.find(2).set('name', 'Bobby Local');

    const source = createUsersTable();
    source.addColumn('role', 'string');
    source.addRow({ id: 1, name: 'Alice Remote', email: 'alice@new.test', role: 'admin' });
    source.addRow({ id: 2, name: 'Bob Remote', email: 'bob@new.test', role: 'user' });
    source.addRow({ id: 3, name: 'Cara', email: 'cara@new.test', role: 'guest' });

    const result = users.merge(source, {
        preserveChanges: true,
        missingSchemaAction: 'add'
    });

    assert.deepEqual(result.addedColumns, ['role']);
    assert.equal(result.updatedRows, 2);
    assert.equal(result.insertedRows, 1);
    assert.equal(users.rows.count, 3);
    assert.equal(users.find(1).get('name'), 'Alice Remote');
    assert.equal(users.find(2).get('name'), 'Bobby Local');
    assert.equal(users.find(2).get('email'), 'bob@new.test');
    assert.equal(users.find(2).get('role'), 'user');
    assert.equal(users.find(3).get('role'), 'guest');
});

test('DataTable.merge can ignore source columns missing from the target schema', () => {
    const users = createUsersTable();
    users.addRow({ id: 1, name: 'Alice', email: 'alice@old.test' });

    const source = createUsersTable();
    source.addColumn('role', 'string');
    source.addRow({ id: 1, name: 'Alice Remote', email: 'alice@new.test', role: 'admin' });
    source.addRow({ id: 2, name: 'Bob', email: 'bob@test.test', role: 'user' });

    const result = users.merge(source, { missingSchemaAction: 'ignore' });

    assert.deepEqual(result.ignoredColumns, ['role']);
    assert.equal(users.columnExists('role'), false);
    assert.equal(users.find(1).get('email'), 'alice@new.test');
    assert.equal(users.find(2).get('name'), 'Bob');
});

test('DataTable.merge throws when missing schema action is error', () => {
    const users = createUsersTable();
    const source = createUsersTable();
    source.addColumn('role', 'string');

    assert.throws(
        () => users.merge(source, { missingSchemaAction: 'error' }),
        SchemaMismatchError
    );
});

test('DataTable.merge supports composite primary keys', () => {
    const target = new DataTable('UserRoles');
    target.addColumn('userId', 'number', { allowNull: false });
    target.addColumn('roleId', 'number', { allowNull: false });
    target.addColumn('label', 'string');
    target.setPrimaryKey(['userId', 'roleId']);
    target.addRow({ userId: 1, roleId: 10, label: 'old' });

    const source = new DataTable('UserRoles');
    source.addColumn('userId', 'number', { allowNull: false });
    source.addColumn('roleId', 'number', { allowNull: false });
    source.addColumn('label', 'string');
    source.setPrimaryKey(['userId', 'roleId']);
    source.addRow({ userId: 1, roleId: 10, label: 'updated' });
    source.addRow({ userId: 1, roleId: 20, label: 'inserted' });

    const result = target.merge(source);

    assert.equal(result.updatedRows, 1);
    assert.equal(result.insertedRows, 1);
    assert.equal(target.find([1, 10]).get('label'), 'updated');
    assert.equal(target.find([1, 20]).get('label'), 'inserted');
});

test('DataSet.merge merges existing tables and adds missing tables and relations', () => {
    const target = new DataSet('Company');
    const targetUsers = target.addTable('Users');
    targetUsers.addColumn('id', 'number', { primaryKey: true });
    targetUsers.addColumn('departmentId', 'number');
    targetUsers.addColumn('name', 'string');
    targetUsers.addRow({ id: 1, departmentId: 1, name: 'Alice' });

    const source = new DataSet('Company');
    const sourceUsers = source.addTable('Users');
    sourceUsers.addColumn('id', 'number', { primaryKey: true });
    sourceUsers.addColumn('departmentId', 'number');
    sourceUsers.addColumn('name', 'string');
    sourceUsers.addRow({ id: 1, departmentId: 2, name: 'Alice Remote' });
    sourceUsers.addRow({ id: 2, departmentId: 1, name: 'Bob' });

    const departments = source.addTable('Departments');
    departments.addColumn('id', 'number', { primaryKey: true });
    departments.addColumn('name', 'string');
    departments.addRow({ id: 1, name: 'HR' });
    departments.addRow({ id: 2, name: 'IT' });

    source.addRelation('DepartmentUsers', 'Departments', 'Users', 'id', 'departmentId');

    const result = target.merge(source, { missingSchemaAction: 'add' });

    assert.equal(result.mergedTables.length, 1);
    assert.deepEqual(result.addedTables, ['Departments']);
    assert.deepEqual(result.relationsAdded, ['DepartmentUsers']);
    assert.equal(target.table('Users').find(1).get('name'), 'Alice Remote');
    assert.equal(target.table('Users').find(2).get('name'), 'Bob');
    assert.equal(target.table('Departments').rows.count, 2);
});

test('DataTable.fromObjects infers schema and imports rows as UNCHANGED with original values', () => {
    const createdAt = new Date('2026-01-01T10:00:00Z');
    const users = DataTable.fromObjects([
        { id: 1, name: 'Mario', active: true, createdAt, metadata: { tier: 'gold' } },
        { id: 2, name: 'Luca', active: false, createdAt: null, metadata: null }
    ], {
        tableName: 'Users',
        primaryKey: 'id'
    });

    assert.equal(users.tableName, 'Users');
    assert.equal(users.columns.get('id').dataType, 'integer');
    assert.equal(users.columns.get('active').dataType, 'boolean');
    assert.equal(users.columns.get('createdAt').dataType, 'date');
    assert.equal(users.rows.count, 2);
    assert.equal(users.rows[0].getRowState(), DataRowState.UNCHANGED);
    assert.equal(users.rows[0].originalValues.name, 'Mario');
    assert.deepEqual(users.getPrimaryKey(), ['id']);

    users.rows[0].set('name', 'Mario Rossi');
    assert.equal(users.rows[0].getRowState(), DataRowState.MODIFIED);
    assert.equal(users.rows[0].originalValues.name, 'Mario');
    assert.equal(users.rows[0].currentValues.name, 'Mario Rossi');
});

test('DataTable.fromQueryResult supports PostgreSQL-like results and field metadata', () => {
    const pgResult = {
        rows: [{ id: 1, name: 'Mario' }],
        fields: [{ name: 'id', dataTypeID: 23 }, { name: 'name', dataTypeID: 1043 }]
    };

    const users = DataTable.fromQueryResult(pgResult, {
        tableName: 'Users',
        primaryKey: 'id',
        useFieldMetadata: true
    });

    assert.equal(users.columns.get('id').dataType, 'integer');
    assert.equal(users.columns.get('name').dataType, 'string');
    assert.equal(users.columns.get('id').sourceColumn, 'id');
    assert.equal(users.findByPrimaryKey(1).get('name'), 'Mario');
    assert.equal(users.findByPrimaryKey(1).getRowState(), DataRowState.UNCHANGED);
});

test('DataTable.fromQueryResult supports MySQL-like tuple results', () => {
    const mysqlResult = [
        [{ id: 1, name: 'Mario' }],
        [{ name: 'id', columnType: 3 }, { name: 'name', columnType: 253 }]
    ];

    const users = DataTable.fromQueryResult(mysqlResult, {
        tableName: 'Users',
        primaryKey: 'id'
    });

    assert.equal(users.rows.count, 1);
    assert.equal(users.columns.get('id').dataType, 'integer');
    assert.equal(users.find(1).get('name'), 'Mario');
});

test('DataTable.fromQueryResult supports SQL Server-like and wrapper results', () => {
    const sqlServerResult = {
        recordset: [{ id: 1, name: 'Mario' }],
        recordsets: [[{ id: 1, name: 'Mario' }]],
        rowsAffected: [1]
    };

    const users = DataTable.fromQueryResult(sqlServerResult, {
        tableName: 'Users',
        primaryKey: 'id'
    });
    assert.equal(users.find(1).get('name'), 'Mario');

    const wrapped = DataTable.fromQueryResult({ data: { items: [{ id: 2, name: 'Luca' }] } }, {
        rowsPath: 'data.items',
        tableName: 'Users',
        primaryKey: 'id'
    });
    assert.equal(wrapped.find(2).get('name'), 'Luca');
});

test('DataTable.fromQueryResult supports postgres.js-style arrays with column metadata', () => {
    const postgresJsResult = [
        { id: 1, name: 'Mario' }
    ];
    postgresJsResult.columns = [
        { name: 'id', dataTypeID: 23 },
        { name: 'name', dataTypeID: 1043 }
    ];

    const users = DataTable.fromQueryResult(postgresJsResult, {
        tableName: 'Users',
        primaryKey: 'id',
        useFieldMetadata: true
    });

    assert.equal(users.columns.get('id').dataType, 'integer');
    assert.equal(users.columns.get('name').dataType, 'string');
    assert.equal(users.find(1).get('name'), 'Mario');
});

test('DataTable.fromQueryResult maps array rows with column metadata', () => {
    const sqliteResult = {
        rows: [
            [1, 'Mario'],
            [2, 'Laura']
        ],
        columns: [
            { name: 'id', type: 'INTEGER' },
            { name: 'name', type: 'TEXT' }
        ]
    };

    const users = DataTable.fromQueryResult(sqliteResult, {
        tableName: 'Users',
        primaryKey: 'id',
        provider: 'sqlite',
        useFieldMetadata: true
    });

    assert.equal(users.columns.get('id').dataType, 'integer');
    assert.equal(users.columns.get('name').dataType, 'string');
    assert.equal(users.rows.count, 2);
    assert.equal(users.find(2).get('name'), 'Laura');
});

test('DataTable.fromQueryResult supports Oracle-style metadata and uppercase column transforms', () => {
    const oracleResult = {
        rows: [
            [1, 'Mario']
        ],
        metaData: [
            { name: 'ID', dbTypeName: 'NUMBER', nullable: false },
            { name: 'FULL_NAME', dbTypeName: 'VARCHAR2', nullable: true }
        ]
    };

    const users = DataTable.fromQueryResult(oracleResult, {
        tableName: 'Users',
        primaryKey: 'id',
        columnNameTransform: 'camelCase',
        useFieldMetadata: true
    });

    assert.equal(users.columnExists('id'), true);
    assert.equal(users.columnExists('fullName'), true);
    assert.equal(users.columns.get('id').dataType, 'number');
    assert.equal(users.columns.get('fullName').dataType, 'string');
    assert.equal(users.find(1).get('fullName'), 'Mario');
});

test('DataTable.fromRows applies include, exclude, rename and columnNameTransform options', () => {
    const users = DataTable.fromRows([
        { user_id: 1, full_name: 'Mario Rossi', ignored_value: 'x', created_at: '2026-01-01T00:00:00Z' }
    ], {
        tableName: 'Users',
        renameColumns: {
            user_id: 'id',
            full_name: 'full_name'
        },
        columnNameTransform: 'camelCase',
        excludeColumns: ['ignored_value'],
        columns: {
            created_at: { type: 'date' }
        },
        primaryKey: 'id'
    });

    assert.equal(users.columnExists('id'), true);
    assert.equal(users.columnExists('fullName'), true);
    assert.equal(users.columnExists('ignoredValue'), false);
    assert.ok(users.rows[0].get('createdAt') instanceof Date);
    assert.equal(users.find(1).get('fullName'), 'Mario Rossi');
});

test('DataTable.loadRows clears, appends, creates missing columns and can ignore extras', () => {
    const users = new DataTable('Users');
    users.addColumn('id', 'integer', { primaryKey: true });
    users.addColumn('name', 'string');

    users.loadRows([{ id: 1, name: 'Mario' }], { clearBeforeLoad: true });
    assert.equal(users.rows.count, 1);
    assert.equal(users.rows[0].getRowState(), DataRowState.UNCHANGED);

    users.loadRows([{ id: 2, name: 'Luca', active: true }], {
        autoCreateColumns: true
    });
    assert.equal(users.columnExists('active'), true);
    assert.equal(users.rows.count, 2);

    users.loadRows([{ id: 3, name: 'Anna', extra: 'ignored' }], {
        autoCreateColumns: false,
        ignoreExtraColumns: true
    });
    assert.equal(users.rows.count, 3);
    assert.equal(users.columnExists('extra'), false);

    users.loadRows([{ id: 4, name: 'Nina' }], {
        append: false,
        autoCreateColumns: false
    });
    assert.equal(users.rows.count, 1);
    assert.equal(users.find(4).get('name'), 'Nina');
});

test('DataTable.mergeRows updates existing rows and inserts missing rows by primary key', () => {
    const users = DataTable.fromObjects([
        { id: 1, name: 'Mario' },
        { id: 2, name: 'Luca' }
    ], {
        tableName: 'Users',
        primaryKey: 'id'
    });

    const result = users.mergeRows([
        { id: 1, name: 'Mario DB' },
        { id: 3, name: 'Anna' }
    ], {
        primaryKey: 'id',
        updateExisting: true,
        addMissing: true,
        markModified: false
    });

    assert.equal(result.updatedRows, 1);
    assert.equal(result.insertedRows, 1);
    assert.equal(users.find(1).get('name'), 'Mario DB');
    assert.equal(users.find(1).getRowState(), DataRowState.UNCHANGED);
    assert.equal(users.find(3).get('name'), 'Anna');
});

test('DataTable exports objects and tracks Added, Modified, Deleted changes', () => {
    const users = DataTable.fromObjects([{ id: 1, name: 'Mario', createdAt: new Date('2026-01-01T00:00:00Z') }], {
        tableName: 'Users',
        primaryKey: 'id'
    });

    users.find(1).set('name', 'Mario Rossi');
    const added = users.addRow({ id: 2, name: 'Luca', createdAt: new Date('2026-01-02T00:00:00Z') });
    users.find(1).delete();

    assert.equal(added.getRowState(), DataRowState.ADDED);
    assert.equal(users.getChanges().length, 2);
    assert.equal(users.getChanges('Added').length, 1);
    assert.equal(users.getChanges('Deleted').length, 1);

    const exported = users.toObjects({
        includeDeleted: true,
        includeRowState: true,
        includeOriginalValues: true,
        dateMode: 'iso-string',
        columnNameMapping: { createdAt: 'created_at' }
    });
    assert.equal(exported[0].rowState, DataRowState.DELETED);
    assert.equal(exported[0].originalValues.name, 'Mario');
    assert.equal(exported[0].created_at, '2026-01-01T00:00:00.000Z');

    users.rejectChanges();
    assert.equal(users.rows.count, 1);
    assert.equal(users.find(1).get('name'), 'Mario');

    users.find(1).set('name', 'Mario Rossi');
    users.acceptChanges();
    assert.equal(users.find(1).getRowState(), DataRowState.UNCHANGED);
    assert.equal(users.find(1).originalValues.name, 'Mario Rossi');
});

test('DataView supports where, filter, orderBy, take, skip and toDataTable', () => {
    const users = DataTable.fromObjects([
        { id: 1, name: 'Mario', age: 31, active: true },
        { id: 2, name: 'Luca', age: 17, active: true },
        { id: 3, name: 'Anna', age: 44, active: true },
        { id: 4, name: 'Paolo', age: 28, active: false }
    ], {
        tableName: 'Users',
        primaryKey: 'id'
    });

    const activeAdults = users
        .createView()
        .filter(row => row.active === true)
        .where('age', '>', 18)
        .orderBy('name', 'asc')
        .skip(1)
        .take(1)
        .toObjects();

    assert.deepEqual(activeAdults.map(row => row.name), ['Mario']);

    const stringFiltered = users.createView({
        filter: "age >= 18 AND active = true",
        sort: 'age DESC'
    });
    assert.deepEqual(stringFiltered.toObjects().map(row => row.name), ['Anna', 'Mario']);
    assert.equal(stringFiltered.toDataTable().rows.count, 2);
});

test('DataSet.fromRecordsets and DataRelation navigate parent and child rows', () => {
    const dataSet = DataSet.fromRecordsets([
        [{ id: 1, name: 'Mario' }],
        [
            { id: 10, user_id: 1, total: 100 },
            { id: 11, user_id: 1, total: 200 }
        ]
    ], {
        tableNames: ['Users', 'Orders'],
        relations: [
            {
                name: 'UserOrders',
                parentTable: 'Users',
                parentColumn: 'id',
                childTable: 'Orders',
                childColumn: 'user_id'
            }
        ]
    });

    const relation = dataSet.getRelation('UserOrders');
    const user = dataSet.table('Users').rows[0];
    const orders = relation.getChildRows(user);
    assert.equal(orders.length, 2);
    assert.equal(relation.getParentRow(orders[0]).get('name'), 'Mario');
});

test('DataTable.toDebugView returns a stable debug payload', () => {
    const users = createUsersTable();
    users.addRow({ id: 1, name: 'Mario', email: 'mario@test.it' });
    users.addRow({ id: 2, name: 'Laura', email: 'laura@test.it' });

    const debugView = users.toDebugView();

    assert.equal(debugView.type, 'DataTable');
    assert.equal(debugView.name, 'Users');
    assert.equal(debugView.rowCount, 2);
    assert.equal(debugView.columnCount, 3);
    assert.deepEqual(debugView.primaryKey, ['id']);
    assert.deepEqual(debugView.rows[0], { id: 1, name: 'Mario', email: 'mario@test.it' });
    assert.deepEqual(debugView.preview.map(row => row.name), ['Mario', 'Laura']);
});

test('DataTable.getSchema returns column metadata for inspection', () => {
    const users = createUsersTable();
    const schema = users.getSchema();

    assert.equal(schema.type, 'DataTableSchema');
    assert.equal(schema.name, 'Users');
    assert.equal(schema.columnCount, 3);
    assert.deepEqual(schema.primaryKey, ['id']);
    assert.deepEqual(
        schema.columns.map(column => ({ name: column.name, dataType: column.dataType, primaryKey: column.primaryKey })),
        [
            { name: 'id', dataType: 'number', primaryKey: true },
            { name: 'name', dataType: 'string', primaryKey: false },
            { name: 'email', dataType: 'string', primaryKey: false }
        ]
    );
});

test('DataTable.getPreview limits rows and toConsoleTable returns plain objects', () => {
    const users = createUsersTable();
    users.addRow({ id: 1, name: 'Mario', email: 'mario@test.it' });
    users.addRow({ id: 2, name: 'Laura', email: 'laura@test.it' });
    users.addRow({ id: 3, name: 'Nina', email: 'nina@test.it' });

    assert.deepEqual(users.getPreview(2).map(row => row.id), [1, 2]);
    assert.deepEqual(users.getPreview(0), []);

    const consoleRows = users.toConsoleTable();
    assert.deepEqual(consoleRows[2], { id: 3, name: 'Nina', email: 'nina@test.it' });
});

test('DataRow.toObject and toDebugView expose values without internal fields', () => {
    const users = createUsersTable();
    const row = users.addRow({ id: 1, name: 'Mario', email: 'mario@test.it' });

    assert.deepEqual(row.toObject(), { id: 1, name: 'Mario', email: 'mario@test.it' });

    const debugView = row.toDebugView();
    assert.equal(debugView.type, 'DataRow');
    assert.equal(debugView.tableName, 'Users');
    assert.equal(debugView.rowState, DataRowState.ADDED);
    assert.deepEqual(debugView.values, { id: 1, name: 'Mario', email: 'mario@test.it' });
});

test('DataView.toDebugView and getPreview describe filtered and sorted rows', () => {
    const users = DataTable.fromObjects([
        { id: 1, name: 'Mario', age: 31 },
        { id: 2, name: 'Laura', age: 26 },
        { id: 3, name: 'Nina', age: 17 }
    ], {
        tableName: 'Users',
        primaryKey: 'id'
    });

    const view = users.createView()
        .where('age', '>=', 18)
        .orderBy('name', 'desc');

    const debugView = view.toDebugView();
    assert.equal(debugView.type, 'DataView');
    assert.equal(debugView.sourceTable, 'Users');
    assert.equal(debugView.rowCount, 2);
    assert.equal(debugView.filter, '1 filter');
    assert.equal(debugView.sort, 'name desc');
    assert.deepEqual(debugView.rows.map(row => row.name), ['Mario', 'Laura']);
    assert.deepEqual(view.getPreview(1), [{ id: 1, name: 'Mario', age: 31 }]);
});

test('DataSet.toDebugView and getSchema expose tables and relations', () => {
    const dataSet = new DataSet('Company');
    const users = dataSet.addTable(createUsersTable());
    users.addRow({ id: 1, name: 'Mario', email: 'mario@test.it' });

    const orders = dataSet.addTable('Orders');
    orders.addColumn('id', 'number', { primaryKey: true });
    orders.addColumn('userId', 'number');
    orders.addRow({ id: 10, userId: 1 });

    dataSet.addRelation('UserOrders', 'Users', 'Orders', 'id', 'userId');

    const debugView = dataSet.toDebugView();
    assert.equal(debugView.type, 'DataSet');
    assert.equal(debugView.name, 'Company');
    assert.equal(debugView.tableCount, 2);
    assert.deepEqual(debugView.tables.map(table => table.name), ['Users', 'Orders']);
    assert.deepEqual(debugView.relations[0], {
        name: 'UserOrders',
        parentTable: 'Users',
        parentColumn: 'id',
        childTable: 'Orders',
        childColumn: 'userId'
    });

    const schema = dataSet.getSchema();
    assert.equal(schema.type, 'DataSetSchema');
    assert.equal(schema.tableCount, 2);
});

test('custom inspect formats DataTable, DataRow and DataSet for Node.js consoles', () => {
    const users = createUsersTable();
    const row = users.addRow({ id: 1, name: 'Mario', email: 'mario@test.it' });
    const dataSet = new DataSet('DebugSet');
    dataSet.addTable(users);

    const tableInspect = util.inspect(users);
    assert.match(tableInspect, /DataTable "Users"/);
    assert.match(tableInspect, /Columns: \[id:number, name:string, email:string\]/);
    assert.match(tableInspect, /Preview:/);

    const rowInspect = util.inspect(row);
    assert.match(rowInspect, /DataRow "Users"/);
    assert.match(rowInspect, /RowState: ADDED/);

    const dataSetInspect = util.inspect(dataSet);
    assert.match(dataSetInspect, /DataSet "DebugSet"/);
    assert.match(dataSetInspect, /Users \(1 rows, 3 columns\)/);
});

test('DataTableChangeSet groups added, modified and deleted rows', () => {
    const users = DataTable.fromObjects([
        { id: 1, name: 'Mario', active: true },
        { id: 2, name: 'Laura', active: true }
    ], {
        tableName: 'Users',
        primaryKey: 'id'
    });

    users.find(1).set('name', 'Mario Rossi');
    users.addRow({ id: 3, name: 'Nina', active: false });
    users.find(2).delete();

    const changeSet = DataTableChangeSet.fromTable(users);

    assert.equal(changeSet.tableName, 'Users');
    assert.deepEqual(changeSet.primaryKey, ['id']);
    assert.equal(changeSet.count, 3);
    assert.equal(changeSet.hasChanges, true);
    assert.deepEqual(changeSet.added.map(change => change.values.id), [3]);
    assert.deepEqual(changeSet.modified[0].changedColumns, ['name']);
    assert.deepEqual(changeSet.modified[0].originalValues.name, 'Mario');
    assert.deepEqual(changeSet.deleted[0].key, { id: 2 });
    assert.equal(changeSet.toObject().hasChanges, true);
    assert.equal(users.getChangeSet().count, 3);
});

test('DataSetChangeSet collects changed tables only by default', () => {
    const dataSet = new DataSet('Company');
    const users = dataSet.addTable('Users');
    users.addColumn('id', 'number', { primaryKey: true });
    users.addColumn('name', 'string');
    users.addRow({ id: 1, name: 'Mario' });
    users.acceptChanges();
    users.find(1).set('name', 'Mario Rossi');

    const departments = dataSet.addTable('Departments');
    departments.addColumn('id', 'number', { primaryKey: true });
    departments.addColumn('name', 'string');
    departments.addRow({ id: 10, name: 'IT' });
    departments.acceptChanges();

    const changeSet = DataSetChangeSet.fromDataSet(dataSet);

    assert.equal(changeSet.dataSetName, 'Company');
    assert.equal(changeSet.count, 1);
    assert.deepEqual(changeSet.tables.map(table => table.tableName), ['Users']);
    assert.equal(changeSet.table('Departments'), null);
});

test('DataRow beginEdit/endEdit/cancelEdit and RowVersion access', () => {
    const t = new DataTable('T');
    t.addColumn('id', 'number', { allowNull: false });
    t.addColumn('value', 'number');
    t.setPrimaryKey('id');

    const r = t.addRow({ id: 1, value: 10 });
    r.acceptChanges();
    assert.equal(r.getRowState(), DataRowState.UNCHANGED);

    r.beginEdit();
    r.set('value', 20);
    assert.equal(r.get('value', 'current'), 10);
    assert.equal(r.get('value', 'proposed'), 20);
    assert.equal(r.get('value'), 20);

    r.cancelEdit();
    assert.equal(r.get('value'), 10);
    assert.equal(r.getRowState(), DataRowState.UNCHANGED);

    r.beginEdit();
    r.set('value', 30);
    r.endEdit();
    assert.equal(r.get('value'), 30);
    assert.equal(r.getRowState(), DataRowState.MODIFIED);
    assert.equal(r.get('value', 'original'), 10);
});

test('UniqueConstraint (multi-column) prevents duplicates', () => {
    const t = new DataTable('Pairs');
    t.addColumn('a', 'number', { allowNull: false });
    t.addColumn('b', 'number', { allowNull: false });
    t.addUniqueConstraint(['a', 'b'], 'UQ_ab');

    t.addRow({ a: 1, b: 1 });
    assert.throws(() => t.addRow({ a: 1, b: 1 }));
    t.addRow({ a: 1, b: 2 });
});

test('ForeignKeyConstraint cascade delete deletes child rows', () => {
    const ds = new DataSet('DS');
    const parents = ds.addTable(DataTable.fromObjects([{ id: 1 }], { tableName: 'Parents', primaryKey: 'id' }));
    const children = ds.addTable(DataTable.fromObjects([{ id: 10, parentId: 1 }], { tableName: 'Children', primaryKey: 'id' }));

    const rel = ds.addRelation('FK_Parents_Children', parents.columns.get('id'), children.columns.get('parentId'));
    ds.addForeignKeyConstraint(rel.relationName, { deleteRule: 'cascade' });

    const p = parents.find(1);
    const c = children.find(10);
    assert.equal(c.getRowState(), DataRowState.UNCHANGED);
    p.delete();
    assert.equal(p.getRowState(), DataRowState.DELETED);
    assert.equal(c.getRowState(), DataRowState.DELETED);
});

test('DataView filter supports OR and parentheses via expression engine', () => {
    const t = DataTable.fromObjects([
        { id: 1, name: 'A' },
        { id: 2, name: 'B' },
        { id: 3, name: 'C' }
    ], {
        tableName: 'T',
        primaryKey: 'id'
    });

    const rows = t.createView({ filter: "(id > 2) OR (name = 'A')" }).getRows();
    const ids = rows.map((r) => r.get('id')).sort();
    assert.deepEqual(ids, [1, 3]);
});

test('serialize/deserialize roundtrip preserves schema and row states', () => {
    const t = DataTable.fromObjects([
        { id: 1, name: 'A' },
        { id: 2, name: 'B' }
    ], { tableName: 'Users', primaryKey: 'id' });

    t.find(1).set('name', 'A2');
    t.find(2).delete();

    const payload = t.serialize({ asObject: true });
    const copy = DataTable.deserialize(payload);

    assert.equal(copy.tableName, 'Users');
    assert.deepEqual(copy.getPrimaryKey(), ['id']);
    assert.equal(copy.find(1).get('name'), 'A2');
    assert.equal(copy.find(2), null);
    assert.equal(copy.getChanges().length, 2);
});

test('join() supports inner join', () => {
    const a = DataTable.fromObjects([{ id: 1, a: 'x' }, { id: 2, a: 'y' }], { tableName: 'A', primaryKey: 'id' });
    const b = DataTable.fromObjects([{ id: 2, b: 'z' }], { tableName: 'B', primaryKey: 'id' });

    const joined = a.join(b, { on: 'id', type: 'inner' });
    assert.equal(joined.rows.count, 1);
    assert.equal(joined.rows[0].get('id'), 2);
    assert.equal(joined.rows[0].get('a'), 'y');
    assert.equal(joined.rows[0].get('b'), 'z');
});
