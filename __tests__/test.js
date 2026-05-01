const assert = require('node:assert/strict');
const test = require('node:test');
const { DataSet, DataTable, DataRowState } = require('../src');
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
    users.addColumn('id', 'number', { allowNull: false, readOnly: true, defaultValue: 0, caption: 'ID', expression: 'id' });
    users.addColumn('email', 'string', { unique: true, defaultValue: '' });
    users.setPrimaryKey(['id']);
    users.addRow({ id: 1, email: 'a@test.com' });

    const copy = users.clone();
    assert.equal(copy.tableName, 'Users');

    const idCol = copy.columns.get('id');
    assert.equal(idCol.allowNull, false);
    assert.equal(idCol.readOnly, true);
    assert.equal(idCol.defaultValue, 0);
    assert.equal(idCol.caption, 'ID');
    assert.equal(idCol.expression, 'id');

    const emailCol = copy.columns.get('email');
    assert.equal(emailCol.unique, true);
    assert.deepEqual(copy.getPrimaryKey(), ['id']);

    assert.equal(copy.rows.count, 1);
    assert.equal(copy.rows[0].get('email'), 'a@test.com');
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
