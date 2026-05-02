# dotnet-datatable

A .NET-style DataTable library for JavaScript and Node.js. Build typed in-memory tables with DataRow access, DataView filtering and sorting, DataSet relationships, row state tracking, schema validation, merge operations and query result mapping from any database client.

<div align="center">
  <p>
    <a href="https://www.npmjs.com/package/dotnet-datatable">
      <img src="https://img.shields.io/npm/v/dotnet-datatable.svg?logo=npm&style=for-the-badge&color=CB3837" alt="NPM Version" />
    </a>
    <a href="https://www.npmjs.com/package/dotnet-datatable">
      <img src="https://img.shields.io/npm/dt/dotnet-datatable.svg?style=for-the-badge&color=CB3837" alt="NPM Downloads" />
    </a>
    <a href="https://github.com/mazeor9/dotnet-datatable/releases/latest">
      <img src="https://img.shields.io/github/v/release/mazeor9/dotnet-datatable?style=for-the-badge&color=green" alt="GitHub Release" />
    </a>
    <a href="https://github.com/mazeor9/dotnet-datatable/blob/main/LICENSE">
      <img src="https://img.shields.io/github/license/mazeor9/dotnet-datatable?style=for-the-badge&color=blue" alt="License" />
    </a>
    <a href="https://github.com/mazeor9/dotnet-datatable">
      <img src="https://img.shields.io/github/stars/mazeor9/dotnet-datatable?style=for-the-badge&color=yellow" alt="GitHub Stars" />
    </a>
    <a href="https://bundlephobia.com/package/dotnet-datatable">
      <img src="https://img.shields.io/bundlephobia/minzip/dotnet-datatable?style=for-the-badge&color=orange" alt="Bundle Size" />
    </a>
  </p>
</div>

## Features

- Strongly typed columns
- Row management
- Sorting and filtering
- Data validation
- Query result loading from plain objects and common SQL client result shapes
- Database result mapping without database drivers or connections
- Schema inference and original value tracking
- Enterprise-style merge for DataTable and DataSet

## Table of Contents

- [Why dotnet-datatable?](#why-dotnet-datatable)
- [Installation](#installation)
- [CSS Themes](#css-themes)
- [Basic Usage](#basic-usage)
- [Debugging & Inspection](#debugging--inspection)
- [Database Integration](#database-integration)
- [Features](#features)
- [Methods](#methods)
  - [Table Operations](#table-operations)
  - [Row Operations](#row-operations)
  - [Row State Management](#row-state-management)
  - [Data Operations](#data-operations)
  - [Query Result Loading](#query-result-loading)
  - [Merge Operations](#merge-operations)
  - [Advanced Filtering](#advanced-filtering-criteria)
  - [Column Operations](#column-operations)
  - [Table Manipulation](#table-manipulation)
  - [DataSet Operations](#dataset-operations)
  - [DataView Operations](#dataview-operations)
  - [Schema Management](#advanced-schema-management)
- [Supported Data Types](#supported-data-types)
- [Advanced Database Usage](#advanced-database-usage)
- [Error Handling](#error-handling)
- [License](#license)

## Installation

```bash
npm install dotnet-datatable
```

## CSS Themes

The package includes optional CSS presets aligned with typical ASP.NET-era and modern .NET UI styling approaches (2014+). These styles target HTML tables when you add the `dotnet-datatable` class (or `aspnet-gridview` / `aspnet-datagrid` / `aspnet-webdatagrid`).

Import one theme:

```javascript
import 'dotnet-datatable/css/aspnetcore-bootstrap5.css';
```

Available themes:

- `dotnet-datatable/css/webforms-2014.css` (ASP.NET WebForms 2014+)
- `dotnet-datatable/css/webdatagrid-2014.css` (WebDataGrid 2014+)
- `dotnet-datatable/css/mvc-bootstrap3.css` (ASP.NET MVC, Bootstrap 3-era)
- `dotnet-datatable/css/aspnetcore-bootstrap5.css` (ASP.NET Core, Bootstrap 5-era)
- `dotnet-datatable/css/blazor-fluentui.css` (Blazor, Fluent-like)

Minimal markup example:

```html
<table class="dotnet-datatable">
  <thead>
    <tr>
      <th class="ddt-sort-asc">Name</th>
      <th>Age</th>
    </tr>
  </thead>
  <tbody>
    <tr><td>Alice</td><td>35</td></tr>
    <tr class="ddt-row-selected"><td>John</td><td>25</td></tr>
  </tbody>
</table>
```

## Basic Usage

```javascript
// Destructuring
const { DataTable } = require('dotnet-datatable');

// Direct import
const DataTable = require('dotnet-datatable').DataTable;

// Create a new table
const dt = new DataTable('Users');
```

## Debugging & Inspection

DataTable, DataRow, DataColumn, DataView and DataSet expose debug-friendly serializers and Node.js custom inspect output. This makes `console.log()`, `console.dir()`, `util.inspect()`, the VS Code Debug Console and Watch panels much easier to read without requiring an IDE extension.

```javascript
const { DataSet, DataTable } = require('dotnet-datatable');

const customers = new DataTable('Customers');
customers.addColumn('ID', 'number', { primaryKey: true });
customers.addColumn('Name', 'string');
customers.addColumn('Email', 'string');
customers.addRow({ ID: 1, Name: 'Mario', Email: 'mario@test.it' });
customers.addRow({ ID: 2, Name: 'Laura', Email: 'laura@test.it' });

// Uses Node.js custom inspect under the hood.
console.log(customers);

// Array of plain objects, ready for console.table().
console.table(customers.toConsoleTable());

// Stable object payload for custom debug tools or future IDE visualizers.
console.dir(customers.toDebugView(), { depth: null });

// Schema-only view.
console.dir(customers.getSchema(), { depth: null });

// Small preview for large tables. Default is 10 rows.
console.table(customers.getPreview(5));
```

Example DataSet and DataView inspection:

```javascript
const dataSet = new DataSet('CRM');
dataSet.addTable(customers);

console.dir(dataSet.toDebugView(), { depth: null });

const activeCustomers = customers
  .createView()
  .where('ID', '>=', 1)
  .orderBy('Name', 'asc');

console.table(activeCustomers.getPreview(5));
console.dir(activeCustomers.toDebugView(), { depth: null });
```

Useful methods:

- `dataTable.toArray()` returns row objects using the existing export behavior.
- `dataTable.toDebugView()` returns `{ type, name, columns, rows, rowCount, columnCount, preview }`.
- `dataTable.toDebugString()` returns the same readable text used by custom inspect.
- `dataTable.toConsoleTable()` returns plain objects for `console.table()`.
- `dataRow.toObject()` returns a plain object with the row values.
- `dataView.toDebugView()` includes source table, rows, row count, sort/filter summary and preview.
- `dataSet.toDebugView()` includes all table debug views and relation metadata.

## Database Integration
`dotnet-datatable` does not connect to databases, execute SQL, or act as an ORM. Use any external client to run your query, then load the returned rows or query result object into a DataTable.

Supported input shapes include:
- Plain arrays of objects, such as Prisma, Knex, SQLite and Drizzle results
- PostgreSQL `pg` results: `{ rows: [...], fields: [...] }`
- MySQL `mysql2` results: `[rows, fields]`
- SQL Server `mssql` results: `{ recordset: [...], recordsets: [...] }`
- Sequelize model instances or plain objects
- Custom wrappers with `rowsPath`, such as `{ data: { items: [...] } }`

```javascript
// PostgreSQL
const { Pool } = require('pg');
const pool = new Pool(config);

const result = await pool.query('SELECT * FROM products');
const products = DataTable.fromQueryResult(result, {
    tableName: 'Products',
    primaryKey: 'id',
    useFieldMetadata: true
});

// MySQL
const mysql = require('mysql2/promise');
const connection = await mysql.createConnection(config);
const mysqlResult = await connection.execute('SELECT * FROM products');

const mysqlProducts = DataTable.fromQueryResult(mysqlResult, {
    tableName: 'Products',
    primaryKey: 'id'
});

// Prisma, Knex, SQLite, Drizzle or any plain object rows
const rows = await prisma.product.findMany();
const prismaProducts = DataTable.fromObjects(rows, {
    tableName: 'Products',
    primaryKey: 'id'
});
```

## Features

- Strongly typed columns
- Row management
- Sorting and filtering
- Data validation
- Query result loading
- Database integration
- Enterprise-style merge for DataTable and DataSet
- DataSet support for related tables 
- DataView for filtered views of tables
- Advanced schema management and serialization

## Methods

### Table Operations

#### Creating a Table
```javascript
const dt = new DataTable('TableName');
```

#### Adding Columns
```javascript
// Add a column with type
dt.addColumn('age', 'number');
dt.addColumn('name', 'string');
dt.addColumn('birthDate', 'date');

// Add column without type
dt.addColumn('description');
```

Column options (constraints and defaults):

```javascript
dt.addColumn('id', 'number', { primaryKey: true, allowNull: false, readOnly: true, defaultValue: 0 });
dt.addColumn('email', 'string', { unique: true });
dt.addColumn('createdAt', 'date', { defaultValue: () => new Date() });
```

Read-only columns can be set while the row is `DETACHED`:

```javascript
const row = dt.newRow();
row.set('id', 1);     // ok (DETACHED)
dt.rows.add(row);     // now ADDED
row.set('id', 2);     // throws (read-only)
```

#### Adding Rows
```javascript
// Add row with object
dt.addRow({ name: 'John', age: 30, birthDate: new Date('1993-01-01') });

// Add row with array
dt.addRow(['Jane', 25, new Date('1998-01-01')]);

// Create and add row manually
const row = dt.newRow();
row.set('name', 'Alice');
row.set('age', 28);
dt.rows.add(row);
```

Primary key, unique and find:

```javascript
const users = new DataTable('Users');
users.addColumn('id', 'number', { primaryKey: true });
users.addColumn('email', 'string', { unique: true });

users.addRow({ id: 1, email: 'a@test.com' });
users.addRow({ id: 2, email: 'b@test.com' });

const u1 = users.find(1);
console.log(u1.get('email')); // 'a@test.com'
```

Composite primary key:

```javascript
const sessions = new DataTable('Sessions');
sessions.addColumn('tenantId', 'number', { allowNull: false });
sessions.addColumn('userId', 'number', { allowNull: false });
sessions.addColumn('token', 'string');

sessions.setPrimaryKey(['tenantId', 'userId']);

sessions.addRow({ tenantId: 1, userId: 10, token: 'x' });
const s = sessions.find([1, 10]);
console.log(s.get('token')); // 'x'
```

### Row Operations
```javascript
// Get value using row index and column name (recommended method)
const value = dt.rows(0).get("value");  // Gets value from first row, column "value"
const name = dt.rows(1).get("name");    // Gets value from second row, column "name"

// Alternative ways to get values
const name = row.get('name');    // Recommended
const age = row.item('age');     // Supported for backwards compatibility

// Set value
row.set('name', 'NewName');

// Remove row
dt.removeRow(0);

// Clear all rows
dt.clear();
```

### Row State Management
dotnet-datatable provides comprehensive row state tracking to monitor changes to your data. Each row has a state that indicates whether it has been added, modified, deleted, or remains unchanged.

#### Row States
- `DETACHED`: Row created via `newRow()` that is not part of the table yet
- `ADDED`: New row that hasn't been saved
- `MODIFIED`: Existing row that has been changed
- `DELETED`: Row marked for deletion
- `UNCHANGED`: Row with no pending changes

Rows loaded with `DataTable.fromObjects()`, `fromRows()`, `fromQueryResult()` or `loadRows()` are initialized as `UNCHANGED` by default and keep a copy of their original values.

#### Individual Row State Operations
```javascript
// Check row state
const row = dt.rows(0);
console.log(row.getRowState()); // 'DETACHED', 'ADDED', 'MODIFIED', 'DELETED', or 'UNCHANGED'

// Check if row has changes
if (row.hasChanges()) {
    console.log('Row has unsaved changes');
}

// Accept changes (mark as UNCHANGED)
row.acceptChanges();

// Reject changes (revert to original values)
row.rejectChanges();

// Mark row for deletion
row.delete();
console.log(row.getRowState()); // 'DELETED'

// Example workflow
const row = dt.newRow();
row.set('name', 'John');
console.log(row.getRowState()); // 'DETACHED'

dt.rows.add(row);
console.log(row.getRowState()); // 'ADDED'

row.acceptChanges();
console.log(row.getRowState()); // 'UNCHANGED'

row.set('name', 'Jane');
console.log(row.getRowState()); // 'MODIFIED'
console.log(row.hasChanges()); // true

row.rejectChanges();
console.log(row.get('name')); // 'John' (reverted)
console.log(row.getRowState()); // 'UNCHANGED'
```

#### Table-Level State Operations
```javascript
// Accept all changes in the table
dt.acceptChanges();

// Reject all changes in the table
dt.rejectChanges();

// Get all rows with changes
const changedRows = dt.getChanges();
console.log(`${changedRows.length} rows have changes`);

// Get changed rows by state
const onlyModified = dt.getChanges('MODIFIED');

// Get rows by specific state
const addedRows = dt.getRowsByState('ADDED');
const modifiedRows = dt.getRowsByState('MODIFIED');
const deletedRows = dt.getRowsByState('DELETED');

// Check if table has any changes
if (dt.hasChanges()) {
    console.log('Table has unsaved changes');
    
    // Show summary of changes
    console.log(`Added: ${dt.getRowsByState('ADDED').length}`);
    console.log(`Modified: ${dt.getRowsByState('MODIFIED').length}`);
    console.log(`Deleted: ${dt.getRowsByState('DELETED').length}`);
}

// Practical example: Save changes workflow
async function saveChanges(dataTable) {
    if (!dataTable.hasChanges()) {
        console.log('No changes to save');
        return;
    }
    
    try {
        const changes = dataTable.getChanges();
        
        for (const row of changes) {
            const state = row.getRowState();
            
            if (state === 'ADDED') {
                // Insert new row to database
                await insertRow(row);
            } else if (state === 'MODIFIED') {
                // Update existing row in database
                await updateRow(row);
            } else if (state === 'DELETED') {
                // Delete row from database
                await deleteRow(row);
            }
        }
        
        // Accept all changes after successful save
        dataTable.acceptChanges();
        console.log('All changes saved successfully');
        
    } catch (error) {
        console.error('Error saving changes:', error);
        // Optionally reject changes on error
        // dataTable.rejectAllChanges();
    }
}
```

#### DataRowState Utility Methods
```javascript
const { DataRowState } = require('dotnet-datatable');

// Check if a state represents a changed row
console.log(DataRowState.isChanged('MODIFIED')); // true
console.log(DataRowState.isChanged('ADDED')); // true
console.log(DataRowState.isChanged('UNCHANGED')); // false

// Check if a state represents an unchanged row
console.log(DataRowState.isUnchanged('UNCHANGED')); // true
console.log(DataRowState.isUnchanged('MODIFIED')); // false
```

#### Advanced State Management Methods
```javascript
// Get detailed summary of all changes
const summary = dt.getChangesSummary();
console.log(summary);
/* Output:
{
    totalRows: 10,
    addedCount: 2,
    modifiedCount: 3,
    deletedCount: 1,
    unchangedCount: 4,
    hasChanges: true,
    addedRows: [DataRow, DataRow],
    modifiedRows: [DataRow, DataRow, DataRow],
    deletedRows: [DataRow]
}
*/

// Use summary for detailed reporting
if (summary.hasChanges) {
    console.log(`Changes detected:`);
    console.log(`- ${summary.addedCount} new rows`);
    console.log(`- ${summary.modifiedCount} modified rows`);
    console.log(`- ${summary.deletedCount} deleted rows`);
    console.log(`- ${summary.unchangedCount} unchanged rows`);
}

// Clear all change tracking without losing data
// This sets all rows to UNCHANGED state
dt.clearChanges();
console.log(dt.hasChanges()); // false

// Practical example: Reset tracking after manual sync
async function syncWithDatabase(dataTable) {
    const summary = dataTable.getChangesSummary();
    
    if (!summary.hasChanges) {
        console.log('No changes to sync');
        return;
    }
    
    console.log(`Syncing ${summary.addedCount + summary.modifiedCount + summary.deletedCount} changes...`);
    
    // Perform database operations...
    // After successful sync, clear the change tracking
    dataTable.clearChanges();
    
    console.log('Sync completed, change tracking reset');
}
```

### Data Operations

#### Filtering Data
The DataTable provides two methods for filtering data:
- `select()`: Works directly with row values as plain objects. Access values using dot notation (e.g., `row.age`)
- `findRows()`: Works with DataRow objects. Access values using the get() method (e.g., `row.get('age')`)

Examples:
```javascript
// Using select - direct property access
const adults = dt.select(row => row.age >= 18);
const activeUsers = dt.select(row => row.age > 25 && row.active === true);

// Using findRows - using get() method
const johns = dt.findRows({ name: 'John' });
const over25 = dt.findRows(row => row.get('age') > 25);
```

### Query Result Loading

Use `fromObjects`, `fromRows`, `fromRecords` or `fromQueryResult` to import data that was already fetched by another library. Imported rows are marked as `UNCHANGED` and their `originalValues` are populated, so later edits are tracked as `MODIFIED`.

```javascript
const users = DataTable.fromObjects([
    { id: 1, name: 'Mario', active: true },
    { id: 2, name: 'Luca', active: false }
], {
    tableName: 'Users',
    primaryKey: 'id'
});

users.rows[0].set('name', 'Mario Rossi');
console.log(users.rows[0].getRowState()); // 'MODIFIED'
console.log(users.rows[0].originalValues.name); // 'Mario'
```

Query result shape detection:

```javascript
const pgTable = DataTable.fromQueryResult(pgResult, {
    tableName: 'Users',
    primaryKey: 'id',
    useFieldMetadata: true
});

const mysqlTable = DataTable.fromQueryResult(mysqlResult, {
    tableName: 'Users',
    primaryKey: 'id'
});

const customTable = DataTable.fromQueryResult(apiResult, {
    rowsPath: 'data.items',
    tableName: 'Users'
});
```

Loading into an existing table:

```javascript
const table = new DataTable('Users');
table.addColumn('id', 'number', { primaryKey: true });
table.addColumn('name', 'string');

table.loadRows(rows, {
    clearBeforeLoad: true,
    autoCreateColumns: false,
    validateSchema: true,
    convertTypes: true
});
```

Common loading options:

```javascript
{
    tableName: 'Users',
    primaryKey: 'id',
    columns: {},
    includeColumns: [],
    excludeColumns: [],
    renameColumns: { user_id: 'id' },
    columnNameTransform: 'camelCase',
    inferSchema: true,
    useFieldMetadata: true,
    autoCreateColumns: true,
    validateSchema: true,
    convertTypes: true,
    rowState: 'UNCHANGED',
    preserveOriginalValues: true,
    strict: false
}
```

### Merge Operations
`DataTable.merge()` imports rows from another table with the same table name and a compatible schema. Rows are matched by primary key, updated when found, and inserted when missing. If no primary key is configured, source rows are appended.

```javascript
const users = new DataTable('Users');
users.addColumn('id', 'number', { primaryKey: true });
users.addColumn('name', 'string');
users.addColumn('email', 'string');

users.addRow({ id: 1, name: 'Alice', email: 'alice@old.test' });
users.acceptChanges();

const otherUsers = new DataTable('Users');
otherUsers.addColumn('id', 'number', { primaryKey: true });
otherUsers.addColumn('name', 'string');
otherUsers.addColumn('email', 'string');
otherUsers.addColumn('role', 'string');

otherUsers.addRow({ id: 1, name: 'Alice Remote', email: 'alice@new.test', role: 'admin' });
otherUsers.addRow({ id: 2, name: 'Bob', email: 'bob@test.test', role: 'user' });

const result = users.merge(otherUsers, {
  preserveChanges: true,
  missingSchemaAction: 'add'
});

console.log(result);
// {
//   tableName: 'Users',
//   addedColumns: ['role'],
//   ignoredColumns: [],
//   updatedRows: 1,
//   insertedRows: 1,
//   preservedRows: 0,
//   skippedRows: 0,
//   primaryKeyAdded: null
// }
```

Merge options:

- `preserveChanges`: when `true`, local `ADDED` rows are left untouched and locally modified column values are preserved.
- `missingSchemaAction: 'add'`: add source columns missing from the target table.
- `missingSchemaAction: 'ignore'`: ignore source columns missing from the target table.
- `missingSchemaAction: 'error'`: throw if the source schema contains missing target columns. This is the default.

`DataSet.merge()` applies the same behavior by table name. With `missingSchemaAction: 'add'`, missing tables are cloned into the target dataset and compatible missing relations are added.

```javascript
const company = new DataSet('Company');
const incoming = new DataSet('Company');

// configure tables on both datasets...

const summary = company.merge(incoming, {
  preserveChanges: true,
  missingSchemaAction: 'add'
});
```

For database refresh scenarios where you already have plain rows, use `mergeRows()`:

```javascript
const result = users.mergeRows(freshRows, {
  primaryKey: 'id',
  updateExisting: true,
  addMissing: true,
  markModified: false
});

console.log(result.updatedRows);
console.log(result.insertedRows);
```

### Advanced Filtering Criteria
dotnet-datatable supports various operators for advanced filtering. Here are all available operators:

```javascript
// Examples of all available operators
dt.findRows({
    // Comparison operators
    age: { $gt: 25 },          // Greater than (>)
    score: { $gte: 90 },       // Greater than or equal (>=)
    price: { $lt: 100 },       // Less than (<)
    quantity: { $lte: 50 },    // Less than or equal (<=)
    status: { $ne: 'active' }, // Not equal (!=)
    
    // Membership operators
    category: { $in: ['A', 'B', 'C'] },  // Value exists in array
    
    // String operators
    name: { $contains: 'john' },     // String contains 'john'
    
    // Regular expressions
    email: /gmail\.com$/,  // Ends with gmail.com
    
    // Exact values
    active: true,          // Exactly matches true
    type: 'user'          // Exactly matches 'user'
});

// Practical examples of combined use
const results = dt.findRows({
    age: { $gt: 18, $lt: 30 },           // Age between 18 and 30
    name: { $contains: 'smith' },         // Name contains 'smith'
    roles: { $in: ['admin', 'editor'] },  // Role is admin or editor
    email: /^[a-z]+@company\.com$/        // Company email
});

// Custom function search
const filtered = dt.findRows(row => {
    const age = row.get('age');
    const status = row.get('status');
    return age > 25 && status === 'active';
});
```

All supported operators:
- `$gt`: Greater than
- `$gte`: Greater than or equal to
- `$lt`: Less than
- `$lte`: Less than or equal to
- `$ne`: Not equal to
- `$in`: Value exists in array
- `$contains`: String contains value
- RegExp: Support for regular expressions

#### Sorting
```javascript
// Simple sort
dt.sort('age', 'asc');

// Multiple criteria sort
dt.sortMultiple(
    { column: 'age', order: 'desc' },
    { column: 'name', order: 'asc' }
);

// Custom sort
dt.sortBy(row => row.get('age') + row.get('name'));
```

#### Loading Data from Database
The package receives data that was already fetched by your database client. It does not create connections or execute queries.

```javascript
// PostgreSQL example
const { Pool } = require('pg');
const pool = new Pool(config);

const result = await pool.query('SELECT * FROM products WHERE category = $1', ['electronics']);
const products = DataTable.fromQueryResult(result, {
    tableName: 'Products',
    primaryKey: 'id',
    useFieldMetadata: true
});

// MySQL example
const mysql = require('mysql2/promise');
const connection = await mysql.createConnection(config);
const mysqlResult = await connection.execute('SELECT * FROM products WHERE price > ?', [100]);
const mysqlProducts = DataTable.fromQueryResult(mysqlResult, {
    tableName: 'Products',
    primaryKey: 'id'
});

// Loading into an existing table is also supported
const dt = new DataTable('Products');
dt.addColumn('id', 'number', { primaryKey: true });
dt.addColumn('name', 'string');
dt.loadRows(result.rows, { clearBeforeLoad: true });

// Load from array of objects
const data = [
    { id: 1, name: 'John' },
    { id: 2, name: 'Jane' }
];
const users = DataTable.fromRows(data, { tableName: 'Users', primaryKey: 'id' });
```

### Column Operations
```javascript
// Check if column exists
dt.columnExists('name');

// Remove column
dt.removeColumn('age');
```

### Table Manipulation
```javascript
// Clone table
const newTable = dt.clone();

// Iterate through rows
for (const row of newTable) {
    console.log(row.get('name')); // using recommended get() method
}
```

### DataSet Operations
DataSet allows you to manage multiple related tables and define relationships between them.

```javascript
const { DataSet, DataTable } = require('dotnet-datatable');

// Create a new dataset
const ds = new DataSet('CompanyData');

// Add tables to the dataset
const employees = ds.addTable('Employees');
employees.addColumn('id', 'number');
employees.addColumn('name', 'string');
employees.addColumn('departmentId', 'number');

const departments = ds.addTable('Departments');
departments.addColumn('id', 'number');
departments.addColumn('name', 'string');

// Add data
departments.addRow({ id: 1, name: 'HR' });
departments.addRow({ id: 2, name: 'IT' });

employees.addRow({ id: 1, name: 'John', departmentId: 2 });
employees.addRow({ id: 2, name: 'Jane', departmentId: 1 });

// Create a relation between tables
const relation = ds.addRelation(
    'EmpDeptRelation',
    'Departments', 
    'Employees',
    'id',
    'departmentId'
);

// Get related rows
const itDept = departments.findOne({ id: 2 });
const itEmployees = ds.getChildRows(itDept, 'EmpDeptRelation');
console.log(itEmployees); // [{ id: 1, name: 'John', departmentId: 2 }]

// Get parent row
const john = employees.findOne({ name: 'John' });
const johnsDept = ds.getParentRow(john, 'EmpDeptRelation');
console.log(johnsDept.get('name')); // 'IT'
```

You can also build a DataSet directly from multiple query result arrays or SQL Server-style recordsets:

```javascript
const ds = DataSet.fromRecordsets([
    usersRows,
    ordersRows
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

const relation = ds.getRelation('UserOrders');
const orders = relation.getChildRows(userRow);
const user = relation.getParentRow(orderRow);
```

### DataView Operations
DataView provides a filtered and sorted view of a DataTable.
```javascript
const { DataTable, DataView } = require('dotnet-datatable');

// Create a table
const users = new DataTable('Users');
users.addColumn('id', 'number');
users.addColumn('name', 'string');
users.addColumn('age', 'number');
users.addColumn('active', 'boolean');

// Add some data
users.addRow({ id: 1, name: 'John', age: 25, active: true });
users.addRow({ id: 2, name: 'Jane', age: 30, active: true });
users.addRow({ id: 3, name: 'Bob', age: 22, active: false });
users.addRow({ id: 4, name: 'Alice', age: 35, active: true });

// Create a view of active users sorted by age
const activeUsersView = new DataView(
    users,
    { active: true },  // Filter
    'age',            // Sort by
    'desc'            // Sort order
);

// Use the view
console.log(`Active users: ${activeUsersView.count}`); // 3

// Get the first row (oldest active user due to desc sort)
const oldest = activeUsersView.firstRow;
console.log(oldest.get('name')); // 'Alice'

// Iterate through view rows
for (const row of activeUsersView) {
    console.log(`${row.get('name')}: ${row.get('age')}`);
}
// Output:
// Alice: 35
// Jane: 30
// John: 25

// Create a new table from the view
const activeUsersTable = activeUsersView.toTable();

// Get view data as array of objects
const activeUsersArray = activeUsersView.toArray();
```

The fluent API is also available:

```javascript
const activeAdults = users
    .createView()
    .where('active', '=', true)
    .where('age', '>', 18)
    .orderBy('name', 'asc')
    .skip(20)
    .take(10)
    .toObjects();

const quickView = users.createView({
    filter: "age >= 18 AND active = true",
    sort: 'name ASC'
});
```

### Advanced Schema Management

The DataTable provides advanced schema management capabilities for working with table structures:

```javascript
const { DataTable } = require('dotnet-datatable');

// Create a table with schema
const users = new DataTable('Users');
users.addColumn('id', 'number');
users.addColumn('name', 'string');
users.addColumn('age', 'number');

// Mark column as primary key
users.columns._columns.get('id').isPrimaryKey = true;
users.columns._columns.get('id').allowNull = false;

// Export the schema to a portable format
const schema = users.exportSchema();
console.log(schema);
/* Output:
{
  tableName: 'Users',
  caseSensitive: false,
  columns: [
    {
      name: 'id',
      dataType: 'number',
      allowNull: false,
      defaultValue: null,
      expression: null,
      readOnly: false,
      unique: true,
      ordinal: 0,
      caption: 'id',
      isPrimaryKey: true
    },
    // ...other columns
  ],
  primaryKey: ['id'],
  uniqueConstraints: []
}
*/

// Save schema to JSON
const schemaJson = users.serializeSchema();
// Later, recreate the table from JSON
const recreatedTable = DataTable.deserializeSchema(schemaJson);

// Create another table with a different schema
const updatedUsers = new DataTable('UpdatedUsers');
updatedUsers.addColumn('id', 'number');
updatedUsers.addColumn('name', 'string');
updatedUsers.addColumn('age', 'number');
updatedUsers.addColumn('email', 'string'); // New column
updatedUsers.columns._columns.get('name').allowNull = false; // Changed nullability

// Compare schemas
const differences = users.compareSchema(updatedUsers);
console.log(differences);
/* Output:
{
  missingColumns: ['email'],
  extraColumns: [],
  typeMismatches: [],
  nullabilityDifferences: [
    {
      column: 'name',
      thisAllowNull: true,
      otherAllowNull: false
    }
  ]
}
*/

// Update schema
const updateResult = users.updateSchema(updatedUsers);
console.log(updateResult);
/* Output:
{
  addedColumns: ['email'],
  removedColumns: [],
  modifiedColumns: [
    {
      column: 'name',
      change: 'allowNull',
      from: true,
      to: false
    }
  ]
}
*/

// Create a table from an existing schema
const newTable = DataTable.importSchema({
  tableName: 'Products',
  columns: [
    { name: 'id', dataType: 'number', allowNull: false, defaultValue: null },
    { name: 'name', dataType: 'string', allowNull: false },
    { name: 'price', dataType: 'number', defaultValue: 0 },
    { name: 'createdAt', dataType: 'date', defaultValue: () => new Date() }
  ],
  primaryKey: ['id']
});
```
The schema management features allow you to:

- Export table structure to a portable format
- Create tables from existing schemas
- Compare schemas between tables to identify differences
- Update a table's schema to match another
- Serialize/deserialize schemas to JSON

This is especially useful for:

- -Creating table structures dynamically based on configuration
- Migrating data between different schema versions
- Generating table documentation
- Schema validation and enforcement

## Supported Data Types
* string
* number
* integer
* date
* boolean
* object
* array
* json
* bigint
* buffer
* any

## Advanced Database Usage
The DataTable automatically creates columns based on query result rows and, when available, field metadata. This is best-effort mapping and does not require any database package dependency:

- PostgreSQL `int2/int4` -> integer, `int8` -> bigint, `numeric/float` -> number, `varchar/text` -> string, `bool` -> boolean, `timestamp/date` -> date, `json/jsonb` -> json, `bytea` -> buffer
- MySQL `INT` -> integer, `BIGINT` -> bigint, `DECIMAL/FLOAT/DOUBLE` -> number, `VARCHAR/TEXT` -> string, `DATE/DATETIME/TIMESTAMP` -> date, `TINYINT(1)` -> boolean, `JSON` -> json, `BLOB` -> buffer
- SQL Server `int/smallint/tinyint` -> integer, `bigint` -> bigint, `decimal/numeric/float/real/money` -> number, `varchar/nvarchar/text` -> string, `bit` -> boolean, `date/datetime/datetime2` -> date, `varbinary` -> buffer
- SQLite `INTEGER` -> integer, `REAL` -> number, `TEXT` -> string, `BLOB` -> buffer

This makes it perfect for scenarios where you need to:
- Cache database results
- Manipulate query results before display
- Create temporary data structures from database queries
- Transform data before sending to the frontend

Limitations:

- The package does not connect to databases.
- The package does not execute SQL queries.
- The package is not an ORM.
- It does not replace Prisma, Sequelize, TypeORM, Drizzle or Knex.
- Complex SQL-like DataView filter strings are intentionally limited.

## Error Handling
The library throws errors for:
* Invalid column operations
* Type mismatches
* Null violations
* Duplicate columns

## License
MIT

## Disclaimer

A DataTable-inspired data structure for JavaScript and TypeScript, designed for developers who like the .NET DataTable, DataView and DataSet programming model.

This project is not affiliated with Microsoft or the official .NET project.
