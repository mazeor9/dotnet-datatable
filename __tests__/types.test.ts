import {
  DataSet,
  DataTable,
  type DataTableDebugView,
  type DataTableSchemaDebugView
} from 'dotnet-datatable';
import { DataTableChangeSet } from 'dotnet-datatable/change-tracking';

interface Customer {
  id: number;
  name: string;
  email: string;
}

const customers = DataTable.fromObjects<Customer>([
  { id: 1, name: 'Mario', email: 'mario@test.it' },
  { id: 2, name: 'Laura', email: 'laura@test.it' }
], {
  tableName: 'Customers',
  primaryKey: 'id'
});

const first = customers.find(1);
const email: string | undefined = first?.get('email');
const rows: Customer[] = customers.toObjects();
const schema: DataTableSchemaDebugView = customers.getSchema();
const debugView: DataTableDebugView = customers.toDebugView();

const dataSet = new DataSet('CRM');
dataSet.addTable(customers);
dataSet.merge(customers, { missingSchemaAction: 'ignore' });

const changeSet = DataTableChangeSet.fromTable(customers);

void email;
void rows;
void schema;
void debugView;
void dataSet;
void changeSet;
