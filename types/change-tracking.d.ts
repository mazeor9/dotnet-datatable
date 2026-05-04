import type { DataRecord, DataRowStateValue, DataSet, DataTable } from './core';

export interface ChangeSetOptions {
  includeColumns?: string[] | null;
  excludeColumns?: string[] | null;
  includeUnchangedTables?: boolean;
}

export interface RowChange {
  state: DataRowStateValue;
  tableName: string;
  key: DataRecord | null;
  values: DataRecord;
  originalValues?: DataRecord;
  originalKey?: DataRecord | null;
  changedColumns?: string[];
}

export interface DataTableChangeSetObject {
  tableName: string;
  primaryKey: string[];
  added: RowChange[];
  modified: RowChange[];
  deleted: RowChange[];
  count: number;
  hasChanges: boolean;
}

export interface DataSetChangeSetObject {
  dataSetName: string;
  tables: DataTableChangeSetObject[];
  count: number;
  hasChanges: boolean;
}

export class DataTableChangeSet {
  constructor(tableName: string, primaryKey: string[], changes?: Partial<{
    added: RowChange[];
    modified: RowChange[];
    deleted: RowChange[];
  }>);

  tableName: string;
  primaryKey: string[];
  added: RowChange[];
  modified: RowChange[];
  deleted: RowChange[];

  static fromTable(table: DataTable<any>, options?: ChangeSetOptions): DataTableChangeSet;

  readonly count: number;
  readonly hasChanges: boolean;

  isEmpty(): boolean;
  toObject(): DataTableChangeSetObject;
  toJSON(): DataTableChangeSetObject;
}

export class DataSetChangeSet {
  constructor(dataSetName: string, tables?: DataTableChangeSet[]);

  dataSetName: string;
  tables: DataTableChangeSet[];

  static fromDataSet(dataSet: DataSet, options?: ChangeSetOptions): DataSetChangeSet;

  readonly count: number;
  readonly hasChanges: boolean;

  isEmpty(): boolean;
  table(tableName: string): DataTableChangeSet | null;
  toObject(): DataSetChangeSetObject;
  toJSON(): DataSetChangeSetObject;
}
