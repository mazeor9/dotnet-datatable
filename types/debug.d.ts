import type { DataRecord } from './core';

export interface DataColumnDebugView {
  type: 'DataColumn';
  name: string;
  dataType?: string | null;
  allowNull?: boolean;
  defaultValue?: unknown;
  readOnly?: boolean;
  unique?: boolean;
  primaryKey?: boolean;
  ordinal?: number;
  caption?: string;
  maxLength?: number | null;
  sourceColumn?: string | null;
  metadata?: unknown;
  tableName?: string;
}

export interface DataRowDebugView {
  type: 'DataRow';
  tableName?: string;
  rowState?: string;
  values: DataRecord;
}

export interface DataTableDebugView {
  type: 'DataTable';
  name?: string;
  tableName?: string;
  columns: DataColumnDebugView[];
  rows: DataRecord[];
  rowCount: number;
  columnCount: number;
  primaryKey?: string[];
  preview?: DataRecord[];
}

export interface DataViewDebugView {
  type: 'DataView';
  sourceTable?: string;
  rows: DataRecord[];
  rowCount: number;
  sort?: string;
  filter?: string;
  preview?: DataRecord[];
}

export interface DataRelationDebugView {
  name?: string;
  parentTable?: string;
  parentColumn?: string;
  childTable?: string;
  childColumn?: string;
}

export interface DataSetDebugView {
  type: 'DataSet';
  name?: string;
  dataSetName?: string;
  tables: DataTableDebugView[];
  tableCount: number;
  relations?: DataRelationDebugView[];
}

export interface DataTableSchemaDebugView {
  type: 'DataTableSchema';
  name?: string;
  tableName?: string;
  columns: DataColumnDebugView[];
  columnCount: number;
  primaryKey: string[];
  caseSensitive: boolean;
}

export interface DataSetSchemaDebugView {
  type: 'DataSetSchema';
  name?: string;
  dataSetName?: string;
  tables: DataTableSchemaDebugView[];
  tableCount: number;
  relations: DataRelationDebugView[];
}

export interface DataColumnCollectionDebugView {
  type: 'DataColumnCollection';
  tableName?: string;
  columns: DataColumnDebugView[];
  columnCount: number;
}

export interface DataRowCollectionDebugView {
  type: 'DataRowCollection';
  tableName?: string;
  rows: DataRecord[];
  rowCount: number;
}

export interface DebugViewOptions {
  maxRows?: number;
  includeDeleted?: boolean;
}

export interface DebugStringOptions {
  maxRows?: number;
}
