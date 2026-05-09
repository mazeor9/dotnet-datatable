import type {
  DataColumnCollectionDebugView,
  DataColumnDebugView,
  DataRowCollectionDebugView,
  DataRowDebugView,
  DataSetDebugView,
  DataSetSchemaDebugView,
  DataTableDebugView,
  DataTableSchemaDebugView,
  DataViewDebugView,
  DebugStringOptions,
  DebugViewOptions
} from './debug';
import type {
  ChangeSetOptions,
  DataSetChangeSet,
  DataSetChangeSetObject,
  DataTableChangeSet,
  DataTableChangeSetObject
} from './change-tracking';
import type { QueryResultMapOptions } from './mapping';

export type DataRecord = Record<string, unknown>;

export type DataTypeName =
  | 'any'
  | 'array'
  | 'bigint'
  | 'boolean'
  | 'buffer'
  | 'date'
  | 'integer'
  | 'json'
  | 'null'
  | 'number'
  | 'object'
  | 'string'
  | 'undefined';

export type SortDirection = 'asc' | 'desc';
export type DataRowStateValue = 'DETACHED' | 'ADDED' | 'MODIFIED' | 'DELETED' | 'UNCHANGED';
export type DataRowVersion = 'current' | 'original' | 'proposed';
export type DataViewFilter<TRow extends object = DataRecord> =
  | string
  | Partial<Record<keyof TRow | string, unknown>>
  | ((row: TRow & DataRow<TRow>, dataRow?: DataRow<TRow>) => boolean);

export interface DataColumnOptions {
  allowNull?: boolean;
  defaultValue?: unknown;
  primaryKey?: boolean;
  isPrimaryKey?: boolean;
  caption?: string;
  expression?: string | ((row: any, dataRow?: DataRow<any>, table?: DataTable<any>, version?: DataRowVersion) => unknown);
  readOnly?: boolean;
  unique?: boolean;
  maxLength?: number | null;
  sourceColumn?: string | null;
  metadata?: unknown;
}

export interface ColumnDefinition extends DataColumnOptions {
  name?: string;
  columnName?: string;
  type?: DataTypeName | string | null;
  dataType?: DataTypeName | string | null;
}

export interface DataTableLoadOptions {
  tableName?: string;
  name?: string;
  primaryKey?: string | string[] | null;
  columns?: Array<string | ColumnDefinition> | Record<string, ColumnDefinition | DataTypeName | string> | null;
  includeColumns?: string[] | null;
  excludeColumns?: string[] | null;
  renameColumns?: Record<string, string> | null;
  columnNameTransform?: 'none' | 'camelCase' | 'pascalCase' | 'snakeCase' | string | ((columnName: string) => string);
  inferSchema?: boolean;
  useFieldMetadata?: boolean;
  autoCreateColumns?: boolean;
  validateSchema?: boolean;
  convertTypes?: boolean;
  rowState?: DataRowStateValue | string;
  preserveOriginalValues?: boolean;
  allowNull?: boolean;
  strict?: boolean;
  clearBeforeLoad?: boolean;
  append?: boolean;
  ignoreExtraColumns?: boolean;
  throwOnExtraColumns?: boolean;
  columnMetadata?: unknown;
  provider?: string | null;
  recordsAlreadyNormalized?: boolean;
  recordAlreadyNormalized?: boolean;
}

export interface MergeRowsOptions extends DataTableLoadOptions {
  updateExisting?: boolean;
  addMissing?: boolean;
  markModified?: boolean;
  validateSchema?: boolean;
}

export interface DataTableMergeOptions {
  preserveChanges?: boolean;
  missingSchemaAction?: 'add' | 'ignore' | 'error' | string;
}

export interface ApplyChangeSetOptions {
  missingRowAction?: 'ignore' | 'add' | 'error' | string;
  conflictPolicy?: 'overwrite' | 'preserve' | 'error' | string;
  strict?: boolean;
}

export interface ApplyChangeSetResult {
  tableName: string;
  appliedAdded: number;
  appliedModified: number;
  appliedDeleted: number;
  skipped: number;
}

export interface ApplyDataSetChangeSetOptions extends ApplyChangeSetOptions {
  missingTableAction?: 'ignore' | 'error' | string;
}

export interface ApplyDataSetChangeSetResult {
  dataSetName: string;
  appliedTables: Array<{ tableName: string; summary: ApplyChangeSetResult }>;
  skippedTables: string[];
}

export interface DataTableMergeResult {
  addedColumns: string[];
  ignoredColumns: string[];
  updatedRows: number;
  insertedRows: number;
  preservedRows: number;
  skippedRows: number;
  primaryKeyAdded: string[] | null;
}

export interface MergeRowsResult {
  updatedRows: number;
  insertedRows: number;
  skippedRows: number;
}

export interface SchemaComparison {
  missingColumns: string[];
  extraColumns: string[];
  typeMismatches: Array<{
    column: string;
    thisType?: string | null;
    otherType?: string | null;
  }>;
  nullabilityDifferences: Array<{
    column: string;
    thisAllowNull?: boolean;
    otherAllowNull?: boolean;
  }>;
}

export interface SchemaUpdateResult {
  addedColumns: string[];
  removedColumns: string[];
  modifiedColumns: Array<{
    column: string;
    change: string;
    from: unknown;
    to: unknown;
  }>;
}

export interface DataTableSchema {
  tableName?: string;
  caseSensitive?: boolean;
  columns: ColumnDefinition[];
  primaryKey?: string[] | null;
  uniqueConstraints?: Array<{ columns: string[]; name?: string }>;
}

export interface ToObjectsOptions {
  includeDeleted?: boolean;
  includeRowState?: boolean;
  includeOriginalValues?: boolean;
  onlyChanged?: boolean;
  columnNameMapping?: Record<string, string> | ((columnName: string) => string) | null;
  dateMode?: 'date' | 'iso-string';
  bigIntMode?: 'bigint' | 'string';
}

export interface DataTableJson {
  tableName: string;
  columns: Array<{
    name: string;
    dataType?: string | null;
    allowNull?: boolean;
    primaryKey?: boolean;
    unique?: boolean;
    readOnly?: boolean;
    maxLength?: number | null;
    sourceColumn?: string | null;
  }>;
  rows: DataRecord[];
}

export interface DataSetJson {
  dataSetName: string;
  tables: DataTableJson[];
  relations: Array<{
    name?: string;
    parentTable?: string;
    parentColumn?: string;
    childTable?: string;
    childColumn?: string;
  }>;
}

export interface DataSetMergeResult {
  dataSetName: string;
  mergedTables: Array<{ tableName: string; result: DataTableMergeResult }>;
  addedTables: string[];
  ignoredTables: string[];
  relationsAdded: string[];
}

export interface DataSetLoadOptions extends DataTableLoadOptions {
  dataSetName?: string;
  tableNames?: Array<string | DataTableLoadOptions>;
  tables?: Array<string | DataTableLoadOptions>;
  tableOptions?: DataTableLoadOptions[];
  relations?: Array<{
    name?: string;
    relationName?: string;
    parentTable: string;
    parentColumn: string;
    childTable: string;
    childColumn: string;
  }>;
}

export const DataRowState: {
  DETACHED: 'DETACHED';
  ADDED: 'ADDED';
  MODIFIED: 'MODIFIED';
  DELETED: 'DELETED';
  UNCHANGED: 'UNCHANGED';
  isChanged(state: DataRowStateValue | string): boolean;
  isUnchanged(state: DataRowStateValue | string): boolean;
  isDetached(state: DataRowStateValue | string): boolean;
};

export class DataColumn {
  constructor(
    columnName: string,
    dataType?: DataTypeName | string | null,
    allowNullOrOptions?: boolean | DataColumnOptions,
    defaultValue?: unknown
  );

  columnName: string;
  dataType: DataTypeName | string | null;
  ordinal: number;
  allowNull: boolean;
  defaultValue: unknown;
  caption: string;
  expression: unknown | ((row: any, dataRow?: DataRow<any>, table?: DataTable<any>, version?: DataRowVersion) => unknown);
  readOnly: boolean;
  unique: boolean;
  isPrimaryKey: boolean;
  maxLength: number | null;
  sourceColumn: string | null;
  metadata: unknown;

  readonly table: DataTable<any> | null;
  readonly isComputed: boolean;

  toJSON(): Omit<DataColumnDebugView, 'type' | 'tableName'>;
  toDebugView(): DataColumnDebugView;
}

export interface DataColumnCollection extends Iterable<DataColumn> {
  readonly count: number;

  add(column: DataColumn): DataColumn;
  add(columnName: string, dataType?: DataTypeName | string | null, options?: DataColumnOptions): DataColumn;
  remove(columnName: string): void;
  contains(columnName: string): boolean;
  has(columnName: string): boolean;
  get(columnNameOrIndex: string | number): DataColumn;
  toArray(): DataColumn[];
  toJSON(): Array<Omit<DataColumnDebugView, 'type' | 'tableName'>>;
  toDebugView(): DataColumnCollectionDebugView;
  setPrimaryKey(columnNames: string | string[]): void;
  getPrimaryKey(): string[];
  clearPrimaryKey(): void;
}

export class DataRow<TRow extends object = DataRecord> {
  constructor(table: DataTable<TRow>, initialState?: DataRowStateValue | string);

  item<K extends keyof TRow & string>(columnName: K): TRow[K];
  item(columnName: string): unknown;
  get<K extends keyof TRow & string>(columnName: K, version?: DataRowVersion): TRow[K];
  get(index: number, version?: DataRowVersion): unknown;
  get(columnName: string, version?: DataRowVersion): unknown;
  set<K extends keyof TRow & string>(columnName: K, value: TRow[K]): void;
  set(columnName: string, value: unknown): void;
  toJSON(): TRow & DataRecord;
  toString(): string;
  acceptChanges(): void;
  rejectChanges(): void;
  beginEdit(): this;
  endEdit(): this;
  cancelEdit(): this;
  hasChanges(): boolean;
  getRowState(): DataRowStateValue;
  delete(): void;
  toObject(): TRow & DataRecord;
  toDebugView(): DataRowDebugView;

  readonly rowState: DataRowStateValue;
  readonly currentValues: TRow & DataRecord;
  readonly originalValues: Partial<TRow> & DataRecord;
  readonly proposedValues: Partial<TRow> & DataRecord | null;
}

export interface DataRowCollection<TRow extends object = DataRecord> extends Iterable<DataRow<TRow>> {
  readonly count: number;
  readonly [index: number]: DataRow<TRow> | undefined;
  (index: number): DataRow<TRow> | undefined;

  add(row: DataRow<TRow> | Partial<TRow> | DataRecord | unknown[]): DataRow<TRow>;
  remove(row: DataRow<TRow>): void;
  removeAt(index: number): void;
  clear(): void;
  countRows(): number;
  toArray(): Array<DataRow<TRow>>;
  toJSON(): Array<TRow & DataRecord>;
  toDebugView(options?: DebugViewOptions): DataRowCollectionDebugView;
  find(key: unknown): DataRow<TRow> | null;
}

export class DataTable<TRow extends object = DataRecord> implements Iterable<DataRow<TRow>> {
  constructor(tableName?: string);

  tableName: string;
  rows: DataRowCollection<TRow>;
  columns: DataColumnCollection;
  caseSensitive: boolean;

  addColumn(columnName: string, dataType?: DataTypeName | string | null, options?: DataColumnOptions): DataColumn;
  removeColumn(columnName: string): void;
  columnExists(columnName: string): boolean;
  newRow(): DataRow<TRow>;
  addRow(values: DataRow<TRow> | Partial<TRow> | DataRecord | unknown[]): DataRow<TRow>;

  static fromObjects<TRow extends object = DataRecord>(objects: TRow[], options?: DataTableLoadOptions): DataTable<TRow>;
  static fromRows<TRow extends object = DataRecord>(rows: TRow[], options?: DataTableLoadOptions): DataTable<TRow>;
  static fromRecords<TRow extends object = DataRecord>(records: TRow[], options?: DataTableLoadOptions): DataTable<TRow>;
  static fromQueryResult<TRow extends object = DataRecord>(queryResult: unknown, options?: QueryResultMapOptions): DataTable<TRow>;

  removeRow(index: number): void;
  deleteRow(index: number): void;
  select(filterExpression?: string | null, sortExpression?: string | null): Array<DataRow<TRow>>;
  select(filterExpression: (values: TRow & DataRecord) => boolean): Array<TRow & DataRecord>;
  selectRows(filterExpression?: DataViewFilter<TRow> | null, sortExpression?: string | null, rowStateFilter?: DataRowStateValue | string | null): Array<DataRow<TRow>>;
  compute(aggregateExpression: string, filterExpression?: DataViewFilter<TRow> | null): unknown;
  sort(columnNameOrComparer: string | ((a: DataRow<TRow>, b: DataRow<TRow>) => number), order?: SortDirection): this;
  sortBy(expression: (row: DataRow<TRow>) => unknown): this;
  sortMultiple(...sortCriteria: Array<{ column: keyof TRow & string; order?: SortDirection } | { column: string; order?: SortDirection }>): this;
  clear(): void;
  clone(): DataTable<TRow>;

  findRows(criteria: Partial<TRow> | DataRecord | ((row: DataRow<TRow>) => boolean)): Array<DataRow<TRow>>;
  findOne(criteria: Partial<TRow> | DataRecord | ((row: DataRow<TRow>) => boolean)): DataRow<TRow> | null;
  loadFromQuery(queryResults: TRow[]): this;
  loadFromQueryAsync(queryPromise: Promise<TRow[]>): Promise<this>;
  loadRows(rows: unknown[], options?: DataTableLoadOptions): this;
  mergeRows(rows: unknown[], options?: MergeRowsOptions): MergeRowsResult;
  exportSchema(): DataTableSchema;
  beginLoadData(): this;
  endLoadData(): this;
  addUniqueConstraint(columns: string | string[], name?: string): { name: string; columns: string[] };
  getUniqueConstraints(): Array<{ name: string; columns: string[] }>;
  addCheckConstraint(predicate: (row: any, dataRow?: DataRow<TRow>, table?: DataTable<TRow>) => boolean, name?: string): { name: string };
  getCheckConstraints(): Array<{ name: string }>;
  compareSchema(otherTable: DataTable): SchemaComparison;
  updateSchema(sourceTable: DataTable, addMissingColumns?: boolean, removeExtraColumns?: boolean): SchemaUpdateResult;
  merge(sourceTable: DataTable<TRow>, options?: DataTableMergeOptions): DataTableMergeResult;
  serializeSchema(): string;
  createView(options?: { filter?: DataViewFilter<TRow>; sort?: string | ((row: DataRow<TRow>) => unknown); sortOrder?: SortDirection }): DataView<TRow>;

  readonly defaultView: DataView<TRow>;

  toObjects(options?: ToObjectsOptions): Array<TRow & DataRecord>;
  toArray(options?: ToObjectsOptions): Array<TRow & DataRecord>;
  getSchema(): DataTableSchemaDebugView;
  getPreview(maxRows?: number): DataRecord[];
  toConsoleTable(): DataRecord[];
  toDebugView(options?: DebugViewOptions): DataTableDebugView;
  toDebugString(options?: DebugStringOptions): string;
  toJSON(): DataTableJson;
  acceptAllChanges(): void;
  rejectAllChanges(): void;
  acceptChanges(): void;
  rejectChanges(): void;
  getChanges(rowState?: DataRowStateValue | string | null): Array<DataRow<TRow>>;
  getChangeSet(options?: ChangeSetOptions): DataTableChangeSet;
  getCommands(options?: ChangeSetOptions): unknown;
  applyChangeSet(changeSet: DataTableChangeSet | DataTableChangeSetObject | unknown, options?: ApplyChangeSetOptions): ApplyChangeSetResult;
  getRowsByState(state: DataRowStateValue | string): Array<DataRow<TRow>>;
  hasChanges(): boolean;
  getChangesSummary(): {
    totalRows: number;
    addedCount: number;
    modifiedCount: number;
    deletedCount: number;
    unchangedCount: number;
    hasChanges: boolean;
    addedRows: Array<DataRow<TRow>>;
    modifiedRows: Array<DataRow<TRow>>;
    deletedRows: Array<DataRow<TRow>>;
  };
  clearChanges(): void;
  setPrimaryKey(columnNames: string | string[]): void;
  getPrimaryKey(): string[];
  find(key: unknown): DataRow<TRow> | null;
  findByPrimaryKey(key: unknown): DataRow<TRow> | null;
  on(eventName: string, handler: (payload: unknown) => void): this;
  off(eventName: string, handler: (payload: unknown) => void): this;
  serialize(options?: { asObject?: boolean }): string | unknown;
  join(otherTable: DataTable<any>, options?: unknown): DataTable<any>;
  groupBy(keys: string | string[] | ((row: DataRow<TRow>) => unknown), aggregations?: unknown): unknown[];
  distinct(columns: string | string[]): DataTable<any>;
  union(otherTable: DataTable<any>, options?: unknown): DataTable<any>;

  [Symbol.iterator](): IterableIterator<DataRow<TRow>>;

  static importSchema<TRow extends object = DataRecord>(schema: DataTableSchema): DataTable<TRow>;
  static deserializeSchema<TRow extends object = DataRecord>(schemaJson: string): DataTable<TRow>;
  static deserialize<TRow extends object = DataRecord>(input: unknown): DataTable<TRow>;
}

export class DataRelation {
  constructor(relationName: string, parentColumn: DataColumn, childColumn: DataColumn);

  relationName: string;
  parentColumn: DataColumn;
  childColumn: DataColumn;
  parentTable: DataTable<any>;
  childTable: DataTable<any>;

  isValid(parentRow: DataRow, childRow: DataRow): boolean;
  getChildRows(parentRow: DataRow): DataRow[];
  getParentRow(childRow: DataRow): DataRow | null;
  toString(): string;
}

export class DataView<TRow extends object = DataRecord> implements Iterable<DataRow<TRow>> {
  constructor(
    table: DataTable<TRow>,
    rowFilter?: DataViewFilter<TRow> | null,
    sort?: string | ((a: DataRow<TRow>, b?: DataRow<TRow>) => number | unknown) | null,
    sortOrder?: SortDirection
  );

  setFilter(filter: DataViewFilter<TRow>): this;
  filter(filter: DataViewFilter<TRow>): this;
  where(columnName: keyof TRow & string | string, operator: string, value: unknown): this;
  setSort(sort: string | ((a: DataRow<TRow>, b?: DataRow<TRow>) => number | unknown), order?: SortDirection): this;
  sort(sort: string | ((a: DataRow<TRow>, b?: DataRow<TRow>) => number | unknown), order?: SortDirection): this;
  orderBy(columnName: keyof TRow & string | string, direction?: SortDirection): this;
  take(count: number): this;
  skip(count: number): this;
  getRows(): Array<DataRow<TRow>>;
  toTable(): DataTable<TRow>;
  toDataTable(): DataTable<TRow>;
  toArray(): Array<TRow & DataRecord>;
  toJSON(): Array<TRow & DataRecord>;
  toObjects(options?: { includeDeleted?: boolean }): Array<TRow & DataRecord>;
  getPreview(maxRows?: number): DataRecord[];
  toDebugView(options?: DebugViewOptions): DataViewDebugView;

  readonly count: number;
  readonly firstRow: DataRow<TRow> | null;

  row(index: number): DataRow<TRow>;
  [Symbol.iterator](): IterableIterator<DataRow<TRow>>;
}

export class DataSet {
  constructor(dataSetName?: string);

  dataSetName: string;
  tables: Map<string, DataTable<any>>;
  relations: DataRelation[];
  enforceConstraints: boolean;

  static fromRecordsets(recordsets: unknown[][], options?: DataSetLoadOptions): DataSet;
  static fromQueryResult(queryResult: unknown, options?: DataSetLoadOptions & QueryResultMapOptions): DataSet;

  addTable<TRow extends object = DataRecord>(tableNameOrTable: DataTable<TRow>): DataTable<TRow>;
  addTable(tableNameOrTable: string): DataTable;
  removeTable(tableName: string): void;
  table(tableName: string): DataTable;
  addRelation(
    relationName: string,
    parentTableOrColumn: string | DataColumn,
    childTableOrColumn: string | DataColumn,
    parentColumnName?: string,
    childColumnName?: string
  ): DataRelation;
  removeRelation(relationName: string): void;
  hasTable(tableName: string): boolean;
  getRelations(tableName: string): DataRelation[];
  getRelation(relationName: string): DataRelation;
  getChildRows(parentRow: DataRow, relationName: string): DataRow[];
  getParentRow(childRow: DataRow, relationName: string): DataRow | null;
  clear(): void;
  getChangeSet(options?: ChangeSetOptions): DataSetChangeSet;
  getCommands(options?: ChangeSetOptions): unknown;
  applyChangeSet(changeSet: DataSetChangeSet | DataSetChangeSetObject | unknown, options?: ApplyDataSetChangeSetOptions): ApplyDataSetChangeSetResult;
  addForeignKeyConstraint(relationName: string, options?: { name?: string; deleteRule?: string; updateRule?: string }): unknown;
  getForeignKeyConstraints(): unknown[];
  merge(source: DataSet | DataTable<any>, options?: DataTableMergeOptions): DataSetMergeResult;
  toJSON(): DataSetJson;
  serialize(options?: { asObject?: boolean }): string | unknown;
  static deserialize(input: unknown): DataSet;
  toDebugView(options?: DebugViewOptions): DataSetDebugView;
  getSchema(): DataSetSchemaDebugView;
  clone(): DataSet;
}
