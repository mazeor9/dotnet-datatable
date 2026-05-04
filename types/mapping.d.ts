import type {
  ColumnDefinition,
  DataRecord,
  DataRow,
  DataTable,
  DataTableLoadOptions,
  DataTypeName
} from './core';

export interface QueryResultMapOptions extends DataTableLoadOptions {
  rowsPath?: string | string[];
  fieldsPath?: string | string[];
  fields?: unknown;
  provider?: string;
  recordsetIndex?: number;
}

export interface QueryResultMappingResult<TRow extends object = DataRecord> {
  rows: TRow[];
  fields?: unknown;
  provider: string;
  recordsets: TRow[][];
}

export class QueryResultMapper {
  static map<TRow extends object = DataRecord>(
    queryResult: unknown,
    options?: QueryResultMapOptions
  ): QueryResultMappingResult<TRow>;

  static extractRows<TRow extends object = DataRecord>(
    queryResult: unknown,
    options?: QueryResultMapOptions
  ): TRow[];

  static extractRecordsets<TRow extends object = DataRecord>(
    queryResult: unknown,
    options?: QueryResultMapOptions
  ): TRow[][];
}

export class SchemaInferer {
  static infer(rows: unknown[], options?: DataTableLoadOptions): ColumnDefinition[];
  static normalizeColumnDefinitions(
    columns: DataTableLoadOptions['columns'],
    options?: DataTableLoadOptions
  ): Map<string, ColumnDefinition>;
}

export class TypeMapper {
  static normalizeType(type: unknown): DataTypeName | string;
  static inferValueType(value: unknown): DataTypeName | string;
  static reconcileTypes(types: Array<DataTypeName | string | null | undefined>): DataTypeName | string;
  static fromPostgresDataTypeID(dataTypeID: unknown): DataTypeName | string;
  static fromMySqlColumnType(columnType: unknown, metadata?: DataRecord, options?: DataRecord): DataTypeName | string;
  static fromSqlServerType(type: unknown): DataTypeName | string;
  static fromSqliteType(type: unknown): DataTypeName | string;
  static fromOracleType(type: unknown, metadata?: DataRecord): DataTypeName | string;
  static fromDatabaseType(type: unknown, provider?: string, metadata?: DataRecord, options?: DataRecord): DataTypeName | string;
  static convertValue<T = unknown>(value: unknown, targetType: DataTypeName | string, options?: { strict?: boolean }): T;
}

export class ColumnMetadataNormalizer {
  static normalize(fields: unknown, options?: DataTableLoadOptions): ColumnDefinition[];
  static normalizeField(field: unknown, options?: DataTableLoadOptions): ColumnDefinition | null;
}

export class DataTableLoader {
  static load<TRow extends object = DataRecord>(
    table: DataTable<TRow>,
    rows: unknown[],
    options?: DataTableLoadOptions
  ): DataTable<TRow>;

  static addRecord<TRow extends object = DataRecord>(
    table: DataTable<TRow>,
    record: unknown,
    options?: DataTableLoadOptions
  ): DataRow<TRow>;

  static normalizeRows<TRow extends object = DataRecord>(
    rows: unknown[],
    options?: DataTableLoadOptions
  ): TRow[];
}
