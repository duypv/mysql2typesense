import type {
  DocumentTransformer,
  SyncDocument,
  TableSyncConfig,
  TransformFieldMapping,
  TypesenseFieldType
} from "../../core/types.js";

const TRUE_VALUES = new Set(["1", "true", "yes", "y", "on"]);

export class ConfigDrivenTransformer implements DocumentTransformer {
  async toDocument(row: Record<string, unknown>, table: TableSyncConfig): Promise<SyncDocument> {
    const primaryValue = row[table.primaryKey];
    if (primaryValue === undefined || primaryValue === null) {
      throw new Error(`Missing primary key \"${table.primaryKey}\" in source row`);
    }

    const document: SyncDocument = {
      id: String(primaryValue)
    };

    for (const mapping of table.transform.fieldMappings) {
      const rawValue = row[mapping.source];
      const resolvedValue = this.resolveValue(rawValue, mapping);

      if (resolvedValue === undefined || resolvedValue === null) {
        if (mapping.optional || table.transform.dropNulls) {
          continue;
        }

        throw new Error(`Field \"${mapping.source}\" is required for table \"${table.table}\"`);
      }

      document[mapping.target] = mapping.target === "id" ? String(resolvedValue) : resolvedValue;
    }

    return document;
  }

  private resolveValue(rawValue: unknown, mapping: TransformFieldMapping): unknown {
    const value = rawValue ?? mapping.defaultValue;
    if (value === undefined || value === null) {
      return value;
    }

    const normalizedValue = this.normalizeSourceValue(value, mapping);
    // JSON source values are already properly typed after parsing; skip coercion
    // to avoid rejecting valid JSON arrays when the target type is "object".
    if (mapping.sourceFormat === "json") {
      return normalizedValue;
    }
    return this.coerceValue(normalizedValue, mapping.type);
  }

  private normalizeSourceValue(value: unknown, mapping: TransformFieldMapping): unknown {
    switch (mapping.sourceFormat ?? "plain") {
      case "json": {
        if (typeof value !== "string") {
          return this.ensureJsonArray(this.sanitizeJsonForTypesense(value));
        }

        const trimmed = value.trim();
        if (trimmed === "") {
          return null;
        }

        try {
          return this.ensureJsonArray(this.sanitizeJsonForTypesense(JSON.parse(trimmed)));
        } catch {
          // JSON parse failed — commonly legacy CSV text like "1,2,3".
          // Split by comma and coerce individual items.
          const items = trimmed
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);

          return items.map((s) => {
            const n = Number(s);
            return Number.isNaN(n) ? s : n;
          });
        }
      }
      case "csv":
        return Array.isArray(value)
          ? value
          : String(value)
              .split(mapping.arraySeparator ?? ",")
              .map((item) => item.trim())
              .filter(Boolean);
      case "datetime": {
        const strValue = String(value).trim();
        if (strValue === "") {
          return null;
        }
        const dateValue = value instanceof Date ? value : new Date(strValue);
        const epoch = dateValue.getTime();
        if (Number.isNaN(epoch)) {
          return null;
        }

        return mapping.timestampResolution === "milliseconds" ? epoch : Math.floor(epoch / 1000);
      }
      case "plain":
      default:
        return value;
    }
  }

  private coerceValue(value: unknown, type: TypesenseFieldType): unknown {
    switch (type) {
      case "string":
      case "string*":
      case "image":
        return String(value);
      case "int32":
      case "int64":
        return this.toNumber(value, true);
      case "float":
        return this.toNumber(value, false);
      case "bool":
        return this.toBoolean(value);
      case "string[]":
        return this.toArray(value).map((item) => String(item));
      case "int32[]":
      case "int64[]":
        return this.toArray(value).map((item) => this.toNumber(item, true));
      case "float[]":
        return this.toArray(value).map((item) => this.toNumber(item, false));
      case "bool[]":
        return this.toArray(value).map((item) => this.toBoolean(item));
      case "object":
        return this.toObject(value);
      case "object[]":
        return this.toArray(value).map((item) => this.toObject(item));
      case "auto":
      case "geopoint":
      case "geopolygon":
      case "geopoint[]":
      default:
        return value;
    }
  }

  private toArray(value: unknown): unknown[] {
    if (Array.isArray(value)) {
      return value;
    }

    return [value];
  }

  private toBoolean(value: unknown): boolean {
    if (typeof value === "boolean") {
      return value;
    }

    if (typeof value === "number") {
      return value !== 0;
    }

    return TRUE_VALUES.has(String(value).trim().toLowerCase());
  }

  private toNumber(value: unknown, integer: boolean): number | null {
    const parsed = Number(value);
    if (Number.isNaN(parsed)) {
      return null;
    }

    return integer ? Math.trunc(parsed) : parsed;
  }

  private toObject(value: unknown): Record<string, unknown> {
    if (typeof value === "string") {
      return JSON.parse(value) as Record<string, unknown>;
    }

    if (typeof value === "object" && value !== null && !Array.isArray(value)) {
      return value as Record<string, unknown>;
    }

    throw new Error(`Cannot coerce value \"${String(value)}\" to object`);
  }

  private sanitizeJsonForTypesense(value: unknown): unknown {
    if (Array.isArray(value)) {
      if (value.length === 0) {
        return value;
      }

      const sanitizedItems = value.map((item) => this.sanitizeJsonForTypesense(item));

      const kinds = new Set(sanitizedItems.map((item) => this.jsonKind(item)));
      if (kinds.size <= 1) {
        return sanitizedItems;
      }

      // Typesense nested typing is strict for array element types.
      // Normalize mixed arrays to string[] to avoid schema conflicts.
      return sanitizedItems.map((item) => (typeof item === "string" ? item : JSON.stringify(item)));
    }

    if (typeof value === "object" && value !== null) {
      const output: Record<string, unknown> = {};
      for (const [key, nested] of Object.entries(value as Record<string, unknown>)) {
        output[key] = this.sanitizeJsonForTypesense(nested);
      }
      return output;
    }

    return value;
  }

  private jsonKind(value: unknown): string {
    if (value === null) return "null";
    if (Array.isArray(value)) return "array";
    return typeof value;
  }

  /** Wrap primitive scalars in an array so json-serialised columns produce consistent array types for Typesense. */
  private ensureJsonArray(value: unknown): unknown {
    if (value === null || value === undefined) return value;
    if (Array.isArray(value) || typeof value === "object") return value;
    return [value];
  }
}