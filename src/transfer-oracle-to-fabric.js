#!/usr/bin/env node
import { connectChildServer } from "./child-client.js";
import { loadConfig, resolveServerEnv } from "./config.js";

const DEFAULT_FORMAT = "csv";

function parseArgs(argv) {
  const args = {};

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      continue;
    }

    const key = token.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith("--")) {
      args[key] = true;
      continue;
    }

    args[key] = next;
    index += 1;
  }

  return args;
}

function usage() {
  return `Usage:
  npm run transfer -- --table FINANCIAL_CUSTOMERS --name financial_customers
  npm run transfer -- --query "select * from financial_customers where risk_rating = 'HIGH'" --name high_risk_customers

Options:
  --table          Oracle table name to transfer.
  --query          Oracle SQL query to transfer. Use either --table or --query.
  --name           Transfer name used in OneLake path.
  --format         csv or jsonl. Default: csv.
  --lakehouse-id   Fabric Lakehouse item ID. Defaults to FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID.
  --path           OneLake base path. Default: Files/transfers.
`;
}

function nowRunId() {
  return new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
}

function text(result) {
  return result.content?.find((part) => part.type === "text")?.text || "";
}

function parseJsonText(result) {
  const value = text(result);
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function parseCsv(csv) {
  const cleanCsv = csv
    .split(/\r?\n/)
    .filter((line) => !/^\d+\s+rows?\s+selected\.?\s*$/i.test(line.trim()))
    .join("\n");
  const rows = [];
  let row = [];
  let cell = "";
  let inQuotes = false;

  for (let index = 0; index < cleanCsv.length; index += 1) {
    const char = cleanCsv[index];
    const next = cleanCsv[index + 1];

    if (char === '"' && inQuotes && next === '"') {
      cell += '"';
      index += 1;
      continue;
    }

    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (char === "," && !inQuotes) {
      row.push(cell);
      cell = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && next === "\n") {
        index += 1;
      }
      row.push(cell);
      if (row.some((value) => value !== "")) {
        rows.push(row);
      }
      row = [];
      cell = "";
      continue;
    }

    cell += char;
  }

  if (cell || row.length > 0) {
    row.push(cell);
    if (row.some((value) => value !== "")) {
      rows.push(row);
    }
  }

  return rows;
}

function csvToJsonLines(csv) {
  const rows = parseCsv(csv);
  if (rows.length === 0) {
    return "";
  }

  const [headers, ...dataRows] = rows;
  return dataRows
    .map((row) => {
      const record = {};
      headers.forEach((header, index) => {
        const raw = row[index] ?? "";
        record[header.toLowerCase()] = raw === "" ? null : coerceValue(raw);
      });
      return JSON.stringify(record);
    })
    .join("\n") + "\n";
}

function cleanSqlclCsv(csv) {
  return csv
    .split(/\r?\n/)
    .filter((line) => !/^\d+\s+rows?\s+selected\.?\s*$/i.test(line.trim()))
    .join("\n")
    .trimEnd() + "\n";
}

function coerceValue(value) {
  if (/^-?\d+(\.\d+)?$/.test(value)) {
    return Number(value);
  }

  return value;
}

function sanitizeName(name) {
  return name.toLowerCase().replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
}

function normalizeSql(args) {
  if (args.query && args.table) {
    throw new Error("Use either --table or --query, not both.");
  }

  if (args.query) {
    return {
      sourceType: "query",
      sql: args.query,
      sourceName: args.name || "oracle_query"
    };
  }

  if (args.table) {
    const table = args.table.toUpperCase();
    if (!/^[A-Z][A-Z0-9_$#]*$/.test(table)) {
      throw new Error(`Invalid table name: ${args.table}`);
    }

    return {
      sourceType: "table",
      sql: `select * from ${table}`,
      sourceName: args.name || table.toLowerCase(),
      table
    };
  }

  throw new Error("Provide --table or --query.");
}

async function oracleExport(oracleConfig, connectionName, transfer) {
  const oracle = await connectChildServer("oracle", oracleConfig);

  try {
    await oracle.client.callTool({
      name: "connect",
      arguments: {
        connection_name: connectionName,
        model: "codex-transfer"
      }
    });

    const dataResult = await oracle.client.callTool({
      name: "sql_run",
      arguments: {
        model: "codex-transfer",
        sql: transfer.sql
      }
    });

    let schema = null;
    if (transfer.table) {
      const schemaResult = await oracle.client.callTool({
        name: "sql_run",
        arguments: {
          model: "codex-transfer",
          sql: `select column_id,
                       column_name,
                       data_type,
                       data_length,
                       data_precision,
                       data_scale,
                       nullable
                  from user_tab_columns
                 where table_name = '${transfer.table}'
                 order by column_id`
        }
      });
      schema = text(schemaResult);
    }

    return {
      csv: text(dataResult).trimEnd() + "\n",
      schema
    };
  } finally {
    await oracle.client.close();
  }
}

async function fabricUpload(fabricConfig, fabricEnv, payloads) {
  const tenant = fabricEnv.FABRIC_TENANT_ID || fabricEnv.FABRIC_TENANT_DOMAIN;
  const workspaceId = fabricEnv.FABRIC_WORKSPACE_ID;
  const lakehouseId = payloads.lakehouseId;

  if (!tenant) {
    throw new Error("Missing FABRIC_TENANT_ID or FABRIC_TENANT_DOMAIN.");
  }

  if (!workspaceId) {
    throw new Error("Missing FABRIC_WORKSPACE_ID.");
  }

  if (!lakehouseId) {
    throw new Error("Missing Fabric Lakehouse ID. Provide --lakehouse-id or FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID.");
  }

  const fabric = await connectChildServer("fabric", fabricConfig);

  try {
    const uploads = [];
    for (const payload of payloads.files) {
      const result = await fabric.client.callTool({
        name: "onelake_upload_file",
        arguments: {
          tenant,
          "auth-method": "Credential",
          "workspace-id": workspaceId,
          "item-id": lakehouseId,
          "file-path": payload.path,
          content: payload.content,
          overwrite: true,
          "content-type": payload.contentType
        }
      });
      uploads.push(parseJsonText(result));
    }

    const listing = await fabric.client.callTool({
      name: "onelake_list_files",
      arguments: {
        tenant,
        "auth-method": "Credential",
        "workspace-id": workspaceId,
        "item-id": lakehouseId,
        path: payloads.basePath,
        recursive: true,
        format: "json"
      }
    });

    return {
      uploads,
      listing: parseJsonText(listing)
    };
  } finally {
    await fabric.client.close();
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log(usage());
    return;
  }

  const { config } = loadConfig();
  const oracleConfig = config.servers.oracle;
  const fabricConfig = config.servers.fabric;
  const oracleEnv = resolveServerEnv(oracleConfig);
  const fabricEnv = resolveServerEnv(fabricConfig);
  const connectionName = oracleEnv.ORACLE_SQLCL_CONNECTION_NAME;
  const format = (args.format || DEFAULT_FORMAT).toLowerCase();
  const runId = nowRunId();
  const transfer = normalizeSql(args);
  const name = sanitizeName(args.name || transfer.sourceName);
  const basePath = `${args.path || "Files/transfers"}/${name}/${runId}`;
  const lakehouseId = args["lakehouse-id"] || fabricEnv.FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID;

  if (!["csv", "jsonl"].includes(format)) {
    throw new Error("--format must be csv or jsonl.");
  }

  if (!connectionName) {
    throw new Error("Missing ORACLE_SQLCL_CONNECTION_NAME.");
  }

  console.log(`Oracle to Fabric offline upload: ${name}`);
  console.log(`Run ID: ${runId}`);
  console.log(`Source: ${transfer.sourceType}${transfer.table ? ` ${transfer.table}` : ""}`);

  const oracleData = await oracleExport(oracleConfig, connectionName, transfer);
  const cleanCsv = cleanSqlclCsv(oracleData.csv);
  const dataContent = format === "jsonl" ? csvToJsonLines(cleanCsv) : cleanCsv;
  const dataExtension = format === "jsonl" ? "jsonl" : "csv";
  const rows = Math.max(parseCsv(cleanCsv).length - 1, 0);

  const manifest = {
    runId,
    source: {
      type: transfer.sourceType,
      table: transfer.table || null,
      sql: transfer.sql
    },
    target: {
      lakehouseId,
      basePath,
      dataFile: `${basePath}/data.${dataExtension}`
    },
    format,
    rows,
    transferredAt: new Date().toISOString()
  };

  const files = [
    {
      path: `${basePath}/data.${dataExtension}`,
      content: dataContent,
      contentType: format === "jsonl" ? "application/x-ndjson" : "text/csv"
    },
    {
      path: `${basePath}/manifest.json`,
      content: JSON.stringify(manifest, null, 2),
      contentType: "application/json"
    }
  ];

  if (oracleData.schema) {
    files.push({
      path: `${basePath}/schema.csv`,
      content: oracleData.schema.trimEnd() + "\n",
      contentType: "text/csv"
    });
  }

  const fabricResult = await fabricUpload(fabricConfig, fabricEnv, {
    lakehouseId,
    basePath,
    files
  });

  console.log(`Rows transferred: ${rows}`);
  console.log(`Fabric Lakehouse ID: ${lakehouseId}`);
  console.log(`OneLake path: ${basePath}`);
  for (const upload of fabricResult.uploads) {
    console.log(`Uploaded: ${upload?.results?.blobPath || upload?.results?.path || "unknown"} (${upload?.status} ${upload?.message})`);
  }
  console.log("Offline upload to Fabric complete.");
}

main().catch((error) => {
  console.error(`Transfer failed: ${error.message}`);
  process.exit(1);
});
