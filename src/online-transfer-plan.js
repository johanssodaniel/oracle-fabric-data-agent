#!/usr/bin/env node
import { connectChildServer } from "./child-client.js";
import { loadConfig, resolveServerEnv } from "./config.js";

const MCP_TIMEOUT_MS = 180000;

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
  npm run online:plan
  npm run online:plan -- --table FINANCIAL_CUSTOMERS --name financial_customers_online

Options:
  --table          Oracle table name to copy online. Default: FINANCIAL_CUSTOMERS.
  --name           Plan name used for Fabric item names and OneLake paths.
  --lakehouse-id   Fabric Lakehouse item ID. Defaults to FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID.
  --path           OneLake base path. Default: Files/online-transfer-plans.
  --oracle-fabric-connection-id      Existing Fabric Oracle connection ID, if known.
  --lakehouse-fabric-connection-id   Existing Fabric Lakehouse connection ID, if required by the pipeline.
  --gateway-id                       Existing Fabric gateway ID, if using gateway-based Oracle connectivity.
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

function callTool(client, params) {
  return client.callTool(params, undefined, {
    timeout: MCP_TIMEOUT_MS,
    resetTimeoutOnProgress: true
  });
}

function parseCsv(csv) {
  const lines = csv
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !/^\d+\s+rows?\s+selected\.?\s*$/i.test(line));

  if (lines.length < 2) {
    return [];
  }

  return lines.slice(1).map((line) => line.split(",").map((cell) => cell.replace(/^"|"$/g, "")));
}

function sanitizeName(name) {
  return name.toLowerCase().replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
}

function normalizeTable(table) {
  const normalized = table.toUpperCase();
  if (!/^[A-Z][A-Z0-9_$#]*$/.test(normalized)) {
    throw new Error(`Invalid table name: ${table}`);
  }

  return normalized;
}

async function inspectOracle(oracleConfig, connectionName, table) {
  const oracle = await connectChildServer("oracle", oracleConfig);

  try {
    const connectResult = await callTool(oracle.client, {
      name: "connect",
      arguments: {
        connection_name: connectionName,
        model: "codex-online-plan"
      }
    });
    ensureOracleConnected(connectResult);

    const rowCountResult = await callTool(oracle.client, {
      name: "sql_run",
      arguments: {
        model: "codex-online-plan",
        sql: `select count(*) as row_count from ${table}`
      }
    });

    const schemaResult = await callTool(oracle.client, {
      name: "sql_run",
      arguments: {
        model: "codex-online-plan",
        sql: `select column_id,
                     column_name,
                     data_type,
                     data_length,
                     data_precision,
                     data_scale,
                     nullable
                from user_tab_columns
               where table_name = '${table}'
               order by column_id`
      }
    });

    const rowCountRows = parseCsv(text(rowCountResult));
    const rowCount = Number(rowCountRows[0]?.[0] || 0);

    return {
      table,
      rowCount,
      schemaCsv: text(schemaResult).trimEnd() + "\n"
    };
  } finally {
    await oracle.client.close();
  }
}

async function createFabricPlan(fabricConfig, fabricEnv, plan) {
  const tenant = fabricEnv.FABRIC_TENANT_ID || fabricEnv.FABRIC_TENANT_DOMAIN;
  const workspaceId = fabricEnv.FABRIC_WORKSPACE_ID;

  if (!tenant) {
    throw new Error("Missing FABRIC_TENANT_ID or FABRIC_TENANT_DOMAIN.");
  }

  if (!workspaceId) {
    throw new Error("Missing FABRIC_WORKSPACE_ID.");
  }

  if (!plan.lakehouseId) {
    throw new Error("Missing Fabric Lakehouse ID. Provide --lakehouse-id or FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID.");
  }

  const commonArgs = {
    tenant,
    "auth-method": "Credential",
    "workspace-id": workspaceId
  };

  const fabric = await connectChildServer("fabric", fabricConfig);

  try {
    let pipeline = null;
    try {
      const result = await callTool(fabric.client, {
        name: "core_create-item",
        arguments: {
          ...commonArgs,
          "display-name": plan.pipelineName,
          "item-type": "DataPipeline",
          description: "Online Oracle to Fabric Data Factory copy shell. Configure the Oracle connector and gateway if required in Fabric."
        }
      });
      pipeline = parseJsonText(result);
    } catch (error) {
      pipeline = { warning: error.message };
    }

    const files = [
      {
        path: `${plan.basePath}/online_copy_blueprint.json`,
        content: JSON.stringify(plan.blueprint, null, 2),
        contentType: "application/json"
      },
      {
        path: `${plan.basePath}/oracle_source_schema.csv`,
        content: plan.oracle.schemaCsv,
        contentType: "text/csv"
      },
      {
        path: `${plan.basePath}/fabric_data_factory_steps.md`,
        content: dataFactorySteps(plan),
        contentType: "text/markdown"
      },
      {
        path: `${plan.basePath}/service_side_copy_runbook.md`,
        content: serviceSideCopyRunbook(plan),
        contentType: "text/markdown"
      }
    ];

    const uploads = [];
    for (const file of files) {
      const upload = await callTool(fabric.client, {
        name: "onelake_upload_file",
        arguments: {
          ...commonArgs,
          "item-id": plan.lakehouseId,
          "file-path": file.path,
          content: file.content,
          overwrite: true,
          "content-type": file.contentType
        }
      });
      uploads.push(parseJsonText(upload));
    }

    return { pipeline, uploads };
  } finally {
    await fabric.client.close();
  }
}

function buildBlueprint({
  runId,
  name,
  table,
  oracle,
  workspaceId,
  lakehouseId,
  oracleFabricConnectionId,
  lakehouseFabricConnectionId,
  gatewayId
}) {
  return {
    runId,
    name,
    intent: "Fabric-service-side Oracle to OneLake transfer. No Oracle rows are exported through the local agent.",
    source: {
      type: "Oracle database",
      table,
      sql: `select * from ADMIN.${table}`,
      validatedByOracleMcp: {
        rowCount: oracle.rowCount,
        schemaFile: "oracle_source_schema.csv"
      }
    },
    fabric: {
      workspaceId,
      targetLakehouseId: lakehouseId,
      oracleFabricConnectionId: oracleFabricConnectionId || "<fabric-oracle-connection-id>",
      lakehouseFabricConnectionId: lakehouseFabricConnectionId || "<fabric-lakehouse-connection-id-if-required>",
      gatewayId: gatewayId || "<optional-gateway-id>",
      recommendedItem: "Copy Job or Data Pipeline Copy activity",
      destination: {
        lakehouseTableOrFolder: table.toLowerCase(),
        loadMode: "Full load for demo; incremental or CDC for production."
      }
    },
    requiredRuntime: {
      gateway: "Optional when the Oracle endpoint is directly reachable from Fabric; required when using gateway-based Oracle connectivity.",
      oracleClient: "Oracle Client for Microsoft Tools on the gateway host when using the gateway path.",
      authentication: "Fabric Oracle database connector with approved Oracle credentials"
    },
    agentUse: {
      oracleMcp: "Discovery, schema validation, and row-count checks.",
      fabricMcp: "Create Fabric item shell, upload blueprint assets, and verify OneLake output after the Fabric job runs."
    }
  };
}

function ensureOracleConnected(result) {
  const message = text(result);
  if (/not established|failed|error/i.test(message)) {
    throw new Error(`Oracle MCP connection failed: ${message.trim()}`);
  }
}

function dataFactorySteps(plan) {
  return `# Fabric Data Factory Online Copy Steps

This plan avoids local CSV/JSON export. Fabric should perform the data movement.

## Source Validation

- Oracle table: \`${plan.table}\`
- Row count observed through Oracle MCP: \`${plan.oracle.rowCount}\`
- Schema file: \`oracle_source_schema.csv\`

## Fabric Setup

1. In the Fabric workspace, create a Copy Job or Data Pipeline Copy activity.
2. Create or select an Oracle database connection in Fabric.
3. If you use a gateway-based connection, select the gateway configured with Oracle Client for Microsoft Tools.
4. Use this source query:

\`\`\`sql
select *
from ADMIN.${plan.table}
\`\`\`

5. Set the target to the Lakehouse:

\`\`\`text
Lakehouse item ID: ${plan.lakehouseId}
Destination: ${plan.table.toLowerCase()}
\`\`\`

6. Run the Fabric job from the Fabric service.
7. Use Fabric MCP to verify the destination table or OneLake files.

## Event Talk Track

The agent validated Oracle through Oracle MCP and prepared the Fabric-side plan through Fabric MCP. The actual data copy is performed online by Fabric Data Factory through the Oracle connector, not by a laptop export/import. Use gateway connectivity when required by the source network or tenant policy.
`;
}

function serviceSideCopyRunbook(plan) {
  return `# Service-Side Oracle To OneLake Copy Runbook

This runbook is for a Fabric Data Factory Copy Job or Copy activity. It does not export Oracle rows through the laptop.

## Validated Source

- Oracle table: \`${plan.table}\`
- Oracle row count from Oracle MCP: \`${plan.oracle.rowCount}\`
- Source SQL:

\`\`\`sql
select *
from ADMIN.${plan.table}
\`\`\`

## Fabric Target

- Workspace ID: \`${plan.workspaceId}\`
- Lakehouse item ID: \`${plan.lakehouseId}\`
- Destination table/folder: \`${plan.table.toLowerCase()}\`

## Connection Settings To Fill In

- Oracle Fabric connection ID: \`${plan.oracleFabricConnectionId || "<fabric-oracle-connection-id>"}\`
- Lakehouse Fabric connection ID, if required: \`${plan.lakehouseFabricConnectionId || "<fabric-lakehouse-connection-id-if-required>"}\`
- Gateway ID, if using gateway connectivity: \`${plan.gatewayId || "<optional-gateway-id>"}\`

## Recommended Service-Side Setup

1. In Fabric Data Factory, create a Copy Job.
2. Select the Oracle database connector as the source.
3. Choose the existing Oracle Fabric connection, or create one.
4. If using gateway connectivity, select the gateway host with Oracle Client for Microsoft Tools installed.
5. Use the source query above.
6. Choose the Lakehouse as destination.
7. Write to \`${plan.table.toLowerCase()}\`.
8. Use full load for the event demo. Use incremental copy or CDC later for production.
9. Run the Copy Job in Fabric.
10. Ask the VS Code agent to verify the destination through Fabric MCP.

## Why The Agent Still Matters

The agent can prove source metadata through Oracle MCP, create or document Fabric artifacts through Fabric MCP, and verify OneLake output after Fabric runs the service-side copy.
`;
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
  const table = normalizeTable(args.table || "FINANCIAL_CUSTOMERS");
  const runId = nowRunId();
  const name = sanitizeName(args.name || `${table}_online`.toLowerCase());
  const basePath = `${args.path || "Files/online-transfer-plans"}/${name}/${runId}`;
  const lakehouseId = args["lakehouse-id"] || fabricEnv.FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID;
  const workspaceId = fabricEnv.FABRIC_WORKSPACE_ID;
  const oracleFabricConnectionId = args["oracle-fabric-connection-id"];
  const lakehouseFabricConnectionId = args["lakehouse-fabric-connection-id"];
  const gatewayId = args["gateway-id"];

  if (!connectionName) {
    throw new Error("Missing ORACLE_SQLCL_CONNECTION_NAME.");
  }

  console.log("Online Oracle to Fabric transfer plan");
  console.log(`Run ID: ${runId}`);
  console.log(`Oracle source table: ${table}`);
  console.log("No Oracle rows will be exported locally.");

  const oracle = await inspectOracle(oracleConfig, connectionName, table);
  const blueprint = buildBlueprint({
    runId,
    name,
    table,
    oracle,
    workspaceId,
    lakehouseId,
    oracleFabricConnectionId,
    lakehouseFabricConnectionId,
    gatewayId
  });
  const plan = {
    runId,
    name,
    table,
    oracle,
    workspaceId,
    lakehouseId,
    oracleFabricConnectionId,
    lakehouseFabricConnectionId,
    gatewayId,
    basePath,
    pipelineName: `oracle_to_onelake_online_${name}_${runId}`,
    blueprint
  };

  const fabric = await createFabricPlan(fabricConfig, fabricEnv, plan);

  console.log(`Oracle row count checked: ${oracle.rowCount}`);
  console.log(`Fabric Lakehouse ID: ${lakehouseId}`);
  console.log(`OneLake plan path: ${basePath}`);
  if (fabric.pipeline?.warning) {
    console.log(`Pipeline shell: not created (${fabric.pipeline.warning})`);
  } else {
    const item = fabric.pipeline?.results?.item || fabric.pipeline?.results || fabric.pipeline?.item;
    console.log(`Pipeline shell: ${plan.pipelineName} (${item?.id || item?.itemId || "created, id not returned"})`);
  }
  for (const upload of fabric.uploads) {
    console.log(`Uploaded plan asset: ${upload?.results?.blobPath || upload?.results?.path || "unknown"} (${upload?.status} ${upload?.message})`);
  }
  console.log("Online transfer plan complete.");
}

main().catch((error) => {
  console.error(`Online transfer plan failed: ${error.message}`);
  process.exit(1);
});
