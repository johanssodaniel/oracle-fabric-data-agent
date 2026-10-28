#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

import { connectChildServer } from "./child-client.js";
import { loadConfig, resolveServerEnv } from "./config.js";
import {
  createOrUpdateNotebookDefinition,
  hasNotebookDefinitionAuth
} from "./fabric-notebook-definition.js";

const TABLE_NAME = "FINANCIAL_CUSTOMERS";
const MCP_TIMEOUT_MS = 180000;

function runId() {
  return new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
}

function section(title) {
  console.log(`\n${title}`);
  console.log("-".repeat(title.length));
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

function tableFromCsv(csv) {
  return csv
    .trim()
    .split(/\r?\n/)
    .filter((line) => line && !/^\d+\s+rows?\s+selected\.?\s*$/i.test(line.trim()))
    .map((line) => line.split(",").map((cell) => cell.replace(/^"|"$/g, "")));
}

function printRows(csv, limit = 8) {
  const rows = tableFromCsv(csv).slice(0, limit);
  for (const row of rows) {
    console.log(`  ${row.join("  ")}`);
  }
}

function cleanSqlclCsv(csv) {
  return csv
    .split(/\r?\n/)
    .filter((line) => !/^\d+\s+rows?\s+selected\.?\s*$/i.test(line.trim()))
    .join("\n")
    .trimEnd() + "\n";
}

function parseSqlScript(script) {
  return script
    .split(/\r?\n/)
    .filter((line) => !/^\s*set\s+/i.test(line))
    .join("\n")
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean);
}

async function oracleSql(oracle, sql) {
  const result = await callTool(oracle.client, {
    name: "sql_run",
    arguments: {
      model: "codex-financial-demo",
      sql
    }
  });
  return text(result);
}

async function resetAndCreateFinancialTable(oracle) {
  try {
    await oracleSql(oracle, `drop table ${TABLE_NAME} purge`);
    console.log(`Dropped existing ${TABLE_NAME}.`);
  } catch (error) {
    if (!/ORA-00942|table or view does not exist/i.test(error.message)) {
      throw error;
    }
    console.log(`No existing ${TABLE_NAME} table found.`);
  }

  const sqlPath = path.resolve("sql/financial_customers.sql");
  const statements = parseSqlScript(fs.readFileSync(sqlPath, "utf8"));

  for (const statement of statements) {
    await oracleSql(oracle, statement);
  }

  console.log(`Created and loaded ${TABLE_NAME}.`);
}

async function gatherStats(oracle) {
  try {
    await oracleSql(
      oracle,
      `begin
         dbms_stats.gather_table_stats(
           ownname => user,
           tabname => '${TABLE_NAME}',
           cascade => true
         );
       end;`
    );
    return "Optimizer statistics gathered.";
  } catch (error) {
    return `Optimizer statistics gather failed: ${error.message}`;
  }
}

async function oracleInspection(oracle) {
  const count = await oracleSql(oracle, `select count(*) as customer_count from ${TABLE_NAME}`);
  const sample = await oracleSql(
    oracle,
    `select customer_number,
            legal_name,
            customer_type,
            country_code,
            kyc_status,
            risk_rating,
            assets_under_management_usd
       from ${TABLE_NAME}
      order by customer_id
      fetch first 6 rows only`
  );
  const riskSummary = await oracleSql(
    oracle,
    `select risk_rating,
            count(*) as customer_count,
            sum(assets_under_management_usd) as total_aum_usd
       from ${TABLE_NAME}
      group by risk_rating
      order by risk_rating`
  );

  return { count, sample, riskSummary };
}

async function oracleOptimizationCheck(oracle) {
  const statsMessage = await gatherStats(oracle);
  const tableStats = await oracleSql(
    oracle,
    `select table_name,
            num_rows,
            blocks,
            to_char(last_analyzed, 'YYYY-MM-DD HH24:MI:SS') as last_analyzed
       from user_tables
      where table_name = '${TABLE_NAME}'`
  );
  const indexes = await oracleSql(
    oracle,
    `select i.index_name,
            i.uniqueness,
            listagg(c.column_name, ', ') within group (order by c.column_position) as columns
       from user_indexes i
       join user_ind_columns c
         on c.index_name = i.index_name
      where i.table_name = '${TABLE_NAME}'
      group by i.index_name, i.uniqueness
      order by i.index_name`
  );

  const explainTarget = `select *
                           from ${TABLE_NAME}
                          where risk_rating = 'HIGH'
                            and kyc_status = 'REVIEW_REQUIRED'`;
  await oracleSql(oracle, `explain plan for ${explainTarget}`);
  const plan = await oracleSql(
    oracle,
    `select plan_table_output
       from table(dbms_xplan.display(null, null, 'BASIC +PREDICATE'))`
  );

  return {
    statsMessage,
    tableStats,
    indexes,
    plan,
    recommendation: "For a larger production table, consider a workload-driven index on (risk_rating, kyc_status) for risk review filters. The demo table is small, so a full scan is acceptable."
  };
}

async function oracleExportCsv(oracle) {
  const csv = await oracleSql(
    oracle,
    `select customer_id,
            customer_number,
            legal_name,
            customer_type,
            country_code,
            to_char(onboarding_date, 'YYYY-MM-DD') as onboarding_date,
            kyc_status,
            risk_rating,
            annual_income_usd,
            assets_under_management_usd,
            credit_score,
            is_pep,
            aml_watchlist_status,
            preferred_channel,
            relationship_manager
       from ${TABLE_NAME}
      order by customer_id`
  );

  const schema = await oracleSql(
    oracle,
    `select column_id,
            column_name,
            data_type,
            data_length,
            data_precision,
            data_scale,
            nullable
       from user_tab_columns
      where table_name = '${TABLE_NAME}'
      order by column_id`
  );

  return {
    csv: cleanSqlclCsv(csv),
    schema: cleanSqlclCsv(schema)
  };
}

function runbook({ run, notebookName, notebookId, sourcePath, basePath }) {
  return `# Financial Customers End-to-End Runbook

Run ID: ${run}

## Storyline

1. Created the Oracle \`FINANCIAL_CUSTOMERS\` table.
2. Inspected customer records and risk summary through Oracle MCP.
3. Checked optimizer statistics, existing indexes, and an explain plan.
4. Performed an offline upload to Fabric OneLake through Fabric MCP.
5. Created a Fabric Notebook with Spark code that loads the data and saves it as Delta.
6. The notebook runs Spark ML logistic regression and KMeans segmentation.

## Fabric Assets

- Uploaded source CSV: \`${sourcePath}\`
- Asset folder: \`${basePath}\`
- Notebook: \`${notebookName}\`
- Notebook ID: \`${notebookId}\`
- Notebook content: embedded \`notebook-content.ipynb\` payload

## Notebook Outputs

\`\`\`text
financial_customers
financial_customer_ml_scores
financial_customer_segments
\`\`\`

## Notebook Execution

The notebook content is already present. Open the generated Fabric Notebook, attach the Lakehouse if needed, and run it.
`;
}

async function fabricUploadAndNotebook(fabricConfig, fabricEnv, payload) {
  const tenant = fabricEnv.FABRIC_TENANT_ID || fabricEnv.FABRIC_TENANT_DOMAIN;
  const workspaceId = fabricEnv.FABRIC_WORKSPACE_ID;
  const lakehouseId = fabricEnv.FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID;

  if (!tenant) {
    throw new Error("Missing FABRIC_TENANT_ID or FABRIC_TENANT_DOMAIN.");
  }

  if (!workspaceId) {
    throw new Error("Missing FABRIC_WORKSPACE_ID.");
  }

  if (!lakehouseId) {
    throw new Error("Missing FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID.");
  }

  const commonArgs = {
    tenant,
    "auth-method": "Credential",
    "workspace-id": workspaceId
  };

    const fabric = await connectChildServer("fabric", fabricConfig);

  try {
    const notebookName = `financial_customers_end_to_end_${payload.run}`;
    const sourcePath = `${payload.basePath}/financial_customers.csv`;
    let notebookId = null;

    if (!hasNotebookDefinitionAuth(fabricEnv)) {
      throw new Error("Missing Fabric REST auth for notebook content upload. Use az login, FABRIC_ACCESS_TOKEN, or service-principal settings with Notebook.ReadWrite.All or Item.ReadWrite.All.");
    }

    const notebook = await createOrUpdateNotebookDefinition({
      workspaceId,
      displayName: notebookName,
      lakehouseId,
      sourcePath,
      env: fabricEnv
    });
    notebookId = notebook.id;
    if (!notebookId) {
      throw new Error("Fabric notebook definition upload completed, but Fabric did not return a Notebook ID.");
    }

    const assets = [
      {
        path: sourcePath,
        content: payload.csv,
        contentType: "text/csv"
      },
      {
        path: "Files/financial_customers.csv",
        content: payload.csv,
        contentType: "text/csv"
      },
      {
        path: `${payload.basePath}/oracle_schema.csv`,
        content: payload.schema,
        contentType: "text/csv"
      },
      {
        path: `${payload.basePath}/end_to_end_runbook.md`,
        content: runbook({
          run: payload.run,
          notebookName,
          notebookId,
          sourcePath,
          basePath: payload.basePath
        }),
        contentType: "text/markdown"
      },
      {
        path: `${payload.basePath}/oracle_optimization_report.txt`,
        content: payload.optimizationReport,
        contentType: "text/plain"
      }
    ];

    const uploads = [];
    for (const asset of assets) {
      const upload = await callTool(fabric.client, {
        name: "onelake_upload_file",
        arguments: {
          ...commonArgs,
          "item-id": lakehouseId,
          "file-path": asset.path,
          content: asset.content,
          overwrite: true,
          "content-type": asset.contentType
        }
      });
      uploads.push(parseJsonText(upload));
    }

    return {
      notebookName,
      notebookId,
      lakehouseId,
      sourcePath,
      uploads
    };
  } finally {
    await fabric.client.close();
  }
}

async function main() {
  const { config } = loadConfig();
  const oracleConfig = config.servers.oracle;
  const fabricConfig = config.servers.fabric;
  const oracleEnv = resolveServerEnv(oracleConfig);
  const fabricEnv = resolveServerEnv(fabricConfig);
  const connectionName = oracleEnv.ORACLE_SQLCL_CONNECTION_NAME;
  const run = runId();

  if (!connectionName) {
    throw new Error("Missing ORACLE_SQLCL_CONNECTION_NAME.");
  }

  console.log("Financial Customers End-to-End MCP Demo");
  console.log(`Run ID: ${run}`);

  const oracle = await connectChildServer("oracle", oracleConfig);
  let inspection;
  let optimization;
  let exportData;

  try {
    await callTool(oracle.client, {
      name: "connect",
      arguments: {
        connection_name: connectionName,
        model: "codex-financial-demo"
      }
    });

    section("1. Create financial table in Oracle");
    await resetAndCreateFinancialTable(oracle);

    section("2. Look at table contents");
    inspection = await oracleInspection(oracle);
    printRows(inspection.count);
    printRows(inspection.sample);

    section("Risk summary");
    printRows(inspection.riskSummary);

    section("3. Check optimizer and index issues");
    optimization = await oracleOptimizationCheck(oracle);
    console.log(`  ${optimization.statsMessage}`);
    printRows(optimization.tableStats);
    printRows(optimization.indexes);
    console.log(`  Recommendation: ${optimization.recommendation}`);

    exportData = await oracleExportCsv(oracle);
  } finally {
    await oracle.client.close();
  }

  const optimizationReport = [
    "TABLE STATS",
    optimization.tableStats,
    "",
    "INDEXES",
    optimization.indexes,
    "",
    "EXPLAIN PLAN",
    optimization.plan,
    "",
    "RECOMMENDATION",
    optimization.recommendation
  ].join("\n");

  section("4. Offline upload to Fabric");
  const fabric = await fabricUploadAndNotebook(fabricConfig, fabricEnv, {
    run,
    basePath: `Files/financial-demo/${run}`,
    csv: exportData.csv,
    schema: exportData.schema,
    optimizationReport
  });
  console.log(`  Lakehouse ID: ${fabric.lakehouseId}`);
  console.log(`  Uploaded source: ${fabric.sourcePath}`);

  section("5. Fabric notebook for Spark Delta load");
  console.log(`  Notebook: ${fabric.notebookName}`);
  console.log(`  Notebook ID: ${fabric.notebookId}`);
  console.log("  Notebook content: embedded notebook-content.ipynb payload");
  console.log("  The notebook loads the uploaded CSV and saves financial_customers as a Delta table.");

  section("6. Simple ML algorithm inside notebook");
  console.log("  The notebook trains LogisticRegression for high-risk scoring.");
  console.log("  The notebook runs KMeans for customer segmentation.");
  console.log("  Expected Delta outputs: financial_customers, financial_customer_ml_scores, financial_customer_segments.");

  section("Demo Result");
  console.log("End-to-end assets are ready. Open the generated Fabric Notebook with code, attach the Lakehouse if needed, and run it.");
}

main().catch((error) => {
  console.error(`End-to-end demo failed: ${error.message}`);
  process.exit(1);
});
