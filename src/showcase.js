#!/usr/bin/env node
import { connectChildServer } from "./child-client.js";
import { loadConfig, resolveServerEnv } from "./config.js";

const TABLE_NAME = "FINANCIAL_CUSTOMERS";
const LAKEHOUSE_NAME = "financial_customers";
const ONELAKE_PATH = "Files/financial_customers.csv";

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

function tableFromCsv(csv) {
  return csv
    .trim()
    .split(/\r?\n/)
    .filter(Boolean)
    .map((line) => line.split(",").map((cell) => cell.replace(/^"|"$/g, "")));
}

function printRows(rows) {
  for (const row of rows) {
    console.log(`  ${row.join("  ")}`);
  }
}

async function exportOracleCustomers(oracleConfig, connectionName) {
  const oracle = await connectChildServer("oracle", oracleConfig);

  try {
    await oracle.client.callTool({
      name: "connect",
      arguments: {
        connection_name: connectionName,
        model: "codex-showcase"
      }
    });

    const countResult = await oracle.client.callTool({
      name: "sql_run",
      arguments: {
        model: "codex-showcase",
        sql: `select count(*) as customer_count from ${TABLE_NAME}`
      }
    });

    const riskResult = await oracle.client.callTool({
      name: "sql_run",
      arguments: {
        model: "codex-showcase",
        sql: `select risk_rating,
                     count(*) as customer_count,
                     sum(assets_under_management_usd) as total_aum_usd
                from ${TABLE_NAME}
               group by risk_rating
               order by risk_rating`
      }
    });

    const exportResult = await oracle.client.callTool({
      name: "sql_run",
      arguments: {
        model: "codex-showcase",
        sql: `select customer_id,
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
      }
    });

    return {
      customerCountCsv: text(countResult),
      riskSummaryCsv: text(riskResult),
      exportCsv: text(exportResult).trimEnd() + "\n"
    };
  } finally {
    await oracle.client.close();
  }
}

async function uploadToFabric(fabricConfig, fabricEnv, csv) {
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

  const fabric = await connectChildServer("fabric", fabricConfig);

  try {
    const workspaces = parseJsonText(await fabric.client.callTool({
      name: "onelake_list_workspaces",
      arguments: {
        tenant,
        "auth-method": "Credential",
        format: "json"
      }
    }));

    const upload = parseJsonText(await fabric.client.callTool({
      name: "onelake_upload_file",
      arguments: {
        tenant,
        "auth-method": "Credential",
        "workspace-id": workspaceId,
        "item-id": lakehouseId,
        "file-path": ONELAKE_PATH,
        content: csv,
        overwrite: true,
        "content-type": "text/csv"
      }
    }));

    const files = parseJsonText(await fabric.client.callTool({
      name: "onelake_list_files",
      arguments: {
        tenant,
        "auth-method": "Credential",
        "workspace-id": workspaceId,
        "item-id": lakehouseId,
        path: "Files",
        recursive: true,
        format: "json"
      }
    }));

    return {
      workspace: workspaces?.results?.workspaces?.[0],
      upload,
      files
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

  if (!connectionName) {
    throw new Error("Missing ORACLE_SQLCL_CONNECTION_NAME.");
  }

  console.log("Financial Customers MCP Showcase");
  console.log("Oracle Autonomous Database -> Fabric OneLake");

  section("1. Source: Oracle MCP");
  console.log(`Connecting through SQLcl MCP saved connection: ${connectionName}`);
  const oracleData = await exportOracleCustomers(oracleConfig, connectionName);
  printRows(tableFromCsv(oracleData.customerCountCsv));

  section("2. Data Quality Snapshot");
  printRows(tableFromCsv(oracleData.riskSummaryCsv));

  section("3. Target: Fabric MCP + OneLake");
  console.log(`Offline upload to Fabric Lakehouse '${LAKEHOUSE_NAME}' at ${ONELAKE_PATH}`);
  const fabricResult = await uploadToFabric(fabricConfig, fabricEnv, oracleData.exportCsv);
  const uploadResults = fabricResult.upload?.results;
  console.log(`  Workspace: ${fabricResult.workspace?.displayName || fabricEnv.FABRIC_WORKSPACE_ID}`);
  console.log(`  Lakehouse ID: ${fabricEnv.FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID}`);
  console.log(`  Upload status: ${fabricResult.upload?.status} ${fabricResult.upload?.message}`);
  console.log(`  Bytes uploaded: ${uploadResults?.contentLength ?? Buffer.byteLength(oracleData.exportCsv, "utf8")}`);

  section("4. Verification");
  const uploadedFile = fabricResult.files?.results?.items?.find((item) => item.name === "financial_customers.csv");
  if (!uploadedFile) {
    throw new Error("Upload verification failed: financial_customers.csv was not listed in OneLake.");
  }
  console.log(`  Found OneLake file: ${uploadedFile.path}`);
  console.log(`  File size: ${uploadedFile.size} bytes`);
  console.log(`  Last modified: ${uploadedFile.lastModified}`);

  section("Demo Result");
  console.log("Oracle MCP queried live customer data, Fabric MCP performed an offline upload to OneLake, and Fabric MCP verified the file.");
}

main().catch((error) => {
  console.error(`\nShowcase failed: ${error.message}`);
  process.exit(1);
});
