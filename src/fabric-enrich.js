#!/usr/bin/env node
import { connectChildServer } from "./child-client.js";
import { loadConfig, resolveServerEnv } from "./config.js";

function runId() {
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

function extractItem(result) {
  const parsed = parseJsonText(result);
  if (typeof parsed !== "object" || parsed === null) {
    return { raw: parsed };
  }

  return parsed.results?.item || parsed.results || parsed.item || parsed;
}

function notebookBootstrap(run, lakehouseId) {
  return `# Financial Customers Notebook Bootstrap
# Run: ${run}
#
# This file is uploaded to OneLake as a companion asset for the Fabric Notebook
# created by the MCP demo. Paste or adapt the code into the generated notebook
# if your Fabric environment does not automatically populate notebook content.

lakehouse_id = "${lakehouseId}"
csv_path = "Files/financial_customers.csv"

df = spark.read.option("header", "true").csv(f"abfss://{lakehouseId}@onelake.dfs.fabric.microsoft.com/{csv_path}")
display(df)

summary = (
    df.groupBy("RISK_RATING")
      .count()
      .orderBy("RISK_RATING")
)
display(summary)
`;
}

function reportSpec(run) {
  return JSON.stringify({
    name: "Financial Customers Report",
    runId: run,
    source: {
      lakehouseFile: "Files/financial_customers.csv",
      transferFolderPattern: "Files/transfers/financial_customers/<run-id>/"
    },
    suggestedPages: [
      {
        title: "Risk Overview",
        visuals: [
          "Customer count by risk rating",
          "Assets under management by risk rating",
          "KYC status by customer type"
        ]
      },
      {
        title: "High Risk Review",
        visuals: [
          "High-risk and PEP flagged customer table",
          "AML watchlist status summary"
        ]
      }
    ],
    measures: [
      "Customer Count = COUNTROWS(FinancialCustomers)",
      "Total AUM = SUM(FinancialCustomers[ASSETS_UNDER_MANAGEMENT_USD])",
      "High Risk Customers = CALCULATE([Customer Count], FinancialCustomers[RISK_RATING] = \"HIGH\")"
    ],
    note: "The current local Fabric MCP server can create Fabric items and upload OneLake assets. Fully authored Power BI report definitions require the Fabric/Power BI report definition workflow for the target tenant."
  }, null, 2);
}

async function createItem(fabric, commonArgs, displayName, itemType, description) {
  try {
    const result = await fabric.client.callTool({
      name: "core_create-item",
      arguments: {
        ...commonArgs,
        "display-name": displayName,
        "item-type": itemType,
        description
      }
    });

    return {
      ok: true,
      displayName,
      itemType,
      item: extractItem(result)
    };
  } catch (error) {
    return {
      ok: false,
      displayName,
      itemType,
      error: error.message
    };
  }
}

async function main() {
  const { config } = loadConfig();
  const fabricConfig = config.servers.fabric;
  const fabricEnv = resolveServerEnv(fabricConfig);
  const tenant = fabricEnv.FABRIC_TENANT_ID || fabricEnv.FABRIC_TENANT_DOMAIN;
  const workspaceId = fabricEnv.FABRIC_WORKSPACE_ID;
  const lakehouseId = fabricEnv.FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID;
  const run = runId();

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
    console.log("Fabric enrichment");
    console.log(`Run ID: ${run}`);
    console.log(`Workspace ID: ${workspaceId}`);
    console.log(`Lakehouse ID: ${lakehouseId}`);

    const notebook = await createItem(
      fabric,
      commonArgs,
      `financial_customers_notebook_${run}`,
      "Notebook",
      "Notebook shell for analyzing Oracle financial customers data in Fabric."
    );

    const pipeline = await createItem(
      fabric,
      commonArgs,
      `oracle_to_fabric_pipeline_${run}`,
      "DataPipeline",
      "Pipeline shell documenting the Oracle to Fabric transfer flow."
    );

    const report = await createItem(
      fabric,
      commonArgs,
      `financial_customers_report_${run}`,
      "Report",
      "Report shell for financial customers risk analysis."
    );

    const basePath = `Files/showcase-assets/${run}`;
    const assets = [
      {
        path: `${basePath}/notebook_bootstrap.py`,
        content: notebookBootstrap(run, lakehouseId),
        contentType: "text/x-python"
      },
      {
        path: `${basePath}/report_spec.json`,
        content: reportSpec(run),
        contentType: "application/json"
      },
      {
        path: `${basePath}/fabric_items.json`,
        content: JSON.stringify({ run, notebook, pipeline, report }, null, 2),
        contentType: "application/json"
      }
    ];

    for (const asset of assets) {
      const upload = await fabric.client.callTool({
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
      const uploadResult = parseJsonText(upload);
      console.log(`Uploaded asset: ${uploadResult?.results?.blobPath || asset.path}`);
    }

    console.log("\nCreated Fabric artifacts:");
    for (const item of [notebook, pipeline, report]) {
      if (item.ok) {
        const id = item.item?.id || item.item?.itemId;
        console.log(`  ${item.itemType}: ${item.displayName} (${id || "created, id not returned"})`);
      } else {
        console.log(`  ${item.itemType}: skipped or failed (${item.error})`);
      }
    }

    console.log(`\nOneLake companion assets: ${basePath}`);
    console.log("Fabric enrichment complete.");
  } finally {
    await fabric.client.close();
  }
}

main().catch((error) => {
  console.error(`Fabric enrichment failed: ${error.message}`);
  process.exit(1);
});
