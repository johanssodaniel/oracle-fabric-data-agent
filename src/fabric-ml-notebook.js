#!/usr/bin/env node
import { connectChildServer } from "./child-client.js";
import { loadConfig, resolveServerEnv } from "./config.js";

const MCP_TIMEOUT_MS = 180000;

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

function callTool(client, params) {
  return client.callTool(params, undefined, {
    timeout: MCP_TIMEOUT_MS,
    resetTimeoutOnProgress: true
  });
}

function extractItem(result) {
  const parsed = parseJsonText(result);
  if (typeof parsed !== "object" || parsed === null) {
    return { raw: parsed };
  }

  return parsed.results?.item || parsed.results || parsed.item || parsed;
}

async function createNotebook(fabric, commonArgs, displayName) {
  const result = await callTool(fabric.client, {
    name: "core_create-item",
    arguments: {
      ...commonArgs,
      "display-name": displayName,
      "item-type": "Notebook",
      description: "ML notebook shell for financial customer risk scoring and segmentation."
    }
  });

  return extractItem(result);
}

function mlNotebookCode({ workspaceId, lakehouseId }) {
  return `# Financial Customers ML Notebook
# Generated for the Oracle Fabric Data Agent demo.
#
# Goal:
# - Load the financial customer data that was imported into the Lakehouse.
# - Save the imported data to a Delta table with Apache Spark.
# - Train a small high-risk customer scoring model.
# - Produce customer segmentation output for demo discussion.
#
# Expected data:
# - Preferred: Lakehouse table named financial_customers.
# - Fallback: Files/financial_customers.csv from the offline upload to Fabric demo.

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.functions import vector_to_array

WORKSPACE_ID = "${workspaceId}"
LAKEHOUSE_ID = "${lakehouseId}"
TABLE_NAME = "financial_customers"
FALLBACK_FILE = "Files/financial_customers.csv"


def load_imported_financial_customers():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(FALLBACK_FILE)
    )


def load_financial_customers():
    try:
        return spark.sql(f"select * from {TABLE_NAME}")
    except Exception as exc:
        print(f"Delta table read failed, loading imported file instead: {exc}")
        imported_df = load_imported_financial_customers()
        (
            imported_df.write
                .mode("overwrite")
                .format("delta")
                .saveAsTable(TABLE_NAME)
        )
        return spark.sql(f"select * from {TABLE_NAME}")


raw_df = load_financial_customers()
display(raw_df)

df = (
    raw_df
    .withColumn("annual_income_usd", F.col("annual_income_usd").cast("double"))
    .withColumn("assets_under_management_usd", F.col("assets_under_management_usd").cast("double"))
    .withColumn("credit_score", F.coalesce(F.col("credit_score").cast("double"), F.lit(0.0)))
    .withColumn("is_pep_flag", F.when(F.upper(F.col("is_pep")) == "Y", F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("aml_flag", F.when(F.upper(F.col("aml_watchlist_status")) != "CLEAR", F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("high_risk_label", F.when(F.upper(F.col("risk_rating")) == "HIGH", F.lit(1.0)).otherwise(F.lit(0.0)))
)

df = df.fillna({
    "annual_income_usd": 0.0,
    "assets_under_management_usd": 0.0,
    "credit_score": 0.0,
    "customer_type": "UNKNOWN",
    "kyc_status": "UNKNOWN",
    "preferred_channel": "UNKNOWN",
    "country_code": "UNKNOWN"
})

categorical_cols = ["customer_type", "kyc_status", "preferred_channel", "country_code"]
numeric_cols = [
    "annual_income_usd",
    "assets_under_management_usd",
    "credit_score",
    "is_pep_flag",
    "aml_flag"
]

indexers = [
    StringIndexer(inputCol=column, outputCol=f"{column}_idx", handleInvalid="keep")
    for column in categorical_cols
]

encoder = OneHotEncoder(
    inputCols=[f"{column}_idx" for column in categorical_cols],
    outputCols=[f"{column}_vec" for column in categorical_cols],
    handleInvalid="keep"
)

assembler = VectorAssembler(
    inputCols=numeric_cols + [f"{column}_vec" for column in categorical_cols],
    outputCol="features",
    handleInvalid="keep"
)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="high_risk_label",
    probabilityCol="high_risk_probability_vector",
    predictionCol="predicted_high_risk",
    maxIter=25
)

risk_pipeline = Pipeline(stages=indexers + [encoder, assembler, lr])
risk_model = risk_pipeline.fit(df)

risk_scored = (
    risk_model
    .transform(df)
    .withColumn("high_risk_probability", vector_to_array("high_risk_probability_vector")[1])
    .select(
        "customer_number",
        "legal_name",
        "customer_type",
        "country_code",
        "kyc_status",
        "risk_rating",
        "assets_under_management_usd",
        "is_pep",
        "aml_watchlist_status",
        "high_risk_probability",
        "predicted_high_risk"
    )
    .orderBy(F.desc("high_risk_probability"))
)

display(risk_scored)

feature_model = Pipeline(stages=indexers + [encoder, assembler]).fit(df)
featured = feature_model.transform(df)

kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="customer_segment")
segment_model = kmeans.fit(featured)
segments = segment_model.transform(featured)

segment_summary = (
    segments
    .groupBy("customer_segment")
    .agg(
        F.count("*").alias("customers"),
        F.avg("assets_under_management_usd").alias("avg_aum"),
        F.avg("annual_income_usd").alias("avg_income"),
        F.avg("high_risk_label").alias("high_risk_rate")
    )
    .orderBy("customer_segment")
)

display(segment_summary)

risk_scored.write.mode("overwrite").format("delta").saveAsTable("financial_customer_ml_scores")
segment_summary.write.mode("overwrite").format("delta").saveAsTable("financial_customer_segments")

print("Created Delta tables:")
print("- financial_customers")
print("- financial_customer_ml_scores")
print("- financial_customer_segments")
`;
}

function runbook({ run, notebookName, notebookId, assetPath }) {
  return `# Financial Customers ML Notebook Runbook

Notebook shell:

\`\`\`text
${notebookName}
\`\`\`

Notebook item ID:

\`\`\`text
${notebookId || "created, id not returned by MCP"}
\`\`\`

Notebook code asset:

\`\`\`text
${assetPath}/financial_customers_ml_notebook.py
\`\`\`

## What It Does

- Reads the imported financial customer data from the Lakehouse.
- Prefers a Lakehouse table named \`financial_customers\`.
- Falls back to \`Files/financial_customers.csv\` from the offline upload to Fabric demo.
- Saves the imported data as the \`financial_customers\` Delta table with Apache Spark.
- Trains a logistic regression high-risk scoring model.
- Runs KMeans customer segmentation.
- Writes these Delta tables:

\`\`\`text
financial_customers
financial_customer_ml_scores
financial_customer_segments
\`\`\`

## Current MCP Boundary

The local Fabric MCP server can create the Notebook item shell and upload this notebook code asset to OneLake. It does not currently expose notebook definition update or notebook job execution tools.

To run it now:

1. Open the generated Notebook in Fabric.
2. Attach the \`financial_customers\` Lakehouse.
3. Paste or import \`financial_customers_ml_notebook.py\`.
4. Run the notebook.
5. Ask the VS Code agent to verify the generated Lakehouse tables through Fabric MCP.

Fabric notebook public APIs support notebook CRUD and on-demand notebook execution, so this can be automated when the agent has a Fabric API auth path for notebook definition update and job execution.

Run ID: ${run}
`;
}

function modelCard({ run, notebookName }) {
  return JSON.stringify({
    runId: run,
    notebook: notebookName,
    modelPurpose: "Demo high-risk customer scoring and customer segmentation.",
    inputData: [
      "Lakehouse table financial_customers",
      "Fallback file Files/financial_customers.csv"
    ],
    outputs: [
      "financial_customers",
      "financial_customer_ml_scores",
      "financial_customer_segments"
    ],
    algorithms: [
      "LogisticRegression for high-risk scoring",
      "KMeans for customer segmentation"
    ],
    limitations: [
      "Demo dataset is intentionally small.",
      "Scores are for showcasing Fabric ML workflow only, not production risk decisions.",
      "Production use needs more training data, validation, model governance, and bias review."
    ]
  }, null, 2);
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

  const notebookName = `financial_customers_ml_${run}`;
  const assetPath = `Files/ml-assets/${run}`;
  const fabric = await connectChildServer("fabric", fabricConfig);

  try {
    console.log("Fabric ML notebook setup");
    console.log(`Run ID: ${run}`);
    console.log(`Workspace ID: ${workspaceId}`);
    console.log(`Lakehouse ID: ${lakehouseId}`);

    const notebook = await createNotebook(fabric, commonArgs, notebookName);
    const notebookId = notebook?.id || notebook?.itemId;

    const assets = [
      {
        path: `${assetPath}/financial_customers_ml_notebook.py`,
        content: mlNotebookCode({ workspaceId, lakehouseId }),
        contentType: "text/x-python"
      },
      {
        path: `${assetPath}/financial_customers_ml_runbook.md`,
        content: runbook({ run, notebookName, notebookId, assetPath }),
        contentType: "text/markdown"
      },
      {
        path: `${assetPath}/financial_customers_model_card.json`,
        content: modelCard({ run, notebookName }),
        contentType: "application/json"
      }
    ];

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

      const uploadResult = parseJsonText(upload);
      console.log(`Uploaded ML asset: ${uploadResult?.results?.blobPath || asset.path}`);
    }

    console.log(`Notebook shell: ${notebookName} (${notebookId || "created, id not returned"})`);
    console.log(`OneLake ML assets: ${assetPath}`);
    console.log("Fabric ML notebook setup complete.");
  } finally {
    await fabric.client.close();
  }
}

main().catch((error) => {
  console.error(`Fabric ML notebook setup failed: ${error.message}`);
  process.exit(1);
});
