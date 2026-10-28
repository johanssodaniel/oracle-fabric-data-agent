import { spawnSync } from "node:child_process";

const FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1";
const TERMINAL_OPERATION_STATUSES = new Set(["Succeeded", "Failed", "Cancelled"]);
const TERMINAL_JOB_STATUSES = new Set(["Completed", "Failed", "Cancelled", "Deduped"]);

export function hasNotebookDefinitionAuth(env = process.env) {
  return Boolean(
    env.FABRIC_ACCESS_TOKEN ||
    env.FABRIC_BEARER_TOKEN ||
    (env.FABRIC_TENANT_ID && env.FABRIC_CLIENT_ID && env.FABRIC_CLIENT_SECRET) ||
    env.FABRIC_USE_AZURE_CLI_TOKEN !== "false"
  );
}

export function notebookPythonCode({ lakehouseId, sourcePath }) {
  return `# Financial Customers Spark Delta + ML
# Created with Fabric notebook definition payload.

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.functions import vector_to_array

LAKEHOUSE_ID = "${lakehouseId}"
SOURCE_PATH = "${sourcePath}"

raw_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(SOURCE_PATH)
)

display(raw_df)

(
    raw_df.write
          .mode("overwrite")
          .format("delta")
          .saveAsTable("financial_customers")
)

customers = spark.sql("select * from financial_customers")
display(customers)

df = (
    customers
    .withColumn("annual_income_usd", F.col("annual_income_usd").cast("double"))
    .withColumn("assets_under_management_usd", F.col("assets_under_management_usd").cast("double"))
    .withColumn("credit_score", F.coalesce(F.col("credit_score").cast("double"), F.lit(0.0)))
    .withColumn("is_pep_flag", F.when(F.upper(F.col("is_pep")) == "Y", F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("aml_flag", F.when(F.upper(F.col("aml_watchlist_status")) != "CLEAR", F.lit(1.0)).otherwise(F.lit(0.0)))
    .withColumn("high_risk_label", F.when(F.upper(F.col("risk_rating")) == "HIGH", F.lit(1.0)).otherwise(F.lit(0.0)))
    .fillna({
        "annual_income_usd": 0.0,
        "assets_under_management_usd": 0.0,
        "credit_score": 0.0,
        "customer_type": "UNKNOWN",
        "kyc_status": "UNKNOWN",
        "preferred_channel": "UNKNOWN",
        "country_code": "UNKNOWN"
    })
)

categorical_cols = ["customer_type", "kyc_status", "preferred_channel", "country_code"]
numeric_cols = ["annual_income_usd", "assets_under_management_usd", "credit_score", "is_pep_flag", "aml_flag"]

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

risk_model = Pipeline(stages=indexers + [
    encoder,
    assembler,
    LogisticRegression(
        featuresCol="features",
        labelCol="high_risk_label",
        probabilityCol="high_risk_probability_vector",
        predictionCol="predicted_high_risk",
        maxIter=25
    )
]).fit(df)

risk_scored = (
    risk_model.transform(df)
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
segments = KMeans(k=3, seed=42, featuresCol="features", predictionCol="customer_segment").fit(featured).transform(featured)

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

print("Delta outputs created:")
print("- financial_customers")
print("- financial_customer_ml_scores")
print("- financial_customer_segments")
`;
}

export function buildNotebookIpynb({ lakehouseId, sourcePath, pythonCode, title = "Financial Customers Spark Delta + ML" }) {
  return {
    cells: [
      {
        cell_type: "markdown",
        metadata: {},
        source: [
          `# ${title}\\n`,
          "\\n",
          "Created by Oracle Fabric Data Agent with a Fabric REST notebook definition payload."
        ]
      },
      {
        cell_type: "code",
        execution_count: null,
        metadata: {},
        outputs: [],
        source: (pythonCode || notebookPythonCode({ lakehouseId, sourcePath })).split("\n").map((line) => `${line}\n`)
      }
    ],
    metadata: {
      kernelspec: {
        display_name: "Synapse PySpark",
        language: "python",
        name: "synapse_pyspark"
      },
      language_info: {
        name: "python"
      }
    },
    nbformat: 4,
    nbformat_minor: 5
  };
}

export function notebookDefinition({ lakehouseId, sourcePath, pythonCode, title }) {
  const notebook = buildNotebookIpynb({ lakehouseId, sourcePath, pythonCode, title });
  const payload = Buffer.from(JSON.stringify(notebook, null, 2), "utf8").toString("base64");

  return {
    format: "ipynb",
    parts: [
      {
        path: "notebook-content.ipynb",
        payload,
        payloadType: "InlineBase64"
      }
    ]
  };
}

export async function createOrUpdateNotebookDefinition({
  workspaceId,
  notebookId,
  displayName,
  lakehouseId,
  sourcePath,
  pythonCode,
  title,
  waitForCompletion = true,
  env = process.env
}) {
  const token = await getFabricToken(env);
  const definition = notebookDefinition({ lakehouseId, sourcePath, pythonCode, title });

  if (notebookId) {
    const result = await fabricApiRequest({
      token,
      method: "POST",
      path: `/workspaces/${workspaceId}/notebooks/${notebookId}/updateDefinition`,
      body: { definition }
    });
    const completed = waitForCompletion ? await pollFabricOperation({ token, response: result }) : null;

    return {
      action: "updated",
      id: notebookId,
      displayName,
      operation: completed,
      response: result
    };
  }

  const result = await fabricApiRequest({
    token,
    method: "POST",
    path: `/workspaces/${workspaceId}/notebooks`,
    body: {
      displayName,
      definition
    }
  });
  const completed = waitForCompletion ? await pollFabricOperation({ token, response: result }) : null;
  const createdItem = completed?.result?.body || result.body || {};

  return {
    action: "created",
    id: createdItem?.id,
    displayName: createdItem?.displayName || result.body?.displayName || displayName,
    operation: completed,
    response: result
  };
}

export async function runNotebookJob({
  workspaceId,
  notebookId,
  parameters = [],
  executionData = null,
  jobType = "RunNotebook",
  waitForCompletion = true,
  env = process.env
}) {
  const token = await getFabricToken(env);
  const body = {};

  if (executionData) {
    body.executionData = executionData;
  }

  if (parameters.length > 0) {
    body.parameters = parameters;
  }

  const requestBody = Object.keys(body).length > 0 ? body : undefined;
  const result = await fabricApiRequest({
    token,
    method: "POST",
    path: `/workspaces/${workspaceId}/items/${notebookId}/jobs/${jobType}/instances`,
    body: requestBody
  });
  const completed = waitForCompletion ? await pollFabricJob({ token, response: result }) : null;

  return {
    notebookId,
    jobInstanceId: jobInstanceIdFromLocation(result.headers.location),
    job: completed,
    response: result
  };
}

export async function getFabricToken(env) {
  const directToken = env.FABRIC_ACCESS_TOKEN || env.FABRIC_BEARER_TOKEN;
  if (directToken) {
    return directToken.replace(/^Bearer\s+/i, "");
  }

  const tenantId = env.FABRIC_TENANT_ID;
  const clientId = env.FABRIC_CLIENT_ID;
  const clientSecret = env.FABRIC_CLIENT_SECRET;

  if (!tenantId || !clientId || !clientSecret) {
    const azureCliToken = getAzureCliFabricToken(env);
    if (azureCliToken) {
      return azureCliToken;
    }

    throw new Error("Missing Fabric REST auth. Set FABRIC_ACCESS_TOKEN, set FABRIC_TENANT_ID/FABRIC_CLIENT_ID/FABRIC_CLIENT_SECRET, or run az login for Azure CLI token fallback.");
  }

  const body = new URLSearchParams({
    client_id: clientId,
    client_secret: clientSecret,
    grant_type: "client_credentials",
    scope: "https://api.fabric.microsoft.com/.default"
  });

  const response = await fetch(`https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded"
    },
    body
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`Fabric token request failed (${response.status}): ${payload.error_description || payload.error || response.statusText}`);
  }

  return payload.access_token;
}

function getAzureCliFabricToken(env) {
  if (env.FABRIC_USE_AZURE_CLI_TOKEN === "false") {
    return null;
  }

  const result = spawnSync("az", [
    "account",
    "get-access-token",
    "--resource",
    "https://api.fabric.microsoft.com",
    "--query",
    "accessToken",
    "-o",
    "tsv"
  ], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"]
  });

  if (result.status !== 0) {
    return null;
  }

  return result.stdout.trim() || null;
}

export async function fabricApiRequest({ token, method, path, body }) {
  const url = path.startsWith("https://") ? path : `${FABRIC_API_BASE}${path}`;
  const response = await fetch(url, {
    method,
    headers: {
      authorization: `Bearer ${token}`,
      "content-type": "application/json"
    },
    body: body ? JSON.stringify(body) : undefined
  });

  const text = await response.text();
  const parsed = text ? tryParseJson(text) : null;

  if (!response.ok) {
    throw new Error(`Fabric API request failed (${response.status}): ${parsed?.errorCode || parsed?.message || parsed?.error?.message || text || response.statusText}`);
  }

  return {
    status: response.status,
    headers: Object.fromEntries(response.headers.entries()),
    body: parsed
  };
}

export async function pollFabricOperation({
  token,
  response,
  maxWaitMs = 600000,
  defaultDelaySeconds = 5
}) {
  if (response.status !== 202) {
    return null;
  }

  const operationUrl = response.headers.location || operationUrlFromId(response.headers["x-ms-operation-id"]);
  if (!operationUrl) {
    return {
      status: "Accepted",
      result: null,
      message: "No operation URL returned."
    };
  }

  const startedAt = Date.now();
  let delaySeconds = Number(response.headers["retry-after"] || defaultDelaySeconds);
  let latest = null;

  while (Date.now() - startedAt < maxWaitMs) {
    await sleep(delaySeconds * 1000);

    latest = await fabricApiRequest({
      token,
      method: "GET",
      path: operationUrl
    });

    const status = latest.body?.status;
    if (TERMINAL_OPERATION_STATUSES.has(status)) {
      if (status !== "Succeeded") {
        throw new Error(`Fabric operation ${status}: ${JSON.stringify(latest.body?.error || latest.body?.failureReason || latest.body)}`);
      }

      return {
        status,
        operation: latest,
        result: await getFabricOperationResult(token, operationUrl)
      };
    }

    delaySeconds = Number(latest.headers["retry-after"] || defaultDelaySeconds);
  }

  throw new Error(`Timed out waiting for Fabric operation: ${operationUrl}`);
}

export async function pollFabricJob({
  token,
  response,
  maxWaitMs = 1800000,
  defaultDelaySeconds = 15
}) {
  if (response.status !== 202) {
    return null;
  }

  const jobUrl = response.headers.location;
  if (!jobUrl) {
    return {
      status: "Accepted",
      result: null,
      message: "No job URL returned."
    };
  }

  const startedAt = Date.now();
  let delaySeconds = Number(response.headers["retry-after"] || defaultDelaySeconds);
  let latest = null;

  while (Date.now() - startedAt < maxWaitMs) {
    await sleep(delaySeconds * 1000);

    latest = await fabricApiRequest({
      token,
      method: "GET",
      path: jobUrl
    });

    const status = latest.body?.status;
    if (TERMINAL_JOB_STATUSES.has(status)) {
      if (status === "Failed" || status === "Cancelled") {
        throw new Error(`Fabric notebook job ${status}: ${JSON.stringify(latest.body?.failureReason || latest.body)}`);
      }

      return {
        status,
        job: latest
      };
    }

    delaySeconds = Number(latest.headers["retry-after"] || defaultDelaySeconds);
  }

  throw new Error(`Timed out waiting for Fabric job: ${jobUrl}`);
}

async function getFabricOperationResult(token, operationUrl) {
  const resultUrl = operationUrl.endsWith("/result") ? operationUrl : `${operationUrl}/result`;

  try {
    return await fabricApiRequest({
      token,
      method: "GET",
      path: resultUrl
    });
  } catch (error) {
    return {
      status: "Unavailable",
      body: null,
      error: error.message
    };
  }
}

function operationUrlFromId(operationId) {
  if (!operationId) {
    return null;
  }

  return `${FABRIC_API_BASE}/operations/${operationId}`;
}

function jobInstanceIdFromLocation(location) {
  if (!location) {
    return null;
  }

  try {
    const pathname = new URL(location, FABRIC_API_BASE).pathname;
    return pathname.split("/").filter(Boolean).at(-1) || null;
  } catch {
    return null;
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function tryParseJson(value) {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}
