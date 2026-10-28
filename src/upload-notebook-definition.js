#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

import { loadConfig, resolveServerEnv } from "./config.js";
import { createOrUpdateNotebookDefinition, runNotebookJob } from "./fabric-notebook-definition.js";

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
  npm run notebook:upload
  npm run notebook:upload -- --display-name financial_customers_ml
  npm run notebook:upload -- --python-file ./notebooks/demo.py --display-name python_demo_notebook
  npm run notebook:upload -- --notebook-id <existing-notebook-id>
  npm run notebook:upload -- --display-name financial_customers_ml --run

Options:
  --display-name   Notebook display name for create. Default: financial_customers_spark_ml_<timestamp>.
  --notebook-id    Existing notebook ID to update with updateDefinition.
  --python-file    Python file to wrap as notebook-content.ipynb.
  --title          Notebook markdown title. Defaults to display name.
  --source-path    OneLake source CSV path. Default: Files/financial_customers.csv.
  --lakehouse-id   Lakehouse item ID. Defaults to FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID.
  --run            Run the notebook after create/update and poll until completion.
  --no-wait        Do not poll async create/update operation completion.

Auth:
  Set FABRIC_ACCESS_TOKEN, set FABRIC_TENANT_ID/FABRIC_CLIENT_ID/FABRIC_CLIENT_SECRET, or run az login.
  Required create/update scope: Notebook.ReadWrite.All or Item.ReadWrite.All.
  Required run scope: Notebook.Execute.All or Item.Execute.All.
`;
}

function runId() {
  return new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log(usage());
    return;
  }

  const { config } = loadConfig();
  const fabricEnv = resolveServerEnv(config.servers.fabric);
  const workspaceId = fabricEnv.FABRIC_WORKSPACE_ID;
  const lakehouseId = args["lakehouse-id"] || fabricEnv.FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID;
  const sourcePath = args["source-path"] || "Files/financial_customers.csv";
  const displayName = args["display-name"] || `financial_customers_spark_ml_${runId()}`;
  const pythonCode = args["python-file"] ? fs.readFileSync(path.resolve(args["python-file"]), "utf8") : undefined;
  const title = args.title || displayName;

  if (!workspaceId) {
    throw new Error("Missing FABRIC_WORKSPACE_ID.");
  }

  if (!lakehouseId) {
    throw new Error("Missing Fabric Lakehouse ID. Provide --lakehouse-id or FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID.");
  }

  const result = await createOrUpdateNotebookDefinition({
    workspaceId,
    notebookId: args["notebook-id"],
    displayName,
    lakehouseId,
    sourcePath,
    pythonCode,
    title,
    waitForCompletion: !args["no-wait"]
  });

  console.log(`Notebook definition ${result.action}.`);
  console.log(`Notebook: ${result.displayName}`);
  console.log(`Notebook ID: ${result.id || args["notebook-id"] || "not returned"}`);
  console.log(`Source path: ${sourcePath}`);
  if (args["python-file"]) {
    console.log(`Python file: ${path.resolve(args["python-file"])}`);
  }
  console.log(`HTTP status: ${result.response.status}`);
  if (result.operation?.status) {
    console.log(`Operation status: ${result.operation.status}`);
  } else if (result.response.headers.location) {
    console.log(`Operation location: ${result.response.headers.location}`);
  }

  if (args.run) {
    const notebookId = result.id || args["notebook-id"];
    if (!notebookId) {
      throw new Error("Cannot run notebook because no notebook ID was returned. Re-run with --notebook-id after provisioning completes.");
    }

    const run = await runNotebookJob({
      workspaceId,
      notebookId
    });

    console.log("Notebook run requested.");
    if (run.jobInstanceId) {
      console.log(`Notebook job ID: ${run.jobInstanceId}`);
    }
    console.log(`Notebook job status: ${run.job?.status || "Accepted"}`);
    if (!run.jobInstanceId && run.job?.job?.body?.id) {
      console.log(`Notebook job ID: ${run.job.job.body.id}`);
    }
    if (run.response.headers.location && !run.job) {
      console.log(`Notebook job location: ${run.response.headers.location}`);
    }
  }
}

main().catch((error) => {
  console.error(`Notebook definition upload failed: ${error.message}`);
  process.exit(1);
});
