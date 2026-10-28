#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

import { loadConfig, resolveServerEnv } from "./config.js";
import {
  buildOracleCopyBlueprint,
  buildPipelineContent,
  createOrUpdateDataPipelineDefinition,
  runDataPipeline,
  runtimeParameters
} from "./fabric-pipeline-definition.js";

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
  npm run pipeline:upload
  npm run pipeline:upload -- --pipeline-id <existing-pipeline-id>
  npm run pipeline:upload -- --run --pipeline-id <existing-pipeline-id>

Options:
  --display-name           Data Pipeline display name for create.
  --pipeline-id            Existing Data Pipeline ID to update.
  --connection-id          Fabric Oracle connection ID.
  --source-query           Oracle source query. Default: select * from ADMIN.FINANCIAL_CUSTOMERS.
  --destination-name       Lakehouse destination name. Default: financial_customers.
  --lakehouse-id           Lakehouse item ID. Defaults to FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID.
  --definition-out         Optional local JSON file path for the generated definition request.
  --run                    Run the pipeline after create/update. Requires an existing or returned pipeline ID.
  --owner-upn              Owner UPN for executionData.
  --owner-object-id        Owner object ID for executionData.
  --no-wait                Do not poll async create/update/run completion.

Auth:
  Set FABRIC_ACCESS_TOKEN, or set FABRIC_TENANT_ID, FABRIC_CLIENT_ID, and FABRIC_CLIENT_SECRET.
  Required create/update scope: DataPipeline.ReadWrite.All or Item.ReadWrite.All.
  Required run scope: DataPipeline.Execute.All or Item.Execute.All.
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
  const displayName = args["display-name"] || `oracle_to_onelake_pipeline_${runId()}`;
  const sourceQuery = args["source-query"] || "select * from ADMIN.FINANCIAL_CUSTOMERS";
  const destinationName = args["destination-name"] || "financial_customers";
  const oracleConnectionId = args["connection-id"] || process.env.FABRIC_ORACLE_CONNECTION_ID || "";
  const description = "Generated Oracle to OneLake Data Pipeline definition with runtime parameters.";

  if (!workspaceId) {
    throw new Error("Missing FABRIC_WORKSPACE_ID.");
  }

  if (!lakehouseId) {
    throw new Error("Missing Fabric Lakehouse ID. Provide --lakehouse-id or FABRIC_FINANCIAL_CUSTOMERS_LAKEHOUSE_ID.");
  }

  const pipelineContent = buildPipelineContent({
    description,
    oracleConnectionId,
    lakehouseId,
    sourceQuery,
    destinationName
  });
  const copyBlueprint = buildOracleCopyBlueprint({
    oracleConnectionId,
    lakehouseId,
    sourceQuery,
    destinationName
  });

  if (args["definition-out"]) {
    const outputPath = path.resolve(args["definition-out"]);
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });
    fs.writeFileSync(outputPath, JSON.stringify({ pipelineContent, copyBlueprint }, null, 2));
    console.log(`Wrote definition preview: ${outputPath}`);
  }

  const result = await createOrUpdateDataPipelineDefinition({
    workspaceId,
    pipelineId: args["pipeline-id"],
    displayName,
    description,
    pipelineContent,
    waitForCompletion: !args["no-wait"]
  });

  console.log(`Data Pipeline definition ${result.action}.`);
  console.log(`Pipeline: ${result.displayName}`);
  console.log(`Pipeline ID: ${result.id || args["pipeline-id"] || "not returned"}`);
  console.log(`HTTP status: ${result.response.status}`);
  if (result.operation?.status) {
    console.log(`Operation status: ${result.operation.status}`);
  } else if (result.response.headers.location) {
    console.log(`Operation location: ${result.response.headers.location}`);
  }

  if (args.run) {
    const pipelineId = result.id || args["pipeline-id"];
    if (!pipelineId) {
      throw new Error("Cannot run pipeline because no pipeline ID was returned. Re-run with --pipeline-id after provisioning completes.");
    }

    const runResult = await runDataPipeline({
      workspaceId,
      pipelineId,
      pipelineName: result.displayName || displayName,
      ownerUserPrincipalName: args["owner-upn"],
      ownerUserObjectId: args["owner-object-id"],
      parameters: runtimeParameters({
        sourceQuery,
        destinationName,
        lakehouseId,
        oracleConnectionId
      }),
      waitForCompletion: !args["no-wait"]
    });

    console.log("Data Pipeline run requested.");
    console.log(`Run HTTP status: ${runResult.response.status}`);
    if (runResult.job?.status) {
      console.log(`Pipeline job status: ${runResult.job.status}`);
    }
    if (runResult.job?.job?.body?.id) {
      console.log(`Pipeline job ID: ${runResult.job.job.body.id}`);
    }
    if (runResult.response.headers.location && !runResult.job) {
      console.log(`Run location: ${runResult.response.headers.location}`);
    }
  }
}

main().catch((error) => {
  console.error(`Data Pipeline definition upload failed: ${error.message}`);
  process.exit(1);
});
