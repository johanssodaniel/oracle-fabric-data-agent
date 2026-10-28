#!/usr/bin/env node
import { loadConfig, resolveServerEnv } from "./config.js";
import { runNotebookJob } from "./fabric-notebook-definition.js";

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
  npm run notebook:run -- --notebook-id <notebook-id>
  npm run notebook:run -- --notebook-id <notebook-id> --no-wait

Options:
  --notebook-id    Fabric Notebook item ID to run.
  --no-wait        Request the run but do not poll until completion.

Auth:
  Set FABRIC_ACCESS_TOKEN, set FABRIC_TENANT_ID/FABRIC_CLIENT_ID/FABRIC_CLIENT_SECRET, or run az login.
  Required scope/app permission: Notebook.Execute.All or Item.Execute.All.
`;
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
  const notebookId = args["notebook-id"];

  if (!workspaceId) {
    throw new Error("Missing FABRIC_WORKSPACE_ID.");
  }

  if (!notebookId) {
    throw new Error("Missing --notebook-id.");
  }

  const result = await runNotebookJob({
    workspaceId,
    notebookId,
    waitForCompletion: !args["no-wait"]
  });

  console.log("Notebook run requested.");
  console.log(`Notebook ID: ${notebookId}`);
  console.log(`HTTP status: ${result.response.status}`);
  if (result.jobInstanceId) {
    console.log(`Notebook job ID: ${result.jobInstanceId}`);
  }
  if (result.job?.status) {
    console.log(`Notebook job status: ${result.job.status}`);
  }
  if (!result.jobInstanceId && result.job?.job?.body?.id) {
    console.log(`Notebook job ID: ${result.job.job.body.id}`);
  }
  if (result.response.headers.location && !result.job) {
    console.log(`Notebook job location: ${result.response.headers.location}`);
  }
}

main().catch((error) => {
  console.error(`Notebook run failed: ${error.message}`);
  process.exit(1);
});
