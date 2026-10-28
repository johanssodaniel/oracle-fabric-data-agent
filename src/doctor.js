#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

import { loadConfig, resolveServerEnv } from "./config.js";

function exists(filePath) {
  return Boolean(filePath && fs.existsSync(filePath));
}

function commandExists(command) {
  if (!command || command.includes("<")) {
    return false;
  }

  if (command.includes("/") || command.includes("\\")) {
    return exists(command);
  }

  const result = spawnSync("sh", ["-c", `command -v ${shellQuote(command)}`], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"]
  });

  return result.status === 0 && Boolean(result.stdout.trim());
}

function shellQuote(value) {
  return `'${String(value).replace(/'/g, "'\\''")}'`;
}

function statusLine(ok, label, detail) {
  const prefix = ok ? "OK" : "MISSING";
  return detail ? `${prefix}  ${label}: ${detail}` : `${prefix}  ${label}`;
}

function fabricAuthState(env) {
  if (env.FABRIC_ACCESS_TOKEN || env.FABRIC_BEARER_TOKEN) {
    return "FABRIC_ACCESS_TOKEN/FABRIC_BEARER_TOKEN set";
  }

  if (env.FABRIC_TENANT_ID && env.FABRIC_CLIENT_ID && env.FABRIC_CLIENT_SECRET) {
    return "service principal settings set";
  }

  if (env.FABRIC_USE_AZURE_CLI_TOKEN !== "false" && commandExists("az")) {
    return "Azure CLI fallback available; run az login if needed";
  }

  return "";
}

function walletStatus(tnsAdmin) {
  if (!exists(tnsAdmin)) {
    return { ok: false, detail: tnsAdmin || "TNS_ADMIN not set" };
  }

  const requiredFiles = ["tnsnames.ora", "sqlnet.ora"];
  const missing = requiredFiles.filter((fileName) => !exists(path.join(tnsAdmin, fileName)));
  if (missing.length > 0) {
    return { ok: false, detail: `missing ${missing.join(", ")} in ${tnsAdmin}` };
  }

  return { ok: true, detail: tnsAdmin };
}

function main() {
  let config;
  let configPath;

  try {
    const loaded = loadConfig();
    config = loaded.config;
    configPath = loaded.path;
  } catch (error) {
    console.log(statusLine(false, "config", error.message));
    process.exitCode = 1;
    return;
  }

  const fabricEnv = resolveServerEnv(config.servers.fabric);
  const oracleEnv = resolveServerEnv(config.servers.oracle);
  const fabricAuth = fabricAuthState(fabricEnv);
  const oracleCommand = config.servers.oracle.command;
  const fabricCommand = config.servers.fabric.command;
  const wallet = walletStatus(oracleEnv.TNS_ADMIN);

  const checks = [
    [true, "config", configPath],
    [exists(".vscode/mcp.json"), "VS Code MCP config", ".vscode/mcp.json"],
    [exists(".github/agents/oracle-fabric-data-agent.agent.md"), "VS Code custom agent", ".github/agents/oracle-fabric-data-agent.agent.md"],
    [commandExists("node"), "Node.js", "node on PATH"],
    [commandExists("npm"), "npm", "npm on PATH"],
    [commandExists(fabricCommand), "Fabric MCP command", fabricCommand],
    [Boolean(fabricEnv.FABRIC_TENANT_ID), "Fabric tenant ID", mask(fabricEnv.FABRIC_TENANT_ID)],
    [Boolean(fabricEnv.FABRIC_WORKSPACE_ID), "Fabric workspace ID", mask(fabricEnv.FABRIC_WORKSPACE_ID)],
    [Boolean(fabricAuth), "Fabric REST auth", fabricAuth || "set FABRIC_ACCESS_TOKEN, service principal settings, or use az login"],
    [commandExists(oracleCommand), "Oracle SQLcl command", oracleCommand],
    [wallet.ok, "Oracle wallet", wallet.detail],
    [Boolean(oracleEnv.ORACLE_SQLCL_CONNECTION_NAME), "SQLcl saved connection name", oracleEnv.ORACLE_SQLCL_CONNECTION_NAME || "set ORACLE_SQLCL_CONNECTION_NAME"]
  ];

  let failed = false;
  console.log("Oracle Fabric Data Agent setup check");
  console.log();

  for (const [ok, label, detail] of checks) {
    console.log(statusLine(ok, label, detail));
    failed ||= !ok;
  }

  console.log();
  console.log("Required outside this package:");
  console.log("- Manager must be added to the Fabric workspace and the workspace must be on Fabric capacity.");
  console.log("- Manager must have Oracle wallet files and Oracle database credentials locally.");
  console.log("- Notebook upload needs Notebook.ReadWrite.All or Item.ReadWrite.All.");
  console.log("- Notebook run needs Notebook.Execute.All or Item.Execute.All.");

  if (failed) {
    process.exitCode = 1;
  }
}

function mask(value) {
  if (!value) {
    return "";
  }

  if (value.length <= 8) {
    return "set";
  }

  return `${value.slice(0, 4)}...${value.slice(-4)}`;
}

main();
