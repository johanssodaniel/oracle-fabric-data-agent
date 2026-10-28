import fs from "node:fs";
import path from "node:path";

const REQUIRED_SERVERS = ["fabric", "oracle"];

export function configPathFromEnv() {
  return process.env.MCP_BRIDGE_CONFIG || path.resolve(process.cwd(), "config/local-settings.json");
}

export function loadConfig(configPath = configPathFromEnv()) {
  loadDotEnv();

  const resolvedPath = path.resolve(configPath);
  let raw;

  try {
    raw = fs.readFileSync(resolvedPath, "utf8");
  } catch (error) {
    throw new Error(`Unable to read config file at ${resolvedPath}: ${error.message}`);
  }

  let config;
  try {
    config = JSON.parse(raw);
  } catch (error) {
    throw new Error(`Config file is not valid JSON: ${error.message}`);
  }

  validateConfig(config, resolvedPath);
  return { config, path: resolvedPath };
}

export function validateConfig(config, sourcePath = "config") {
  if (!config || typeof config !== "object") {
    throw new Error(`${sourcePath} must contain a JSON object.`);
  }

  if (!config.servers || typeof config.servers !== "object") {
    throw new Error(`${sourcePath} must define a "servers" object.`);
  }

  for (const serverName of REQUIRED_SERVERS) {
    const server = config.servers[serverName];
    if (!server || typeof server !== "object") {
      throw new Error(`${sourcePath} must define servers.${serverName}.`);
    }

    const type = server.type || "stdio";
    if (!["stdio", "streamable-http", "streamableHttp"].includes(type)) {
      throw new Error(`${sourcePath} must define servers.${serverName}.type as "stdio" or "streamable-http" when provided.`);
    }

    if (type === "stdio" && (!server.command || typeof server.command !== "string")) {
      throw new Error(`${sourcePath} must define servers.${serverName}.command as a string.`);
    }

    if ((type === "streamable-http" || type === "streamableHttp") && (!server.url || typeof server.url !== "string")) {
      throw new Error(`${sourcePath} must define servers.${serverName}.url as a string for streamable HTTP servers.`);
    }

    if (server.args !== undefined && !Array.isArray(server.args)) {
      throw new Error(`${sourcePath} must define servers.${serverName}.args as an array when provided.`);
    }

    if (server.env !== undefined && (!server.env || typeof server.env !== "object" || Array.isArray(server.env))) {
      throw new Error(`${sourcePath} must define servers.${serverName}.env as an object when provided.`);
    }
  }
}

export function resolveServerEnv(serverConfig) {
  const env = { ...process.env };

  for (const [key, value] of Object.entries(serverConfig.env || {})) {
    if (typeof value !== "string") {
      env[key] = String(value);
      continue;
    }

    env[key] = resolveEnvString(value);
  }

  return env;
}

export function resolveEnvString(value) {
  return value.replace(/\$\{([A-Z0-9_]+)\}/gi, (_match, variableName) => process.env[variableName] || "");
}

export function resolveHeaders(headers = {}) {
  return Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [key, typeof value === "string" ? resolveEnvString(value) : String(value)])
  );
}

export function findUnresolvedEnvRefs(serverConfig) {
  const missing = [];

  for (const [key, value] of Object.entries(serverConfig.env || {})) {
    if (typeof value !== "string") {
      continue;
    }

    for (const match of value.matchAll(/\$\{([A-Z0-9_]+)\}/gi)) {
      const variableName = match[1];
      if (!process.env[variableName]) {
        missing.push({ key, variableName });
      }
    }
  }

  for (const [key, value] of Object.entries(serverConfig.headers || {})) {
    if (typeof value !== "string") {
      continue;
    }

    for (const match of value.matchAll(/\$\{([A-Z0-9_]+)\}/gi)) {
      const variableName = match[1];
      if (!process.env[variableName]) {
        missing.push({ key, variableName });
      }
    }
  }

  return missing;
}

function loadDotEnv() {
  const envPath = path.resolve(process.env.MCP_BRIDGE_ENV_FILE || ".env");

  if (!fs.existsSync(envPath)) {
    return;
  }

  const lines = fs.readFileSync(envPath, "utf8").split(/\r?\n/);

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }

    const equalsIndex = trimmed.indexOf("=");
    if (equalsIndex === -1) {
      continue;
    }

    const key = trimmed.slice(0, equalsIndex).trim();
    const value = unquoteEnvValue(trimmed.slice(equalsIndex + 1).trim());

    if (key && process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

function unquoteEnvValue(value) {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }

  return value;
}
