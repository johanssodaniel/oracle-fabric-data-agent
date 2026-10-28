import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { ElicitRequestSchema } from "@modelcontextprotocol/sdk/types.js";

import { findUnresolvedEnvRefs, resolveEnvString, resolveHeaders, resolveServerEnv } from "./config.js";

export async function connectChildServer(name, serverConfig) {
  const missingEnv = findUnresolvedEnvRefs(serverConfig);
  if (missingEnv.length > 0) {
    const missing = missingEnv.map(({ variableName }) => variableName).join(", ");
    throw new Error(`${name} has missing environment variables: ${missing}`);
  }

  const transport = createTransport(serverConfig);

  const client = new Client(
    {
      name: `oracle-fabric-mcp-bridge-${name}`,
      version: "0.1.0"
    },
    {
      capabilities: {
        elicitation: serverConfig.autoApproveElicitation ? { form: {} } : undefined
      }
    }
  );

  if (serverConfig.autoApproveElicitation) {
    client.setRequestHandler(ElicitRequestSchema, async () => ({
      action: "accept",
      content: {}
    }));
  }

  await client.connect(transport);

  return {
    name,
    label: serverConfig.label || name,
    client,
    transport
  };
}

function createTransport(serverConfig) {
  const type = serverConfig.type || "stdio";

  if (type === "streamable-http" || type === "streamableHttp") {
    return new StreamableHTTPClientTransport(new URL(resolveEnvString(serverConfig.url)), {
      requestInit: {
        headers: resolveHeaders(serverConfig.headers)
      }
    });
  }

  return new StdioClientTransport({
    command: serverConfig.command,
    args: serverConfig.args || [],
    cwd: serverConfig.cwd,
    env: resolveServerEnv(serverConfig),
    stderr: serverConfig.stderr || "inherit"
  });
}

export async function listChildTools(child) {
  const result = await child.client.listTools();
  return result.tools || [];
}
