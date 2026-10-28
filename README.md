# Oracle Fabric Data Agent

A small VS Code agent and command-line toolkit for exploring Oracle through SQLcl MCP and working with Microsoft Fabric.

## Quick Start

```bash
npm install
cp .env.example .env
cp config/minimal-settings.example.json config/local-settings.json
cp .vscode/mcp.example.json .vscode/mcp.json
```

Configure the copied files with your own Fabric workspace, Oracle wallet, SQLcl installation, and saved SQLcl connection. Local configuration, credentials, wallets, downloads, and generated output are ignored by Git.

For VS Code, reload the window after creating `.vscode/mcp.json`. The Fabric Core server opens a Microsoft sign-in flow; Oracle uses your local SQLcl connection store.

## Verify

```bash
MCP_BRIDGE_CONFIG=config/local-settings.json npm run doctor
```

## Useful Commands

```bash
# Inspect Oracle and create a Fabric transfer plan
MCP_BRIDGE_CONFIG=config/local-settings.json npm run online:plan

# Upload an Oracle query or table to OneLake
MCP_BRIDGE_CONFIG=config/local-settings.json npm run transfer -- --table YOUR_TABLE --name your_table

# Upload a Fabric notebook definition
MCP_BRIDGE_CONFIG=config/local-settings.json npm run notebook:upload
```

## Security

Never commit `.env`, wallet files, local MCP configuration, access tokens, service-principal credentials, SQLcl downloads, or generated output. Use the supplied examples as templates.

## References

- [Oracle SQLcl MCP](https://docs.oracle.com/en/database/oracle/sql-developer-command-line/26.1/index.html)
- [Microsoft Fabric MCP](https://learn.microsoft.com/en-us/rest/api/fabric/articles/mcp-servers/what-is-fabric-mcp-server)
