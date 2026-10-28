# Oracle Fabric Data Agent

A VS Code agent and command-line toolkit for exploring Oracle through SQLcl MCP and working with Microsoft Fabric.

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

## References

- [Oracle SQLcl MCP](https://docs.oracle.com/en/database/oracle/sql-developer-command-line/26.1/index.html)
- [Microsoft Fabric MCP](https://learn.microsoft.com/en-us/rest/api/fabric/articles/mcp-servers/what-is-fabric-mcp-server)
