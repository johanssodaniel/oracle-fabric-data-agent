import { fabricApiRequest, getFabricToken, pollFabricJob, pollFabricOperation } from "./fabric-notebook-definition.js";

export function hasPipelineDefinitionAuth(env = process.env) {
  return Boolean(
    env.FABRIC_ACCESS_TOKEN ||
    env.FABRIC_BEARER_TOKEN ||
    (env.FABRIC_TENANT_ID && env.FABRIC_CLIENT_ID && env.FABRIC_CLIENT_SECRET)
  );
}

export function buildPipelineContent({
  description,
  oracleConnectionId,
  lakehouseId,
  sourceQuery,
  destinationName
}) {
  return {
    properties: {
      description,
      parameters: {
        sourceQuery: {
          type: "String",
          defaultValue: sourceQuery
        },
        destinationName: {
          type: "String",
          defaultValue: destinationName
        },
        lakehouseId: {
          type: "String",
          defaultValue: lakehouseId
        },
        oracleConnectionId: {
          type: "String",
          defaultValue: oracleConnectionId || "<fabric-oracle-connection-id>"
        }
      },
      activities: [
        {
          name: "ValidateRuntimeParameters",
          type: "Wait",
          state: "Active",
          typeProperties: {
            waitTimeInSeconds: 1
          }
        }
      ],
      annotations: [
        "oracle-to-onelake",
        "generated-by-oracle-fabric-data-agent"
      ]
    }
  };
}

export function buildOracleCopyBlueprint({
  oracleConnectionId,
  lakehouseId,
  sourceQuery,
  destinationName
}) {
  return {
    note: "Blueprint for replacing the validation activity with a Fabric Copy activity once tenant-specific connection bindings are known.",
    source: {
      connector: "Oracle database",
      connectionId: oracleConnectionId || "<fabric-oracle-connection-id>",
      query: sourceQuery
    },
    sink: {
      connector: "Lakehouse",
      lakehouseId,
      destination: destinationName,
      format: "Delta table or Lakehouse file, depending on selected Copy activity sink settings."
    },
    runtimeParameters: {
      sourceQuery,
      destinationName,
      lakehouseId,
      oracleConnectionId: oracleConnectionId || "<fabric-oracle-connection-id>"
    }
  };
}

export function dataPipelineDefinition(pipelineContent) {
  const payload = Buffer.from(JSON.stringify(pipelineContent, null, 2), "utf8").toString("base64");

  return {
    parts: [
      {
        path: "pipeline-content.json",
        payload,
        payloadType: "InlineBase64"
      }
    ]
  };
}

export async function createOrUpdateDataPipelineDefinition({
  workspaceId,
  pipelineId,
  displayName,
  description,
  pipelineContent,
  waitForCompletion = true,
  env = process.env
}) {
  const token = await getFabricToken(env);
  const definition = dataPipelineDefinition(pipelineContent);

  if (pipelineId) {
    const result = await fabricApiRequest({
      token,
      method: "POST",
      path: `/workspaces/${workspaceId}/dataPipelines/${pipelineId}/updateDefinition`,
      body: { definition }
    });
    const completed = waitForCompletion ? await pollFabricOperation({ token, response: result }) : null;

    return {
      action: "updated",
      id: pipelineId,
      displayName,
      operation: completed,
      response: result
    };
  }

  const result = await fabricApiRequest({
    token,
    method: "POST",
    path: `/workspaces/${workspaceId}/dataPipelines`,
    body: {
      displayName,
      description,
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

export async function runDataPipeline({
  workspaceId,
  pipelineId,
  pipelineName,
  ownerUserPrincipalName,
  ownerUserObjectId,
  parameters = [],
  waitForCompletion = true,
  env = process.env
}) {
  const token = await getFabricToken(env);
  const body = {
    executionData: {
      pipelineName,
      OwnerUserPrincipalName: ownerUserPrincipalName || env.FABRIC_OWNER_USER_PRINCIPAL_NAME || "",
      OwnerUserObjectId: ownerUserObjectId || env.FABRIC_OWNER_USER_OBJECT_ID || ""
    }
  };

  if (parameters.length > 0) {
    body.parameters = parameters;
  }

  const result = await fabricApiRequest({
    token,
    method: "POST",
    path: `/workspaces/${workspaceId}/items/${pipelineId}/jobs/Pipeline/instances`,
    body
  });
  const completed = waitForCompletion ? await pollFabricJob({ token, response: result }) : null;

  return {
    pipelineId,
    job: completed,
    response: result
  };
}

export function runtimeParameters({
  sourceQuery,
  destinationName,
  lakehouseId,
  oracleConnectionId
}) {
  return [
    {
      name: "sourceQuery",
      type: "Text",
      value: sourceQuery
    },
    {
      name: "destinationName",
      type: "Text",
      value: destinationName
    },
    {
      name: "lakehouseId",
      type: "Text",
      value: lakehouseId
    },
    {
      name: "oracleConnectionId",
      type: "Text",
      value: oracleConnectionId || "<fabric-oracle-connection-id>"
    }
  ];
}
