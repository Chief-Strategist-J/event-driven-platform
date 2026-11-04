package contracts

const (
	OffsetManagementOpenAPISpec = `
openapi: 3.1.0
info:
  title: Offset Management API
  version: 1.0.0
paths:
  /api/v1/offsets/commit:
    post:
      summary: Commit stored partition offset
      responses:
        '200':
          description: Offset committed successfully
  /api/v1/offsets/rewind:
    post:
      summary: Rewind partition offset for replay
      responses:
        '200':
          description: Offset rewound successfully
  /api/v1/offsets/fast-forward:
    post:
      summary: Fast forward partition offset past poison messages
      responses:
        '200':
          description: Offset fast forwarded successfully
  /api/v1/offsets/lag:
    get:
      summary: Evaluate partition lag severity
      responses:
        '200':
          description: Lag status retrieved
`
)

