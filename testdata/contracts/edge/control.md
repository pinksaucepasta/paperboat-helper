# Edge Control Contract 1.0

The control plane is authoritative for environment, connector generation, route intent,
entitlement, and node assignment. The edge owns live connector observations, attachment,
forwarding, and byte counters. Messages are authenticated with the exact credential class
declared by their schema and use a unique operation ID.

Connector admission consumes a single-use `connector_admission` credential atomically with
recording `(environment_id, helper_id, connector_generation, edge_pool)`. The generation
must equal current desired state. A retry of the same operation and canonical request returns
the recorded decision. Reuse with different data, a stale generation, wrong node/pool, or a
revoked environment fails before a connector is accepted. Replacing a connector advances
generation, drains the old connection, detaches its routes, and rejects late traffic from it.

Admission responses include the assigned `edge_node_id`, an authenticated frps
`edge_endpoint`, and at least one operation-bound local route/proxy descriptor. The helper
configures only those descriptors and never derives endpoint or public-host values. After
login, readiness is reported only after every handed-off proxy is `running`. The edge owns
the frp `run_id`; same-run reconnects resume the generation only while its admission remains
unexpired and unrevoked. A new generation requires a fresh single-use admission.

Route attachment requires an admitted live connector with matching environment, generation,
edge node, route revision, and protocol. Preview host ownership is exclusive. Duplicate
attachment is idempotent; another environment receives `route_conflict` without disclosing
the owner. Stale detach cannot remove a newer route. Draining nodes accept no new assignment
and retain existing streams until their deadline before explicit termination.

Usage is reported per `(edge_node_id, counter_epoch, environment_id, route_id, direction)`
as an absolute monotonically increasing byte counter, never a delta. A process restart uses
a new random counter epoch and begins at zero. The server persists the greatest counter per
epoch and computes deltas transactionally; duplicate or lower observations add no usage.
Reassignment starts a new route ownership revision and may overlap old reports without
double-counting because route, node, and epoch identities remain distinct. Reports carry
observed interval bounds and an operation ID. An uncertain delivery is retried unchanged.

All decisions return a stable request ID. Logs and metrics may contain environment, node,
generation, route revision, result, and bounded byte counts, but never credentials, public
signed URLs, target content, headers, or provider-specific secrets.
