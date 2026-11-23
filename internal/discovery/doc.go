// Package discovery exposes Relay's global service topology to an external
// routing provider over a small, read-only HTTP API.
//
// It is disabled unless DISCOVERY_ADDR is configured. When enabled, the worker
// serves GET /discovery/traefik with a Traefik HTTP-provider dynamic
// configuration derived from the PostgreSQL service registry (the same State
// handle the worker already owns). The endpoint has no authentication: bind it
// to a private address or restrict access at the network boundary. It exposes
// internal host/replica topology and never environment, secret, or container
// payload data.
//
// The discovery response is a materialized view of eligible endpoints. It never
// drives matching, builds, reconciliation, or execution; a database error is
// reported as 503 rather than as an empty configuration so a provider keeps its
// last successful routes. The provider (not Relay) owns entrypoints, TLS, and
// certificate resolvers.
package discovery
