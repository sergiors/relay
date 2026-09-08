// Package function is the pure decision layer for Relay functions.
//
// This package reads the /functions root and models what the runtime needs:
//   - Discovery: each direct subdirectory is a function with a template.yaml
//   - Validation: templates are parsed and validated, including per-rule
//     timeout resolution and handler form
//   - Matching: rules pair handlers with patterns evaluated against events
//   - Fingerprinting: a deterministic hash of a function's contents gates
//     reconciler rebuilds
//
// Key Features:
//   - A rule that omits a timeout resolves to DefaultTimeout; zero, negative,
//     or unparseable values are rejected
//
// The package has no side effects beyond reading the filesystem; building,
// execution, and Redis are owned elsewhere.
package function
