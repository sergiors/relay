# `@sergiors/relay-runtime`

## Installation

This is the official Node.js library for developing Relay applications and
using Relay's public handler interfaces. It is optional for Relay's execution;
add it as a normal application dependency when your application imports its
declarations.

The library supports Node.js 24 and newer.

```sh
pnpm add @sergiors/relay-runtime
```

The initial release exports the `HandlerContext` TypeScript declaration. Relay's
Node.js bootstrap constructs the actual context object for each invocation and
passes it to the handler; this package does not provide that implementation.
Future releases may add Relay application-development utilities.

TypeScript:

```ts
import type { HandlerContext } from "@sergiors/relay-runtime";

export async function handler(
  event: Record<string, unknown>,
  context: HandlerContext,
): Promise<void> {
  console.log(
    `request ${context.requestId} has ${context.getRemainingTimeMs()}ms left`,
  );
}
```

JavaScript with `checkJs`:

```js
/** @typedef {import("@sergiors/relay-runtime").HandlerContext} HandlerContext */

/**
 * @param {Record<string, unknown>} event
 * @param {HandlerContext} context
 */
export async function handler(event, context) {
  console.log(
    `request ${context.requestId} has ${context.getRemainingTimeMs()}ms left`,
  );
}
```

`requestId`, `attempt`, `maxAttempts`, `deadline`, `app`, and `handler` are
always present. `deadline` is an RFC 3339 timestamp string.
`messageId` and `traceId` are present only when available.
`getRemainingTimeMs()` returns the non-negative remaining time in milliseconds.

The JSDoc example in `examples/jsdoc-handler.mjs` can be checked after building
the declarations with `pnpm run check`. The corresponding TypeScript example is
in `examples/typescript-handler.ts`. Relay's bootstrap constructs each runtime
context; additional application-development utilities may be added in future
releases.
