/** @typedef {import("@sergiors/relay-runtime").HandlerContext} HandlerContext */

/**
 * @param {Record<string, unknown>} event
 * @param {HandlerContext} context
 */
export async function handler(event, context) {
  void event;
  console.log(
    `request ${context.requestId} has ${context.getRemainingTimeMs()}ms left`,
  );
}
