import type { HandlerContext } from "@sergiors/relay-runtime";

export async function handler(
  event: Record<string, unknown>,
  context: HandlerContext,
): Promise<void> {
  void event;
  console.log(
    `request ${context.requestId} has ${context.getRemainingTimeMs()}ms left`,
  );
}
