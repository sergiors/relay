// Handler for the resource-limits-node example. The per-container memory/CPU/PID
// limits from template.yaml `resources:` bound this process; the handler itself
// is ordinary code with no resource awareness.
export function handler(event, context) {
  console.log(
    `handled ${event.recordType} on ${event.collectionName} (attempt ${context.attempt}/${context.maxAttempts})`,
  );
}
