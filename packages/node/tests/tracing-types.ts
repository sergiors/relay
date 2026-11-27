import { Tracer } from "../src/tracing.js";

const tracer = new Tracer("types");
const syncResult: number = tracer.span("sync", (span) => {
  span.addEvent("sync");
  return 1;
});
const asyncResult: Promise<string> = tracer.span(
  "async",
  { recordException: true },
  async (span) => {
    span.addEvent("async");
    return "done";
  },
);
declare const thenable: PromiseLike<string>;
const thenableResult: Promise<string> = tracer.span("thenable", () => thenable);

const receiver = {
  prefix: "hello",
  greet(this: { prefix: string }, name: string): string {
    return `${this.prefix} ${name}`;
  },
};
const captured: typeof receiver.greet = tracer.captureMethod(receiver.greet);
const result: string = captured.call(receiver, "world");

function identity<T>(value: T): T {
  return value;
}
const capturedGeneric: typeof identity = tracer.captureMethod(identity);
const genericResult: boolean = capturedGeneric(true);

export {
  asyncResult,
  genericResult,
  result,
  syncResult,
  thenableResult,
};
