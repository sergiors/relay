// Forces @opentelemetry/api resolution to fail for this child process — the
// standalone shape where the optional API package is not installed. The
// Logger's createRequire must treat the failure as "no telemetry", not crash.
"use strict";

const Module = require("node:module");

const originalResolve = Module._resolveFilename;
Module._resolveFilename = function (request, ...rest) {
  if (request === "@opentelemetry/api") {
    const error = new Error(`Cannot find module '${request}'`);
    error.code = "MODULE_NOT_FOUND";
    throw error;
  }
  return originalResolve.call(this, request, ...rest);
};
