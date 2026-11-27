// Supported import mechanisms and the packed package: ESM static import and
// CommonJS dynamic import() resolve the documented subpath through package
// self-reference, native require() is rejected as documented, and an
// npm-installed and pnpm-installed tarball exposes working exports, the
// same require() rejection, and type declarations that compile for a
// consumer.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const typescriptTsc = join(packageRoot, "node_modules", "typescript", "bin", "tsc");
const typeRoots = join(packageRoot, "node_modules", "@types");

function runNode(args, cwd) {
  return spawnSync(process.execPath, args, { cwd, encoding: "utf8" });
}

function jsonLines(text) {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.startsWith("{"))
    .map((line) => JSON.parse(line));
}

const REQUIRE_PROBE =
  'try { require("@sergiors/relay-runtime/logging"); process.exit(2); }' +
  'catch (e) { console.log(e.code); }';

test("ESM import and CommonJS dynamic import are supported, native require is not", () => {
  // ESM static import through the package's own name (self-reference)
  const esm = runNode(
    [
      "--input-type=module",
      "-e",
      'import { Logger, JsonFormatter } from "@sergiors/relay-runtime/logging";' +
        'console.log(typeof Logger + ":" + typeof JsonFormatter);',
    ],
    packageRoot,
  );
  assert.equal(esm.status, 0, `ESM self-import failed:\n${esm.stderr}`);
  assert.equal(esm.stdout.trim(), "function:function");

  // CommonJS dynamic import() of the same subpath
  const dynamic = runNode(
    [
      "-e",
      'import("@sergiors/relay-runtime/logging")' +
        '.then((m) => console.log(typeof m.Logger))' +
        '.catch((e) => { console.error(e.code || e.message); process.exit(1); });',
    ],
    packageRoot,
  );
  assert.equal(dynamic.status, 0, `CommonJS dynamic import failed:\n${dynamic.stderr}`);
  assert.equal(dynamic.stdout.trim(), "function");

  // native require() has no export — the documented ESM-only contract
  const native = runNode(["-e", REQUIRE_PROBE], packageRoot);
  assert.equal(native.status, 0, `require probe failed:\n${native.stderr}`);
  assert.equal(native.stdout.trim(), "ERR_PACKAGE_PATH_NOT_EXPORTED");
});

test("the packed package installs with npm and pnpm with working exports and types", (t) => {
  const npmVersion = spawnSync("npm", ["--version"], { encoding: "utf8" });
  const pnpmVersion = spawnSync("pnpm", ["--version"], { encoding: "utf8" });
  assert.equal(
    npmVersion.status,
    0,
    "environmental blocker: npm is required to install the packed package",
  );
  assert.equal(
    pnpmVersion.status,
    0,
    "environmental blocker: pnpm (the declared package manager) is required to pack and install",
  );
  assert.ok(
    existsSync(typescriptTsc),
    "environmental blocker: packages/node/node_modules is not installed (run pnpm install)",
  );

  const workspace = mkdtempSync(join(tmpdir(), "relay-runtime-pack-"));
  t.after(() => rmSync(workspace, { recursive: true, force: true }));

  // pack once with the declared package manager (npm pack as fallback); the
  // prepack hook builds dist
  const packDir = join(workspace, "pack");
  mkdirSync(packDir);
  const pack = spawnSync("pnpm", ["pack", "--pack-destination", packDir], {
    cwd: packageRoot,
    encoding: "utf8",
  });
  let packOutput = pack.stdout + pack.stderr;
  assert.equal(
    pack.status,
    0,
    `packing with pnpm failed (is packages/node installed?):\n${packOutput}`,
  );
  const tarballs = readdirSync(packDir).filter((name) => name.endsWith(".tgz"));
  assert.equal(tarballs.length, 1, `packing produced ${tarballs.length} tarballs`);
  const tarball = join(packDir, tarballs[0]);

  const consumerContract =
    'import type { HandlerContext } from "@sergiors/relay-runtime";\n' +
    'import {\n' +
    "  JsonFormatter,\n" +
    "  Logger,\n" +
    "  type LogAttributes,\n" +
    "  type LogLevelName,\n" +
    "  type LogRecord,\n" +
    "  type LoggerOptions,\n" +
    '} from "@sergiors/relay-runtime/logging";\n' +
    "\n" +
    'const level: LogLevelName = "INFO";\n' +
    'const options: LoggerOptions = { service: "types", level };\n' +
    "const log = new Logger(options);\n" +
    "const attributes: LogAttributes = { count: 1 };\n" +
    'log.info("typed %s", "record", attributes);\n' +
    "\n" +
    "const formatter = new JsonFormatter();\n" +
    "const line: string = formatter.format({} as LogRecord);\n" +
    "\n" +
    "declare const context: HandlerContext;\n" +
    "const requestId: string = context.requestId;\n" +
    "const remaining: number = context.getRemainingTimeMs();\n" +
    "\n" +
    "export { line, requestId, remaining, log };\n";

  for (const manager of ["npm", "pnpm"]) {
    const consumer = join(workspace, manager);
    mkdirSync(consumer);
    writeFileSync(
      join(consumer, "package.json"),
      JSON.stringify({
        name: `${manager}-consumer`,
        version: "0.0.0",
        private: true,
      }),
    );

    const install =
      manager === "npm"
        ? spawnSync(
            "npm",
            ["install", "--no-audit", "--no-fund", "--loglevel=error", tarball],
            { cwd: consumer, encoding: "utf8" },
          )
        : spawnSync("pnpm", ["add", tarball], { cwd: consumer, encoding: "utf8" });
    assert.equal(
      install.status,
      0,
      `${manager} install of the packed tarball failed:\n${install.stdout}\n${install.stderr}`,
    );

    // exports: the subpath resolves and runs
    const esm = runNode(
      [
        "--input-type=module",
        "-e",
        'import { Logger } from "@sergiors/relay-runtime/logging";' +
          'const log = new Logger({ service: "packed", level: "INFO" });' +
          'log.info("packed-ok", { via: "consumer" });',
      ],
      consumer,
    );
    assert.equal(esm.status, 0, `${manager} consumer ESM import failed:\n${esm.stderr}`);
    const [record] = jsonLines(esm.stderr).filter(
      (entry) => entry.message === "packed-ok",
    );
    assert.ok(record, `${manager} consumer emitted no record; stderr:\n${esm.stderr}`);
    assert.equal(record.service, "packed");
    assert.deepEqual(record.attributes, { via: "consumer" });

    // require() stays rejected from an installed copy too
    const native = runNode(["-e", REQUIRE_PROBE], consumer);
    assert.equal(native.status, 0, `${manager} require probe failed:\n${native.stderr}`);
    assert.equal(native.stdout.trim(), "ERR_PACKAGE_PATH_NOT_EXPORTED");

    // types: the packed declarations compile for the consumer
    writeFileSync(join(consumer, "contract.ts"), consumerContract);
    writeFileSync(
      join(consumer, "tsconfig.json"),
      JSON.stringify({
        compilerOptions: {
          module: "ESNext",
          moduleResolution: "Bundler",
          target: "ES2024",
          strict: true,
          noEmit: true,
          types: ["node"],
          typeRoots: [typeRoots],
        },
        files: ["contract.ts"],
      }),
    );
    const typecheck = spawnSync(
      process.execPath,
      [typescriptTsc, "-p", join(consumer, "tsconfig.json")],
      { cwd: consumer, encoding: "utf8" },
    );
    assert.equal(
      typecheck.status,
      0,
      `${manager} consumer typecheck failed:\n${typecheck.stdout}\n${typecheck.stderr}`,
    );
  }
});
