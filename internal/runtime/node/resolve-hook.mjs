// Relay's Node.js ESM/CJS resolve hook for SOURCE_MOUNT.
//
// This module is the ONE shared implementation of the bare-specifier
// re-anchoring that keeps a source-mounted container's dependency image
// authoritative. The Node engine writes it into the image at
// /relay/resolve-hook.mjs (only for a SOURCE_MOUNT image; a baked image never
// receives it) and it is consumed two ways:
//
//   - the pooled invocation bootstrap (bootstrap.mjs) dynamically imports it
//     when it runs in source-mount mode, so a handler's bare imports resolve
//     from the dependency tree; and
//   - a mounted Node entrypoint service runs it as a preload
//     (`node --import /relay/resolve-hook.mjs /app/src/<entry>`), because a
//     service process never runs the bootstrap.
//
// Both paths rely on the module SELF-INSTALLING at import time: importing it
// (dynamically, or through --import) calls registerHooks synchronously before
// any user module is evaluated.
//
// The hook re-anchors a BARE package specifier whose PARENT module lives under
// the mounted source root (/app/src) or the generated TypeScript overlay
// (/tmp/relay-gen) to the dependency root (/app), the image workdir where the
// dependency image's node_modules and the managed @opentelemetry/api live.
// Relative, absolute, URL-scheme ("node:", "file:", "data:", ...), and
// package-imports ("#...") specifiers keep MOUNT_ROOT as their resolution root.
//
// The one exception is the app's OWN package self-reference. Node enables a
// package self-reference ONLY for a manifest that defines both a non-empty
// "name" and an "exports" field; the mounted manifest (/app/src/package.json)
// then owns the "exports" targets, which are APP SOURCE paths under the mount —
// not under the dependency image's /app, where they do not exist. Re-anchoring
// such a specifier to /app would look the app name up in the dependency tree and
// fail (or resolve a different package). So when the specifier is exactly the
// mounted app's name (or a subpath of it) and the mounted manifest declares
// name+exports, the hook resolves it THROUGH the mounted manifest with the
// module system's native semantics:
//
//   - ESM (conditions include "import", never "require"): delegate to Node's
//     resolver with the parent URL re-anchored at /app/src/package.json, so the
//     manifest's own "exports" conditions select the target natively.
//   - CommonJS (conditions include "require"): resolve through a require()
//     bound to /app/src/package.json, which applies the manifest's "require"
//     export conditions natively.
//
// The exception covers parents under the mount (/app/src) AND parents under the
// generated TypeScript overlay (/tmp/relay-gen): a bundled TS handler imports
// its own package by name as an EXTERNAL specifier, and the generated bundle
// lives outside the app's package scope, so the hook is what resolves it. Every
// OTHER bare specifier from either location anchors to the dependency root.
//
// ESM and CommonJS need different mechanisms for the dependency re-anchor too,
// because Node's synchronous default resolver honors a re-anchored parentURL
// only for ESM:
//
//   - ESM: delegate to Node's own resolver with the parent URL re-anchored at
//     /app/package.json, so package "exports"/"imports" conditions and the
//     shared @opentelemetry/api singleton are exactly those of a module loaded
//     from /app.
//   - CommonJS: Node's default require resolver IGNORES an overridden parentURL
//     and still walks node_modules from the ORIGINAL requiring module, so a
//     host node_modules under the mount would win. Instead resolve the bare
//     specifier through a require() bound to /app/package.json
//     (createRequire), which walks /app/node_modules with the dependency image's
//     "require" export conditions, and short-circuit with the resolved file URL.
//     Builtins ("fs", "fs/promises", ...) are not files and are deferred to Node
//     so they keep their builtin identity.
//
// Matching uses both the lexical path and its resolved real path for the mount
// and the generated overlay, so a host or test layout that reaches the same
// directory through a symlink is covered too.

import { registerHooks, createRequire, isBuiltin } from "node:module";
import { realpathSync, readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

// These MUST match the Node engine's constants (internal/runtime/node/engine.go:
// SourceMountTarget and workDir) and the bootstrap's GENERATED_ROOT
// (bootstrap.mjs); the mounted plan pins them and the image's bind mount targets
// MOUNT_ROOT.
const MOUNT_ROOT = "/app/src";
const BAKED_ROOT = "/app";
// GENERATED_ROOT is the writable overlay the bootstrap bundles a mounted
// TypeScript handler into. It mirrors bootstrap.mjs's GENERATED_ROOT; a
// generated module's parent URL is outside the mount, so it is matched
// separately.
const GENERATED_ROOT = "/tmp/relay-gen";

// anchor is the dependency-image resolution base: /app/package.json, beside the
// dependency image's node_modules and the managed @opentelemetry/api. It is the
// parentURL for the ESM re-anchor and the require() base for the CommonJS
// resolution.
const anchor = pathToFileURL(BAKED_ROOT + "/package.json").href;

// bakedRequire is a require() function whose node_modules walk starts at
// BAKED_ROOT. CommonJS bare specifiers from the mount are resolved through it so
// the dependency tree — and its "require" export conditions — is authoritative.
const bakedRequire = createRequire(anchor);

// appSourceAnchor is the MOUNTED app's own manifest. A package self-reference
// must resolve through it (not the dependency root): its "exports" targets are
// app source paths under the mount. The ESM branch uses it as the parentURL and
// the CommonJS branch uses it as a require() base, so Node applies the manifest's
// own exports and conditions natively.
const appSourceAnchor = pathToFileURL(MOUNT_ROOT + "/package.json").href;
const appSourceRequire = createRequire(appSourceAnchor);

// appPackageName is the mounted app's package name when the manifest is a valid
// self-reference source (a non-empty "name" AND an "exports" field), or null
// otherwise. The manifest is immutable for a container's lifetime (a source edit
// rotates the warm generation), so it is read once. A manifest missing either
// field is not a self-reference source: its own name resolves like any other
// bare dependency through BAKED_ROOT/node_modules.
const appPackageName = readAppPackageName();

// readAppPackageName returns the mounted manifest's name when it declares both a
// non-empty "name" and an "exports" field. An unreadable or malformed manifest is
// treated as absent (no self-reference exception), leaving the dependency
// re-anchor in effect.
function readAppPackageName() {
  let raw;
  try {
    raw = readFileSync(MOUNT_ROOT + "/package.json", "utf8");
  } catch (e) {
    return null;
  }
  let pkg;
  try {
    pkg = JSON.parse(raw);
  } catch (e) {
    return null;
  }
  if (!pkg || typeof pkg.name !== "string" || pkg.name === "") return null;
  if (pkg.exports === undefined || pkg.exports === null) return null;
  return pkg.name;
}

// isBareSpecifier reports whether specifier is a bare package specifier (e.g.
// "pkg", "@scope/pkg", or "pkg/subpath"), as opposed to a relative, absolute,
// URL-scheme ("node:", "file:", "data:", ...), or package-imports ("#...")
// specifier. Only bare specifiers are re-anchored; every other specifier must
// keep resolving from the mounted source.
function isBareSpecifier(specifier) {
  if (specifier === "") return false;
  const first = specifier[0];
  if (first === "." || first === "/" || first === "#") return false;
  const colon = specifier.indexOf(":");
  if (colon > 0 && /^[a-zA-Z][a-zA-Z0-9+.-]*$/.test(specifier.slice(0, colon))) {
    return false;
  }
  return true;
}

// isAppSelfReference reports whether specifier is exactly the mounted app's own
// package name or a subpath of it, and the mounted manifest is a valid
// self-reference source (see appPackageName). Node enables self-reference only
// for a manifest with name+exports, so a manifest missing either is not a
// self-reference source.
function isAppSelfReference(specifier) {
  if (appPackageName === null) return false;
  return (
    specifier === appPackageName || specifier.startsWith(appPackageName + "/")
  );
}

// isRequireContext reports whether this resolution request is a CommonJS
// require() (or require.resolve) rather than an ESM import. Node populates the
// "require" export condition for a require and "import" for an import, and the
// two are mutually exclusive, so the condition list is the discriminator.
function isRequireContext(context) {
  return (
    Array.isArray(context.conditions) && context.conditions.includes("require")
  );
}

// resolvingSelfReference guards the CommonJS self-reference branch. The
// appSourceRequire base is /app/src/package.json, which is itself under
// MOUNT_ROOT, so the require.resolve() walk re-enters this hook; while the guard
// is set the hook delegates straight to Node, which resolves the self-reference
// natively against the mounted manifest. require.resolve is synchronous, so a
// module-level flag is sufficient.
let resolvingSelfReference = false;

// resolveAppSelfReference resolves a mounted app's package self-reference through
// the MOUNTED manifest using the module system's native semantics, so the
// manifest's own "exports" targets and conditions apply. A self-reference name is
// never a builtin; the builtin check is defensive symmetry with the dependency
// branch.
function resolveAppSelfReference(specifier, context, nextResolve) {
  if (isRequireContext(context)) {
    if (isBuiltin(specifier)) {
      return nextResolve(specifier, context);
    }
    resolvingSelfReference = true;
    try {
      const resolved = appSourceRequire.resolve(specifier);
      return { url: pathToFileURL(resolved).href, shortCircuit: true };
    } finally {
      resolvingSelfReference = false;
    }
  }
  // ESM: re-anchor the parent at the mounted manifest so Node's own self-reference
  // resolution reads that manifest's "exports" and conditions.
  return nextResolve(specifier, { ...context, parentURL: appSourceAnchor });
}

// installResolveHook registers the synchronous resolve hook. registerHooks is
// synchronous and applies to every subsequent import/require on this thread,
// including the main entry that follows a --import preload. Only bare
// specifiers whose parent module lives under MOUNT_ROOT or GENERATED_ROOT are
// considered.
function installResolveHook() {
  const sourcePrefix = pathToFileURL(MOUNT_ROOT + "/").href;
  let realSourcePrefix = sourcePrefix;
  try {
    realSourcePrefix = pathToFileURL(realpathSync(MOUNT_ROOT) + "/").href;
  } catch (e) {
    // The mount root is always present in a mounted container; if it is not
    // resolvable, the lexical comparison alone still covers the canonical path.
  }
  // The generated overlay is created lazily at the first TypeScript transpile,
  // after this hook installs, and Node resolves a loaded module URL to its REAL
  // path, so both the lexical and the real overlay prefix are matched. The real
  // prefix is only resolvable once the overlay directory exists, so it is
  // resolved lazily (and cached) on the first resolve that reaches the check.
  const generatedPrefixes = [pathToFileURL(GENERATED_ROOT + "/").href];
  let generatedRealResolved = false;

  function isUnderGenerated(parent) {
    for (const prefix of generatedPrefixes) {
      if (parent.startsWith(prefix)) return true;
    }
    if (!generatedRealResolved) {
      try {
        const real = pathToFileURL(realpathSync(GENERATED_ROOT) + "/").href;
        generatedPrefixes.push(real);
        generatedRealResolved = true;
        return parent.startsWith(real);
      } catch (e) {
        // The overlay does not exist yet; retry on a later resolve.
      }
    }
    return false;
  }

  registerHooks({
    resolve(specifier, context, nextResolve) {
      if (!isBareSpecifier(specifier)) {
        return nextResolve(specifier, context);
      }
      // Re-entrant call from the CommonJS self-reference branch: delegate to
      // Node so the mounted manifest resolves natively.
      if (resolvingSelfReference) {
        return nextResolve(specifier, context);
      }
      const parent = context.parentURL;
      if (typeof parent !== "string" || !parent.startsWith("file:")) {
        return nextResolve(specifier, context);
      }
      const inMount =
        parent.startsWith(sourcePrefix) || parent.startsWith(realSourcePrefix);
      // Only probe the generated overlay when the parent is not already known to
      // be under the mount: the two roots are disjoint, and this avoids a realpath
      // probe on every mounted-source bare import.
      const inGenerated = !inMount && isUnderGenerated(parent);
      if (!inMount && !inGenerated) {
        return nextResolve(specifier, context);
      }
      // The app's own self-reference resolves through the mounted manifest
      // whether the parent is mounted source or generated TypeScript output.
      if (isAppSelfReference(specifier)) {
        return resolveAppSelfReference(specifier, context, nextResolve);
      }
      // Every OTHER bare specifier — from mounted source or from the generated
      // TypeScript overlay — anchors to the dependency root.
      if (isRequireContext(context)) {
        // CommonJS: resolve through the dependency-image require so the walk
        // starts at /app/node_modules regardless of a host node_modules under
        // the mount. Builtins have no file; defer so Node keeps them builtin.
        if (isBuiltin(specifier)) {
          return nextResolve(specifier, context);
        }
        const resolved = bakedRequire.resolve(specifier);
        return { url: pathToFileURL(resolved).href, shortCircuit: true };
      }
      // ESM: re-anchor the parent at the baked workdir so the dependency
      // image's package "exports"/"imports" conditions are honored exactly.
      return nextResolve(specifier, { ...context, parentURL: anchor });
    },
  });
}

// Self-install: the module exists only to install the hook, and both consumers
// import it for that side effect (the bootstrap dynamically, a service via
// --import).
installResolveHook();
