import { readFileSync } from "node:fs";
import path from "node:path";

const CGROUP_FILE = "/proc/self/cgroup";
const MOUNTINFO_FILE = "/proc/self/mountinfo";

function decodeMountField(value) {
  return value.replace(/\\(040|011|012|134)/g, (_, octal) =>
    String.fromCharCode(Number.parseInt(octal, 8)),
  );
}

function absolutePath(value) {
  if (!value.startsWith("/") || value.includes("\0")) return null;
  const normalized = path.posix.normalize(value);
  return normalized.startsWith("/") ? normalized : null;
}

function mapCgroupPath(root, mountPoint, membership) {
  const cleanRoot = absolutePath(root);
  const cleanMountPoint = absolutePath(mountPoint);
  const cleanMembership = absolutePath(membership);
  if (!cleanRoot || !cleanMountPoint || !cleanMembership) return null;

  const relative = path.posix.relative(cleanRoot, cleanMembership);
  if (
    relative === ".." ||
    relative.startsWith("../") ||
    path.posix.isAbsolute(relative)
  ) {
    return null;
  }

  const mapped = path.posix.resolve(cleanMountPoint, relative);
  const mountPrefix = cleanMountPoint.endsWith("/")
    ? cleanMountPoint
    : cleanMountPoint + "/";
  if (mapped !== cleanMountPoint && !mapped.startsWith(mountPrefix)) {
    return null;
  }
  return mapped;
}

function parseMemberships(contents) {
  const memberships = { unified: null, v1Memory: null };
  for (const line of contents.split("\n")) {
    const first = line.indexOf(":");
    const second = line.indexOf(":", first + 1);
    if (first < 0 || second < 0) continue;

    const controllers = line.slice(first + 1, second).split(",");
    const membership = line.slice(second + 1);
    if (
      line.slice(0, first) === "0" &&
      controllers.length === 1 &&
      !controllers[0]
    ) {
      memberships.unified = membership;
    } else if (controllers.includes("memory")) {
      memberships.v1Memory = membership;
    }
  }
  return memberships;
}

function parseMounts(contents, memberships) {
  const unifiedCandidates = [];
  const v1MemoryCandidates = [];
  for (const line of contents.split("\n")) {
    const fields = line.split(" ");
    const separator = fields.indexOf("-", 6);
    if (separator < 6 || fields.length < separator + 4) continue;

    const root = decodeMountField(fields[3]);
    const mountPoint = decodeMountField(fields[4]);
    const fileSystem = fields[separator + 1];

    if (fileSystem === "cgroup2") {
      if (memberships.unified === null) continue;
      const directory = mapCgroupPath(
        root,
        mountPoint,
        memberships.unified,
      );
      if (directory === null) continue;
      unifiedCandidates.push({
        current: path.posix.join(directory, "memory.current"),
        peak: path.posix.join(directory, "memory.peak"),
      });
      continue;
    }

    if (
      fileSystem !== "cgroup" ||
      !fields[separator + 3].split(",").includes("memory") ||
      memberships.v1Memory === null
    ) {
      continue;
    }
    const directory = mapCgroupPath(
      root,
      mountPoint,
      memberships.v1Memory,
    );
    if (directory === null) continue;
    v1MemoryCandidates.push({
      current: path.posix.join(directory, "memory.usage_in_bytes"),
      peak: path.posix.join(directory, "memory.max_usage_in_bytes"),
    });
  }
  return [...unifiedCandidates, ...v1MemoryCandidates];
}

export function discoverCgroupMemoryPaths(cgroupContents, mountInfoContents) {
  return parseMounts(mountInfoContents, parseMemberships(cgroupContents));
}

function discoverRuntimePaths() {
  try {
    return discoverCgroupMemoryPaths(
      readFileSync(CGROUP_FILE, "utf8"),
      readFileSync(MOUNTINFO_FILE, "utf8"),
    );
  } catch {
    return [];
  }
}

let runtimePaths = [];
let pathsInitialized = false;

export function initializeCgroupMemory() {
  if (pathsInitialized) return;
  runtimePaths = discoverRuntimePaths();
  pathsInitialized = true;
}

export function parseCgroupMemoryValue(contents) {
  const value = contents.trim();
  if (!/^\d+$/.test(value)) return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function readCounter(file, readFile) {
  try {
    return parseCgroupMemoryValue(readFile(file, "utf8"));
  } catch {
    return null;
  }
}

export function readCgroupMemory(
  paths = runtimePaths,
  readFile = readFileSync,
) {
  for (const candidate of paths) {
    const currentBytes = readCounter(candidate.current, readFile);
    const peakBytes = readCounter(candidate.peak, readFile);
    if (currentBytes !== null || peakBytes !== null) {
      return { currentBytes, peakBytes };
    }
  }
  return { currentBytes: null, peakBytes: null };
}
