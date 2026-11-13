import { readFileSync } from "node:fs";

import ts from "typescript";

// interfaceContract reads a declaration file and returns the declared
// members of one interface in declaration order:
// { name, kind: "property" | "method", type, optional }.
export function interfaceContract(file, interfaceName) {
  const source = readFileSync(file, "utf8");
  const sourceFile = ts.createSourceFile(
    file,
    source,
    ts.ScriptTarget.ES2024,
    true,
  );
  const declaration = sourceFile.statements.find(
    (node) =>
      ts.isInterfaceDeclaration(node) && node.name.text === interfaceName,
  );

  if (declaration === undefined) {
    throw new Error(`${file} does not declare interface ${interfaceName}`);
  }

  return declaration.members.map((member) => {
    const name = member.name.getText(sourceFile);
    const optional = member.questionToken !== undefined;

    if (ts.isMethodSignature(member)) {
      const params = member.parameters
        .map((parameter) => parameter.getText(sourceFile))
        .join(", ");
      return {
        name,
        kind: "method",
        params,
        type: member.type ? member.type.getText(sourceFile) : "void",
        optional,
      };
    }

    return {
      name,
      kind: "property",
      type: member.type.getText(sourceFile),
      optional,
    };
  });
}

// bootstrapContextMembers extracts the HandlerContext contract from Relay's
// Node bootstrap source: the fields the constructor assigns on `this` and
// the prototype methods, without importing or executing the bootstrap.
export function bootstrapContextMembers(file) {
  const source = readFileSync(file, "utf8");
  const start = source.indexOf("class HandlerContext {");

  if (start === -1) {
    throw new Error(`${file} no longer defines class HandlerContext`);
  }

  const open = source.indexOf("{", start);
  let depth = 0;
  let end = -1;
  for (let i = open; i < source.length; i += 1) {
    if (source[i] === "{") {
      depth += 1;
    } else if (source[i] === "}") {
      depth -= 1;
      if (depth === 0) {
        end = i;
        break;
      }
    }
  }

  if (end === -1) {
    throw new Error(`unterminated HandlerContext class in ${file}`);
  }

  const block = source.slice(start, end + 1);
  const fields = [...block.matchAll(/this\.(\w+)\s*=/g)].map((m) => m[1]);
  const methods = [...block.matchAll(/^ {2}(\w+)\s*\(/gm)]
    .map((m) => m[1])
    .filter((name) => name !== "constructor");

  return { fields, methods };
}
