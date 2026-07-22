import fs from "node:fs";
import path from "node:path";

import { fail } from "./errors.mjs";

function rowObjects(result) {
  const names = result.columns.map((column) => column.name);
  return result.rows.map((row) => Object.fromEntries(names.map((name, index) => [name, row[index]])));
}

export function renderJson(result) {
  return `${JSON.stringify(
    {
      columns: result.columns,
      rows: rowObjects(result),
      rowCount: result.rowCount,
      truncated: result.truncated,
      elapsedMs: result.elapsedMs,
    },
    null,
    2,
  )}\n`;
}

function csvScalar(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function quoteCsv(value) {
  const text = csvScalar(value);
  if (/[",\r\n]/.test(text)) {
    return `"${text.replaceAll('"', '""')}"`;
  }
  return text;
}

export function renderCsv(result, { header = true } = {}) {
  const lines = [];
  if (header) {
    lines.push(result.columns.map((column) => quoteCsv(column.name)).join(","));
  }
  for (const row of result.rows) {
    lines.push(row.map(quoteCsv).join(","));
  }
  return `${lines.join("\n")}\n`;
}

export function renderResult(result, format, options = {}) {
  if (format === "json") {
    return renderJson(result);
  }
  if (format === "csv") {
    return renderCsv(result, options);
  }
  fail("ARGUMENT_INVALID", `unsupported output format: ${format}`, 2);
}

export function writeResult({
  content,
  outputPath,
  projectRoot,
  format,
  result,
  stdout = process.stdout,
}) {
  if (!outputPath || outputPath === "-") {
    stdout.write(content);
    return null;
  }

  if (path.isAbsolute(outputPath)) {
    fail("OUTPUT_PATH_INVALID", "--output must be relative to the project root", 2);
  }

  let root;
  let parent;
  const candidate = path.resolve(projectRoot || process.cwd(), outputPath);
  try {
    root = fs.realpathSync(projectRoot || process.cwd());
    parent = fs.realpathSync(path.dirname(candidate));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    fail("OUTPUT_PATH_INVALID", `cannot resolve output directory: ${message}`, 2);
  }

  const writePath = path.join(parent, path.basename(candidate));
  const relative = path.relative(root, writePath);
  if (
    relative === "" ||
    relative === ".." ||
    relative.startsWith(`..${path.sep}`) ||
    path.isAbsolute(relative)
  ) {
    fail("OUTPUT_PATH_INVALID", "--output must stay within the project root", 2);
  }

  let descriptor;
  try {
    const flags =
      fs.constants.O_WRONLY |
      fs.constants.O_CREAT |
      fs.constants.O_EXCL |
      (fs.constants.O_NOFOLLOW || 0);
    descriptor = fs.openSync(writePath, flags, 0o600);
    fs.writeFileSync(descriptor, content, { encoding: "utf8" });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    fail(
      "OUTPUT_WRITE_FAILED",
      `cannot create output file ${candidate}; the path must not already exist: ${message}`,
      2,
    );
  } finally {
    if (descriptor !== undefined) {
      fs.closeSync(descriptor);
    }
  }

  stdout.write(
    `${JSON.stringify({
      output: candidate,
      format,
      rowCount: result.rowCount,
      truncated: result.truncated,
      elapsedMs: result.elapsedMs,
    })}\n`,
  );
  return candidate;
}
