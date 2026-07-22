import { spawn } from "node:child_process";

export function runProcessWithTimeout(
  command,
  args,
  {
    timeoutMs,
    env = process.env,
    cwd = process.cwd(),
    stdout = process.stdout,
    stderr = process.stderr,
    killGraceMs = 1_000,
  },
) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let timedOut = false;
    let forceKillTimer = null;

    child.stdout.on("data", (chunk) => stdout.write(chunk));
    child.stderr.on("data", (chunk) => stderr.write(chunk));
    child.once("error", reject);

    const timeoutTimer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
      forceKillTimer = setTimeout(() => child.kill("SIGKILL"), killGraceMs);
    }, timeoutMs);

    child.once("close", (exitCode, signal) => {
      clearTimeout(timeoutTimer);
      if (forceKillTimer) {
        clearTimeout(forceKillTimer);
      }
      resolve({ exitCode, signal, timedOut });
    });
  });
}
