export class EbbQueryError extends Error {
  constructor(code, message, options = {}) {
    super(message, options);
    this.name = "EbbQueryError";
    this.code = code;
    this.exitCode = options.exitCode ?? 1;
  }
}

export function fail(code, message, exitCode = 1) {
  throw new EbbQueryError(code, message, { exitCode });
}

export function errorPayload(error) {
  return {
    error: {
      code: error instanceof EbbQueryError ? error.code : "INTERNAL_ERROR",
      message: error instanceof Error ? error.message : String(error),
    },
  };
}
