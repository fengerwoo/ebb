#!/usr/bin/env node

import { workerMain } from "./ebb-query-data.mjs";

process.exitCode = await workerMain(process.argv.slice(2));
