require("dotenv").config();

const os = require("os");
const http = require("http");
const express = require("express");
const cors = require("cors");
const { Server: SocketIOServer } = require("socket.io");
const IORedis = require("ioredis");
const { createClient } = require("@supabase/supabase-js");
const { Queue, Worker, QueueEvents, Job } = require("bullmq");
const { v4: uuidv4 } = require("uuid");

/* =========================
   CONFIG
========================= */

function toNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toBoolean(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

const IS_PROD = (process.env.NODE_ENV || "development") === "production";

const config = {
  app: {
    name: process.env.APP_NAME || "product-search-system",
    env: process.env.NODE_ENV || "development",
    port: toNumber(process.env.PORT, 4000),
    version: process.env.APP_VERSION || "1.0.0",
    hostname: os.hostname(),
  },
  redis: {
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: toNumber(process.env.REDIS_PORT, 6379),
    password: process.env.REDIS_PASSWORD || undefined,
    db: toNumber(process.env.REDIS_DB, 0),
    family: toNumber(process.env.REDIS_FAMILY, 4),
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    lazyConnect: false,
    // Production: use TLS for Render Redis (e.g. Upstash / Render Redis)
    tls: toBoolean(process.env.REDIS_TLS, false) ? {} : undefined,
    retryStrategy(times) {
      if (times > 20) return null; // stop retrying after 20 attempts
      return Math.min(times * 200, 5000);
    },
  },
  supabase: {
    url: process.env.SUPABASE_URL,
    serviceRoleKey:
      process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY,
  },
  queue: {
    name: process.env.QUEUE_NAME || "ProductSearchQueue",
    jobName: process.env.QUEUE_JOB_NAME || "search-product",
    attempts: toNumber(process.env.QUEUE_ATTEMPTS, 3),
    backoffDelayMs: toNumber(process.env.QUEUE_BACKOFF_DELAY_MS, 2000),
    removeOnComplete: toNumber(process.env.JOB_RETENTION_COMPLETED, 500),
    removeOnFail: toNumber(process.env.JOB_RETENTION_FAILED, 1000),
    timeoutMs: toNumber(process.env.JOB_TIMEOUT_MS, 10 * 60 * 1000),
    rateLimiterMax: toNumber(process.env.QUEUE_RATE_LIMIT_MAX, 20),
    rateLimiterDurationMs: toNumber(
      process.env.QUEUE_RATE_LIMIT_DURATION_MS,
      60 * 1000
    ),
    stalledInterval: toNumber(process.env.QUEUE_STALLED_INTERVAL_MS, 30000),
    maxStalledCount: toNumber(process.env.QUEUE_MAX_STALLED_COUNT, 1),
  },
  worker: {
    id: process.env.WORKER_ID || `${os.hostname()}-${process.pid}`,
    concurrency: toNumber(process.env.WORKER_CONCURRENCY, 4),
    heartbeatIntervalMs: toNumber(process.env.WORKER_HEARTBEAT_MS, 15000),
    offlineTimeoutMs: toNumber(process.env.WORKER_OFFLINE_TIMEOUT_MS, 90000),
    sweepIntervalMs: toNumber(process.env.WORKER_SWEEP_INTERVAL_MS, 30000),
  },
  logging: {
    level: process.env.LOG_LEVEL || (IS_PROD ? "info" : "debug"),
  },
  cors: {
    // Comma-separated origins in env, or wildcard for dev
    origin: process.env.CORS_ORIGIN
      ? process.env.CORS_ORIGIN.split(",").map((s) => s.trim())
      : "*",
  },
};

if (!config.supabase.url || !config.supabase.serviceRoleKey) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

/* =========================
   LOGGER
========================= */

const levels = ["debug", "info", "warn", "error"];
const currentLevelIndex = levels.indexOf(config.logging.level);

function shouldLog(level) {
  const idx = levels.indexOf(level);
  if (idx === -1) return true;
  return idx >= currentLevelIndex;
}

function write(level, message, meta = {}) {
  if (!shouldLog(level)) return;
  const payload = {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...meta,
  };
  const line = JSON.stringify(payload);
  if (level === "error") {
    process.stderr.write(line + "\n");
    return;
  }
  process.stdout.write(line + "\n");
}

const logger = {
  debug: (message, meta = {}) => write("debug", message, meta),
  info: (message, meta = {}) => write("info", message, meta),
  warn: (message, meta = {}) => write("warn", message, meta),
  error: (message, meta = {}) => write("error", message, meta),
};

/* =========================
   ERRORS
========================= */

class AppError extends Error {
  constructor(message, statusCode = 500, code = "APP_ERROR", details = null) {
    super(message);
    this.name = "AppError";
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

function buildError(message, statusCode = 500, code = "APP_ERROR", details = null) {
  return new AppError(message, statusCode, code, details);
}

/* =========================
   SYSTEM
========================= */

function getMemoryStats() {
  const memory = process.memoryUsage();
  return {
    rss: memory.rss,
    heapTotal: memory.heapTotal,
    heapUsed: memory.heapUsed,
    external: memory.external,
    arrayBuffers: memory.arrayBuffers,
    systemFree: os.freemem(),
    systemTotal: os.totalmem(),
  };
}

function getCpuStats() {
  return {
    cores: os.cpus().length,
    loadAvg: os.loadavg(),
    usage: process.cpuUsage(),
    architecture: os.arch(),
    platform: os.platform(),
  };
}

function getWorkerSystemInfo() {
  return {
    hostname: os.hostname(),
    cpu: getCpuStats(),
    memory: getMemoryStats(),
    nodeVersion: process.version,
    pid: process.pid,
  };
}

/* =========================
   REDIS
========================= */

function createRedisConnection(label = "default") {
  const opts = {
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password,
    db: config.redis.db,
    family: config.redis.family,
    maxRetriesPerRequest: config.redis.maxRetriesPerRequest,
    enableReadyCheck: config.redis.enableReadyCheck,
    lazyConnect: config.redis.lazyConnect,
    retryStrategy: config.redis.retryStrategy,
  };
  if (config.redis.tls) opts.tls = config.redis.tls;

  const connection = new IORedis(opts);
  connection.__label = label;
  return connection;
}

const queueConnection = createRedisConnection("queue");
const workerConnection = createRedisConnection("worker");
const eventsConnection = createRedisConnection("events");

for (const connection of [queueConnection, workerConnection, eventsConnection]) {
  connection.on("connect", () => {
    logger.info("Redis connected", { connection: connection.__label });
  });
  connection.on("ready", () => {
    logger.info("Redis ready", { connection: connection.__label });
  });
  connection.on("error", (error) => {
    logger.error("Redis connection error", {
      connection: connection.__label,
      error: error.message,
    });
  });
  connection.on("close", () => {
    logger.warn("Redis connection closed", { connection: connection.__label });
  });
  connection.on("reconnecting", () => {
    logger.info("Redis reconnecting", { connection: connection.__label });
  });
}

/* =========================
   SUPABASE
========================= */

const supabase = createClient(
  config.supabase.url,
  config.supabase.serviceRoleKey,
  {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  }
);

/* =========================
   DB REPOS
========================= */

async function upsertWorker(row) {
  const { data, error } = await supabase
    .from("workers")
    .upsert([row], { onConflict: "worker_id" })
    .select()
    .single();
  if (error) throw error;
  return data;
}

async function updateWorker(workerId, patch) {
  const { data, error } = await supabase
    .from("workers")
    .update(patch)
    .eq("worker_id", workerId)
    .select()
    .single();
  if (error) throw error;
  return data;
}

async function getWorkers(filter = {}) {
  let query = supabase.from("workers").select("*");
  if (filter.status) {
    query = query.eq("status", filter.status);
  }
  const { data, error } = await query.order("last_heartbeat", { ascending: false });
  if (error) throw error;
  return data || [];
}

async function getStaleWorkers(cutoffIso) {
  const { data, error } = await supabase
    .from("workers")
    .select("*")
    .neq("status", "offline")
    .lt("last_heartbeat", cutoffIso);
  if (error) throw error;
  return data || [];
}

async function markWorkersOffline(workerIds) {
  if (!workerIds.length) return [];
  const { data, error } = await supabase
    .from("workers")
    .update({
      status: "offline",
      updated_at: new Date().toISOString(),
    })
    .in("worker_id", workerIds)
    .select();
  if (error) throw error;
  return data || [];
}

async function upsertJobStatus(row) {
  const { data, error } = await supabase
    .from("job_status")
    .upsert([row], { onConflict: "job_id" })
    .select()
    .single();
  if (error) throw error;
  return data;
}

async function updateJobStatus(jobId, patch) {
  const { data, error } = await supabase
    .from("job_status")
    .update(patch)
    .eq("job_id", jobId)
    .select()
    .single();
  if (error) throw error;
  return data;
}

async function getJobStatusRow(jobId) {
  const { data, error } = await supabase
    .from("job_status")
    .select("*")
    .eq("job_id", jobId)
    .single();
  if (error) throw error;
  return data;
}

async function getCompletedJobs(limit = 50) {
  const { data, error } = await supabase
    .from("job_status")
    .select("*")
    .eq("status", "completed")
    .order("finished_at", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

async function getFailedJobs(limit = 50) {
  const { data, error } = await supabase
    .from("job_status")
    .select("*")
    .eq("status", "failed")
    .order("updated_at", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

async function insertJobLog(row) {
  const { error } = await supabase.from("job_logs").insert([row]);
  if (error) throw error;
}

async function getProductsCountByJobId(jobId) {
  const { count, error } = await supabase
    .from("products")
    .select("*", { count: "exact", head: true })
    .eq("job_id", jobId);
  if (error) throw error;
  return count || 0;
}

async function getProductsByJobId(jobId) {
  const { data, error } = await supabase
    .from("products")
    .select("*")
    .eq("job_id", jobId)
    .order("last_seen_at", { ascending: false });
  if (error) throw error;
  return data || [];
}

/* =========================
   QUEUE
========================= */

const searchQueue = new Queue(config.queue.name, {
  connection: queueConnection,
  defaultJobOptions: {
    attempts: config.queue.attempts,
    backoff: {
      type: "exponential",
      delay: config.queue.backoffDelayMs,
    },
    removeOnComplete: config.queue.removeOnComplete,
    removeOnFail: config.queue.removeOnFail,
  },
});

const queueEvents = new QueueEvents(config.queue.name, {
  connection: eventsConnection,
});

async function safeQueueLog(event, payload = {}) {
  logger.info(`Queue event: ${event}`, payload);
  const jobId = payload.jobId || null;
  if (!jobId) return;
  try {
    await insertJobLog({
      job_id: jobId,
      timestamp: new Date().toISOString(),
      level: "info",
      message: `Queue event: ${event}`,
      worker: payload.prev || payload.workerId || null,
      duration: null,
    });
  } catch (_) {}
}

queueEvents.on("completed", async ({ jobId, returnvalue }) => {
  await safeQueueLog("completed", { jobId });
  try {
    let parsed = returnvalue;
    if (typeof returnvalue === "string") {
      try { parsed = JSON.parse(returnvalue); } catch (_) {}
    }
    await updateJobStatus(jobId, {
      status: "completed",
      progress: 100,
      finished_at: new Date().toISOString(),
      result: parsed || null,
      updated_at: new Date().toISOString(),
    });
  } catch (_) {}
});

queueEvents.on("failed", async ({ jobId, failedReason }) => {
  logger.error("Queue event: failed", { jobId, failedReason });
  try {
    await updateJobStatus(jobId, {
      status: "failed",
      error_message: failedReason,
      updated_at: new Date().toISOString(),
    });
    await insertJobLog({
      job_id: jobId,
      timestamp: new Date().toISOString(),
      level: "error",
      message: failedReason || "Job failed",
      worker: null,
      duration: null,
    });
  } catch (_) {}
});

queueEvents.on("stalled", ({ jobId }) => safeQueueLog("stalled", { jobId }));
queueEvents.on("progress", ({ jobId, data }) => safeQueueLog("progress", { jobId, data }));
queueEvents.on("drained", () => safeQueueLog("drained"));
queueEvents.on("waiting", ({ jobId }) => safeQueueLog("waiting", { jobId }));
queueEvents.on("active", ({ jobId, prev }) => safeQueueLog("active", { jobId, prev }));
queueEvents.on("removed", ({ jobId }) => safeQueueLog("removed", { jobId }));
queueEvents.on("cleaned", (payload) => safeQueueLog("cleaned", payload));

/* =========================
   BUYHATKE SCRAPER IMPORT
========================= */

const { processBuyhatkeJob } = require("./worker");

/* =========================
   LIVE STREAMING (SOCKET.IO)
========================= */

let io = null;

function emitToJob(jobId, event, payload) {
  if (!io || !jobId) return;
  io.to(String(jobId)).emit(event, { jobId, ...payload, ts: Date.now() });
}

function pushProductLive(jobId, product) {
  emitToJob(jobId, "product", { product });
}

/* =========================
   WORKER SERVICE
========================= */

async function registerWorker(workerId, queueName, status = "online") {
  const system = getWorkerSystemInfo();
  const now = new Date().toISOString();
  const row = {
    worker_id: workerId,
    hostname: system.hostname,
    cpu: JSON.stringify(system.cpu),
    memory: JSON.stringify(system.memory),
    version: process.version,
    started_at: now,
    last_heartbeat: now,
    queue_name: queueName,
    status,
    current_job: null,
    jobs_processing: 0,
    updated_at: now,
  };
  const worker = await upsertWorker(row);
  logger.info("Worker registered", {
    workerId,
    hostname: system.hostname,
    queueName,
    status,
  });
  return worker;
}

async function sendHeartbeat(workerId, payload = {}) {
  const system = getWorkerSystemInfo();
  const now = new Date().toISOString();
  return updateWorker(workerId, {
    last_heartbeat: now,
    cpu: JSON.stringify(system.cpu),
    memory: JSON.stringify(system.memory),
    jobs_processing: payload.jobsProcessing ?? 0,
    current_job: payload.currentJob || null,
    queue_name: payload.queueName || null,
    status: payload.status || "online",
    updated_at: now,
  });
}

async function markWorkerStatus(workerId, status, extra = {}) {
  return updateWorker(workerId, {
    status,
    updated_at: new Date().toISOString(),
    ...extra,
  });
}

async function sweepOfflineWorkers() {
  const cutoffIso = new Date(Date.now() - config.worker.offlineTimeoutMs).toISOString();
  const stale = await getStaleWorkers(cutoffIso);
  if (!stale.length) return 0;
  const workerIds = stale.map((w) => w.worker_id);
  await markWorkersOffline(workerIds);
  for (const worker of stale) {
    logger.warn("Worker marked offline", {
      workerId: worker.worker_id,
      hostname: worker.hostname,
      lastHeartbeat: worker.last_heartbeat,
    });
  }
  return workerIds.length;
}

async function listWorkers() {
  await sweepOfflineWorkers();
  const workers = await getWorkers();
  return { success: true, count: workers.length, workers };
}

/* =========================
   JOB SERVICE
========================= */

function normalizeJobState(state) {
  switch (state) {
    case "waiting":
    case "delayed":
    case "prioritized":
    case "waiting-children":
      return "queued";
    case "active":
      return "running";
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    default:
      return "queued";
  }
}

async function createSearchJob({ query, priority = 0, delay = 0 }) {
  const trimmedQuery = String(query || "").trim();
  if (!trimmedQuery) {
    throw buildError("query is required", 400, "VALIDATION_ERROR");
  }

  const jobId = uuidv4();
  const createdAt = new Date().toISOString();

  const jobData = { jobId, query: trimmedQuery, createdAt };

  await searchQueue.add(config.queue.jobName, jobData, {
    jobId,
    priority,
    delay,
    timeout: config.queue.timeoutMs,
  });

  await upsertJobStatus({
    job_id: jobId,
    queue_name: config.queue.name,
    job_name: config.queue.jobName,
    query: trimmedQuery,
    status: "queued",
    progress: 0,
    products_scraped: 0,
    products_saved: 0,
    offers_found: 0,
    errors: 0,
    current_url: null,
    current_product: null,
    current_store: null,
    estimated_remaining_time: null,
    created_at: createdAt,
    updated_at: createdAt,
  });

  logger.info("Job queued", {
    jobId,
    query: trimmedQuery,
    priority,
    delay,
    queueName: config.queue.name,
  });

  emitToJob(jobId, "status", { status: "queued", query: trimmedQuery });

  return { success: true, jobId, status: "queued" };
}

async function getJobStatus(jobId) {
  const job = await Job.fromId(searchQueue, jobId);
  if (!job) throw buildError("Job not found", 404, "NOT_FOUND");

  const state = await job.getState();
  const productsSaved = await getProductsCountByJobId(jobId);
  let dbStatus = null;
  try { dbStatus = await getJobStatusRow(jobId); } catch (_) {}

  return {
    jobId,
    name: job.name,
    data: job.data,
    status: normalizeJobState(state),
    progress: job.progress || dbStatus?.progress || 0,
    attemptsMade: job.attemptsMade,
    timestamp: job.timestamp,
    processedOn: job.processedOn || null,
    finishedOn: job.finishedOn || null,
    failedReason: job.failedReason || dbStatus?.error_message || null,
    returnvalue: job.returnvalue || dbStatus?.result || null,
    productsSaved,
    dbStatus,
  };
}

async function getQueueSnapshot() {
  return searchQueue.getJobCounts(
    "waiting", "active", "completed", "failed", "delayed", "prioritized", "paused"
  );
}

async function pauseQueue() {
  await searchQueue.pause();
  return { success: true, paused: true };
}

async function resumeQueue() {
  await searchQueue.resume();
  return { success: true, paused: false };
}

/* =========================
   METRICS / HEALTH
========================= */

async function getQueueMetrics() {
  const counts = await searchQueue.getJobCounts(
    "waiting", "active", "completed", "failed", "delayed", "prioritized", "paused"
  );
  const waiting = await searchQueue.getWaiting(0, 20);
  const active = await searchQueue.getActive(0, 20);

  let queueLatency = null;
  if (waiting.length > 0) {
    queueLatency = Date.now() - waiting[0].timestamp;
  }

  return {
    queueSize:
      (counts.waiting || 0) +
      (counts.active || 0) +
      (counts.delayed || 0) +
      (counts.prioritized || 0),
    counts,
    queueLatencyMs: queueLatency,
    activeSample: active.length,
  };
}

async function getRedisLatency(redisConnection) {
  const startedAt = Date.now();
  await redisConnection.ping();
  return Date.now() - startedAt;
}

async function getMetrics(redisConnection) {
  const queue = await getQueueMetrics();
  const redisLatencyMs = await getRedisLatency(redisConnection);
  return {
    averageJobTimeMs: null,
    jobsPerMinute: null,
    productsPerMinute: null,
    averageProductsPerJob: null,
    workerUtilization: null,
    queueSize: queue.queueSize,
    queueLatencyMs: queue.queueLatencyMs,
    redisLatencyMs,
    counts: queue.counts,
  };
}

async function getRedisHealth(redisConnection) {
  try {
    const pong = await redisConnection.ping();
    return { status: pong === "PONG" ? "up" : "degraded" };
  } catch (error) {
    return { status: "down", error: error.message };
  }
}

async function getSupabaseHealth() {
  try {
    const { error } = await supabase
      .from("workers")
      .select("worker_id", { head: true, count: "exact" });
    if (error) return { status: "down", error: error.message };
    return { status: "up" };
  } catch (error) {
    return { status: "down", error: error.message };
  }
}

async function getServerHealth() {
  const memory = process.memoryUsage();
  const cpu = process.cpuUsage();
  const queue = await searchQueue.getJobCounts(
    "waiting", "active", "completed", "failed", "delayed", "prioritized"
  );
  return {
    status: "up",
    uptimeSeconds: Math.floor(process.uptime()),
    hostname: os.hostname(),
    pid: process.pid,
    memory,
    cpu,
    cores: os.cpus().length,
    loadAvg: os.loadavg(),
    queue,
  };
}

async function getHealthSnapshot(redisConnection) {
  const [server, redis, supabaseHealth] = await Promise.all([
    getServerHealth(),
    getRedisHealth(redisConnection),
    getSupabaseHealth(),
  ]);
  return { success: true, server, redis, supabase: supabaseHealth };
}

/* =========================
   WORKER RUNNER
========================= */

let currentJobId = null;
let jobsProcessing = 0;
let heartbeatTimer = null;
let sweepTimer = null;
let httpServer = null;
let bullWorker = null;

async function updateJobProgress(job, patch = {}) {
  const progress = patch.progress ?? 0;
  await job.updateProgress(progress);

  const dbPatch = {
    progress,
    updated_at: new Date().toISOString(),
  };

  if (patch.status) dbPatch.status = patch.status;
  if (patch.current_url !== undefined) dbPatch.current_url = patch.current_url;
  if (patch.current_product !== undefined) dbPatch.current_product = patch.current_product;
  if (patch.current_store !== undefined) dbPatch.current_store = patch.current_store;
  if (patch.products_scraped !== undefined) dbPatch.products_scraped = patch.products_scraped;
  if (patch.products_saved !== undefined) dbPatch.products_saved = patch.products_saved;
  if (patch.offers_found !== undefined) dbPatch.offers_found = patch.offers_found;
  if (patch.errors !== undefined) dbPatch.errors = patch.errors;
  if (patch.estimated_remaining_time !== undefined) {
    dbPatch.estimated_remaining_time = patch.estimated_remaining_time;
  }

  emitToJob(job.id, "progress", { ...dbPatch });

  await updateJobStatus(job.id, dbPatch);
}

async function appendJobLog(jobId, level, message, extra = {}) {
  emitToJob(jobId, "log", { level, message, ...extra });
  try {
    await insertJobLog({
      job_id: jobId,
      timestamp: new Date().toISOString(),
      level,
      message,
      worker: config.worker.id,
      duration: extra.duration ?? null,
    });
  } catch (_) {}
}

async function workerProcessor(job) {
  currentJobId = job.id;
  jobsProcessing += 1;

  try {
    await markWorkerStatus(config.worker.id, "online", {
      current_job: job.id,
      jobs_processing: jobsProcessing,
    });

    emitToJob(job.id, "status", { status: "running" });

    await updateJobStatus(job.id, {
      status: "running",
      started_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    });

    await appendJobLog(job.id, "info", `Started job for query: ${job.data.query}`);

    const result = await processBuyhatkeJob({
      job,
      supabase,
      logger,
      config,
      helpers: {
        updateJobProgress: async (patch) => updateJobProgress(job, patch),
        appendJobLog: async (level, message, extra = {}) =>
          appendJobLog(job.id, level, message, extra),
        pushProductLive: (product) => pushProductLive(job.id, product),
        cacheProduct: async () => {}, // extend if you add Redis caching later
      },
    });

    const productsSaved = await getProductsCountByJobId(job.id);

    emitToJob(job.id, "completed", { result, productsSaved });

    await updateJobStatus(job.id, {
      status: "completed",
      progress: 100,
      products_saved: productsSaved,
      finished_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      result,
    });

    await appendJobLog(job.id, "info", "Job completed successfully");

    return result;
  } catch (error) {
    logger.error("Worker job failed", {
      workerId: config.worker.id,
      jobId: job.id,
      error: error.message,
      stack: error.stack,
    });

    emitToJob(job.id, "failed", { error: error.message });

    await updateJobStatus(job.id, {
      status: "failed",
      error_message: error.message,
      updated_at: new Date().toISOString(),
    }).catch(() => {});

    await appendJobLog(job.id, "error", error.message).catch(() => {});

    throw error;
  } finally {
    jobsProcessing = Math.max(0, jobsProcessing - 1);
    currentJobId = null;

    await markWorkerStatus(config.worker.id, "online", {
      current_job: null,
      jobs_processing: jobsProcessing,
    }).catch(() => {});
  }
}

function createBullWorker() {
  return new Worker(config.queue.name, workerProcessor, {
    connection: workerConnection,
    concurrency: config.worker.concurrency,
    limiter: {
      max: config.queue.rateLimiterMax,
      duration: config.queue.rateLimiterDurationMs,
    },
    stalledInterval: config.queue.stalledInterval,
    maxStalledCount: config.queue.maxStalledCount,
  });
}

/* =========================
   EXPRESS SERVER
========================= */

const app = express();

// Trust Render's reverse proxy so req.ip / rate limiting work correctly
app.set("trust proxy", 1);

app.use(cors({
  origin: config.cors.origin,
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
}));

app.use(express.json({ limit: "1mb" }));

// Security headers (lightweight — add helmet if you want more)
app.use((_req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("X-XSS-Protection", "1; mode=block");
  next();
});

app.get("/", (_req, res) => {
  res.json({
    success: true,
    name: config.app.name,
    version: config.app.version,
    env: config.app.env,
  });
});

app.post("/jobs", async (req, res, next) => {
  try {
    const { query, priority = 0, delay = 0 } = req.body || {};
    const result = await createSearchJob({ query, priority, delay });
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

app.get("/jobs/:jobId", async (req, res, next) => {
  try {
    const result = await getJobStatus(req.params.jobId);
    res.json({ success: true, ...result });
  } catch (error) {
    next(error);
  }
});

app.get("/jobs/:jobId/products", async (req, res, next) => {
  try {
    const products = await getProductsByJobId(req.params.jobId);
    res.json({ success: true, count: products.length, products });
  } catch (error) {
    next(error);
  }
});

app.get("/jobs/completed/list", async (req, res, next) => {
  try {
    const limit = toNumber(req.query.limit, 50);
    const jobs = await getCompletedJobs(limit);
    res.json({ success: true, count: jobs.length, jobs });
  } catch (error) {
    next(error);
  }
});

app.get("/jobs/failed/list", async (req, res, next) => {
  try {
    const limit = toNumber(req.query.limit, 50);
    const jobs = await getFailedJobs(limit);
    res.json({ success: true, count: jobs.length, jobs });
  } catch (error) {
    next(error);
  }
});

app.get("/queue", async (req, res, next) => {
  try {
    const snapshot = await getQueueSnapshot();
    res.json({ success: true, queueName: config.queue.name, snapshot });
  } catch (error) {
    next(error);
  }
});

app.post("/queue/pause", async (req, res, next) => {
  try {
    res.json(await pauseQueue());
  } catch (error) {
    next(error);
  }
});

app.post("/queue/resume", async (req, res, next) => {
  try {
    res.json(await resumeQueue());
  } catch (error) {
    next(error);
  }
});

app.get("/workers", async (req, res, next) => {
  try {
    res.json(await listWorkers());
  } catch (error) {
    next(error);
  }
});

app.get("/metrics", async (req, res, next) => {
  try {
    const result = await getMetrics(queueConnection);
    res.json({ success: true, metrics: result });
  } catch (error) {
    next(error);
  }
});

app.get("/health", async (req, res, next) => {
  try {
    const result = await getHealthSnapshot(queueConnection);
    const overallUp =
      result.server.status === "up" &&
      result.redis.status !== "down" &&
      result.supabase.status !== "down";
    res.status(overallUp ? 200 : 503).json(result);
  } catch (error) {
    next(error);
  }
});

/* =========================
   ERROR HANDLER
========================= */

// eslint-disable-next-line no-unused-vars
app.use((error, req, res, next) => {
  const statusCode = error.statusCode || 500;
  const code = error.code || "INTERNAL_SERVER_ERROR";

  logger.error("Request failed", {
    path: req.path,
    method: req.method,
    error: error.message,
    code,
    // Only include stack in non-production to avoid leaking internals
    ...(IS_PROD ? {} : { stack: error.stack }),
  });

  res.status(statusCode).json({
    success: false,
    error: {
      code,
      message: error.message || "Internal server error",
      details: IS_PROD ? null : (error.details || null),
    },
  });
});

/* =========================
   SHUTDOWN
========================= */

function registerShutdown(handler) {
  let shuttingDown = false;

  async function wrapped(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.warn("Shutdown signal received", { signal });

    try {
      await handler(signal);
      logger.info("Graceful shutdown complete", { signal });
      process.exit(0);
    } catch (error) {
      logger.error("Graceful shutdown failed", {
        signal,
        error: error.message,
        stack: error.stack,
      });
      process.exit(1);
    }
  }

  process.on("SIGINT", () => wrapped("SIGINT"));
  process.on("SIGTERM", () => wrapped("SIGTERM"));

  process.on("unhandledRejection", (reason) => {
    logger.error("Unhandled promise rejection", {
      error: reason instanceof Error ? reason.message : String(reason),
      stack: reason instanceof Error ? reason.stack : undefined,
    });
  });

  process.on("uncaughtException", (error) => {
    logger.error("Uncaught exception", {
      error: error.message,
      stack: error.stack,
    });
    process.exit(1);
  });
}

/* =========================
   BOOTSTRAP
========================= */

async function start() {
  await registerWorker(config.worker.id, config.queue.name, "online");

  bullWorker = createBullWorker();

  bullWorker.on("ready", () => {
    logger.info("BullMQ worker ready", {
      workerId: config.worker.id,
      queueName: config.queue.name,
      concurrency: config.worker.concurrency,
    });
  });

  bullWorker.on("error", (error) => {
    logger.error("BullMQ worker error", {
      workerId: config.worker.id,
      error: error.message,
      stack: error.stack,
    });
  });

  heartbeatTimer = setInterval(async () => {
    try {
      await sendHeartbeat(config.worker.id, {
        jobsProcessing,
        currentJob: currentJobId,
        queueName: config.queue.name,
        status: "online",
      });
    } catch (error) {
      logger.error("Worker heartbeat failed", {
        workerId: config.worker.id,
        error: error.message,
      });
    }
  }, config.worker.heartbeatIntervalMs);

  sweepTimer = setInterval(async () => {
    try {
      await sweepOfflineWorkers();
    } catch (error) {
      logger.error("Offline worker sweep failed", { error: error.message });
    }
  }, config.worker.sweepIntervalMs);

  httpServer = http.createServer(app);

  io = new SocketIOServer(httpServer, {
    cors: {
      origin: config.cors.origin,
      methods: ["GET", "POST"],
    },
    // Render supports long-polling; prefer WebSocket but allow fallback
    transports: ["websocket", "polling"],
    // Tune for Render's 30s idle timeout on free tier
    pingTimeout: 20000,
    pingInterval: 10000,
  });

  io.on("connection", (socket) => {
    logger.debug("Socket connected", { socketId: socket.id });

    socket.on("join", (jobId) => {
      if (typeof jobId === "string" && jobId.length > 0) {
        socket.join(jobId);
        logger.debug("Socket joined job room", { socketId: socket.id, jobId });
      }
    });

    socket.on("disconnect", (reason) => {
      logger.debug("Socket disconnected", { socketId: socket.id, reason });
    });
  });

  httpServer.listen(config.app.port, () => {
    logger.info("Server started", {
      port: config.app.port,
      env: config.app.env,
      app: config.app.name,
      workerId: config.worker.id,
      liveStreaming: true,
    });
  });
}

registerShutdown(async () => {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  if (sweepTimer) clearInterval(sweepTimer);

  await markWorkerStatus(config.worker.id, "offline", {
    current_job: null,
    jobs_processing: 0,
  }).catch(() => {});

  if (io) {
    await new Promise((resolve) => io.close(() => resolve()));
  } else if (httpServer) {
    await new Promise((resolve, reject) => {
      httpServer.close((err) => (err ? reject(err) : resolve()));
    });
  }

  if (bullWorker) await bullWorker.close();

  await queueEvents.close().catch(() => {});
  await searchQueue.close().catch(() => {});

  await Promise.allSettled([
    queueConnection.quit(),
    workerConnection.quit(),
    eventsConnection.quit(),
  ]);
});

start().catch((error) => {
  logger.error("Application failed to start", {
    error: error.message,
    stack: error.stack,
  });
  process.exit(1);
});
