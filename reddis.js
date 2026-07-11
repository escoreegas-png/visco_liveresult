"use strict";
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
/**
 * Parse a Redis URL (redis:// or rediss://) into IORedis connection options.
 * Falls back to host/port/password env vars when no URL is provided.
 */
function parseRedisOptions() {
  const url = process.env.REDIS_URL;
  if (url) {
    try {
      const parsed = new URL(url);
      const useTls = parsed.protocol === "rediss:";
      return {
        host: parsed.hostname,
        port: toNumber(parsed.port, 6379),
        password: parsed.password ? decodeURIComponent(parsed.password) : undefined,
        username: parsed.username ? decodeURIComponent(parsed.username) : undefined,
        db: toNumber(parsed.pathname?.replace(/^\//, ""), 0),
        tls: useTls ? {} : undefined,
      };
    } catch (err) {
      // Will be caught at startup — crash early rather than silently use defaults
      throw new Error(`Invalid REDIS_URL: ${err.message}`);
    }
  }
  // Fallback: individual env vars
  return {
    host: process.env.REDIS_HOST || "127.0.0.1",
    port: toNumber(process.env.REDIS_PORT, 6379),
    password: process.env.REDIS_PASSWORD || undefined,
    db: toNumber(process.env.REDIS_DB, 0),
    tls: toBoolean(process.env.REDIS_TLS, false) ? {} : undefined,
  };
}
const IS_PROD = (process.env.NODE_ENV || "development") === "production";
const redisBase = parseRedisOptions();
const config = {
  app: {
    name: process.env.APP_NAME || "product-search-system",
    env: process.env.NODE_ENV || "development",
    port: toNumber(process.env.PORT, 4000),
    version: process.env.APP_VERSION || "1.0.0",
    hostname: os.hostname(),
  },
  redis: {
    ...redisBase,
    family: toNumber(process.env.REDIS_FAMILY, 4),
    maxRetriesPerRequest: null,  // Required by BullMQ
    enableReadyCheck: false,     // Upstash: skip READY check over TLS
    lazyConnect: false,
    connectTimeout: 10_000,
    keepAlive: 10_000,
    retryStrategy(times) {
      if (times > 20) {
        logger.error("Redis retry limit exceeded — giving up", { times });
        return null;
      }
      const delay = Math.min(times * 300, 5_000);
      logger.warn("Redis retry scheduled", { times, delayMs: delay });
      return delay;
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
    rateLimiterDurationMs: toNumber(process.env.QUEUE_RATE_LIMIT_DURATION_MS, 60_000),
    stalledInterval: toNumber(process.env.QUEUE_STALLED_INTERVAL_MS, 30_000),
    maxStalledCount: toNumber(process.env.QUEUE_MAX_STALLED_COUNT, 1),
  },
  worker: {
    id:
      process.env.WORKER_ID ||
      `${os.hostname()}-${process.pid}`,
    concurrency: toNumber(process.env.WORKER_CONCURRENCY, 4),
    heartbeatIntervalMs: toNumber(process.env.WORKER_HEARTBEAT_MS, 15_000),
    offlineTimeoutMs: toNumber(process.env.WORKER_OFFLINE_TIMEOUT_MS, 90_000),
    sweepIntervalMs: toNumber(process.env.WORKER_SWEEP_INTERVAL_MS, 30_000),
  },
  logging: {
    level: process.env.LOG_LEVEL || (IS_PROD ? "info" : "debug"),
  },
  cors: {
    origin: process.env.CORS_ORIGIN
      ? process.env.CORS_ORIGIN.split(",").map((s) => s.trim())
      : "*",
  },
};
if (!config.supabase.url || !config.supabase.serviceRoleKey) {
  throw new Error(
    "Missing required env: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set"
  );
}
/* =========================
   LOGGER
========================= */
const LOG_LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const currentLogLevel = LOG_LEVELS[config.logging.level] ?? 1;
function writeLog(level, message, meta = {}) {
  if ((LOG_LEVELS[level] ?? 0) < currentLogLevel) return;
  const payload = {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...meta,
  };
  const line = JSON.stringify(payload);
  if (level === "error" || level === "warn") {
    process.stderr.write(line + "\n");
  } else {
    process.stdout.write(line + "\n");
  }
}
const logger = {
  debug: (msg, meta = {}) => writeLog("debug", msg, meta),
  info:  (msg, meta = {}) => writeLog("info",  msg, meta),
  warn:  (msg, meta = {}) => writeLog("warn",  msg, meta),
  error: (msg, meta = {}) => writeLog("error", msg, meta),
};
/* =========================
   CUSTOM ERRORS
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
   SYSTEM HELPERS
========================= */
function getMemoryStats() {
  const m = process.memoryUsage();
  return {
    rss: m.rss,
    heapTotal: m.heapTotal,
    heapUsed: m.heapUsed,
    external: m.external,
    arrayBuffers: m.arrayBuffers,
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
function buildRedisOpts() {
  return {
    host: config.redis.host,
    port: config.redis.port,
    password: config.redis.password,
    username: config.redis.username,
    db: config.redis.db,
    family: config.redis.family,
    maxRetriesPerRequest: config.redis.maxRetriesPerRequest,
    enableReadyCheck: config.redis.enableReadyCheck,
    lazyConnect: config.redis.lazyConnect,
    connectTimeout: config.redis.connectTimeout,
    keepAlive: config.redis.keepAlive,
    retryStrategy: config.redis.retryStrategy,
    ...(config.redis.tls ? { tls: config.redis.tls } : {}),
  };
}
function createRedisConnection(label = "default") {
  const opts = buildRedisOpts();
  const conn = new IORedis(opts);
  conn.__label = label;
  conn.on("connect",     () => logger.info("Redis connected",     { connection: label }));
  conn.on("ready",       () => logger.info("Redis ready",         { connection: label }));
  conn.on("error",  (err) => logger.error("Redis connection error", { connection: label, error: err.message }));
  conn.on("close",       () => logger.warn("Redis connection closed",  { connection: label }));
  conn.on("reconnecting",() => logger.info("Redis reconnecting",  { connection: label }));
  conn.on("end",         () => logger.warn("Redis connection ended (no more retries)", { connection: label }));
  return conn;
}
const queueConnection  = createRedisConnection("queue");
const workerConnection = createRedisConnection("worker");
const eventsConnection = createRedisConnection("events");
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
   DB REPOSITORIES
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
  if (filter.status) query = query.eq("status", filter.status);
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
    .update({ status: "offline", updated_at: new Date().toISOString() })
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
  } catch (err) {
    logger.warn("Failed to insert queue event log", { event, jobId, error: err.message });
  }
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
  } catch (err) {
    logger.warn("Failed to update job status on completed event", { jobId, error: err.message });
  }
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
  } catch (err) {
    logger.warn("Failed to record job failure in DB", { jobId, error: err.message });
  }
});
queueEvents.on("stalled",  ({ jobId })       => safeQueueLog("stalled",  { jobId }));
queueEvents.on("progress", ({ jobId, data })  => safeQueueLog("progress", { jobId, data }));
queueEvents.on("drained",  ()                 => safeQueueLog("drained"));
queueEvents.on("waiting",  ({ jobId })        => safeQueueLog("waiting",  { jobId }));
queueEvents.on("active",   ({ jobId, prev })  => safeQueueLog("active",   { jobId, prev }));
queueEvents.on("removed",  ({ jobId })        => safeQueueLog("removed",  { jobId }));
queueEvents.on("cleaned",  (payload)          => safeQueueLog("cleaned",  payload));
queueEvents.on("error", (err) => {
  logger.error("QueueEvents error", { error: err.message, stack: err.stack });
});
/* =========================
   BUYHATKE SCRAPER IMPORT
========================= */
const { processBuyhatkeJob } = require("./worker");
/* =========================
   LIVE STREAMING (SOCKET.IO)
========================= */
/** @type {import("socket.io").Server | null} */
let io = null;
function emitToJob(jobId, event, payload) {
  if (!io || !jobId) return;
  try {
    io.to(String(jobId)).emit(event, { jobId, ...payload, ts: Date.now() });
  } catch (err) {
    logger.warn("Socket emit failed", { jobId, event, error: err.message });
  }
}
function pushProductLive(jobId, product) {
  emitToJob(jobId, "product", { product });
}
/* =========================
   WORKER MANAGEMENT
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
  logger.info("Worker registered", { workerId, hostname: system.hostname, queueName, status });
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
  for (const w of stale) {
    logger.warn("Worker marked offline (stale heartbeat)", {
      workerId: w.worker_id,
      hostname: w.hostname,
      lastHeartbeat: w.last_heartbeat,
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
  logger.info("Job queued", { jobId, query: trimmedQuery, priority, delay });
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
  logger.info("Queue paused", { queueName: config.queue.name });
  return { success: true, paused: true };
}
async function resumeQueue() {
  await searchQueue.resume();
  logger.info("Queue resumed", { queueName: config.queue.name });
  return { success: true, paused: false };
}
/* =========================
   METRICS & HEALTH
========================= */
async function getQueueMetrics() {
  const counts = await searchQueue.getJobCounts(
    "waiting", "active", "completed", "failed", "delayed", "prioritized", "paused"
  );
  const waiting = await searchQueue.getWaiting(0, 20);
  const active  = await searchQueue.getActive(0, 20);
  const queueLatency = waiting.length > 0 ? Date.now() - waiting[0].timestamp : null;
  return {
    queueSize:
      (counts.waiting || 0) +
      (counts.active  || 0) +
      (counts.delayed || 0) +
      (counts.prioritized || 0),
    counts,
    queueLatencyMs: queueLatency,
    activeSample: active.length,
  };
}
async function getRedisLatency(conn) {
  const t = Date.now();
  await conn.ping();
  return Date.now() - t;
}
async function getMetrics(conn) {
  const queue = await getQueueMetrics();
  const redisLatencyMs = await getRedisLatency(conn);
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
async function getRedisHealth(conn) {
  try {
    const pong = await conn.ping();
    return { status: pong === "PONG" ? "up" : "degraded" };
  } catch (err) {
    return { status: "down", error: err.message };
  }
}
async function getSupabaseHealth() {
  try {
    const { error } = await supabase
      .from("workers")
      .select("worker_id", { head: true, count: "exact" });
    if (error) return { status: "down", error: error.message };
    return { status: "up" };
  } catch (err) {
    return { status: "down", error: err.message };
  }
}
async function getServerHealth() {
  const queue = await searchQueue.getJobCounts(
    "waiting", "active", "completed", "failed", "delayed", "prioritized"
  );
  return {
    status: "up",
    uptimeSeconds: Math.floor(process.uptime()),
    hostname: os.hostname(),
    pid: process.pid,
    memory: process.memoryUsage(),
    cpu: process.cpuUsage(),
    cores: os.cpus().length,
    loadAvg: os.loadavg(),
    queue,
  };
}
async function getHealthSnapshot(conn) {
  const [server, redis, supabaseHealth] = await Promise.all([
    getServerHealth(),
    getRedisHealth(conn),
    getSupabaseHealth(),
  ]);
  return { success: true, server, redis, supabase: supabaseHealth };
}
/* =========================
   BULLMQ WORKER PROCESSOR
========================= */
let currentJobId    = null;
let jobsProcessing  = 0;
let heartbeatTimer  = null;
let sweepTimer      = null;
let httpServer      = null;
let bullWorker      = null;
async function updateJobProgress(job, patch = {}) {
  const progress = patch.progress ?? 0;
  // BullMQ internal progress tracking
  await job.updateProgress(progress);
  const dbPatch = {
    progress,
    updated_at: new Date().toISOString(),
  };
  if (patch.status             !== undefined) dbPatch.status                    = patch.status;
  if (patch.current_url        !== undefined) dbPatch.current_url               = patch.current_url;
  if (patch.current_product    !== undefined) dbPatch.current_product           = patch.current_product;
  if (patch.current_store      !== undefined) dbPatch.current_store             = patch.current_store;
  if (patch.products_scraped   !== undefined) dbPatch.products_scraped          = patch.products_scraped;
  if (patch.products_saved     !== undefined) dbPatch.products_saved            = patch.products_saved;
  if (patch.offers_found       !== undefined) dbPatch.offers_found              = patch.offers_found;
  if (patch.errors             !== undefined) dbPatch.errors                    = patch.errors;
  if (patch.estimated_remaining_time !== undefined) {
    dbPatch.estimated_remaining_time = patch.estimated_remaining_time;
  }
  emitToJob(job.id, "progress", { ...dbPatch });
  // Non-critical — don't let DB failures kill the job
  await updateJobStatus(job.id, dbPatch).catch((err) => {
    logger.warn("updateJobProgress DB write failed", { jobId: job.id, error: err.message });
  });
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
  } catch (err) {
    logger.warn("appendJobLog DB write failed", { jobId, error: err.message });
  }
}
async function workerProcessor(job) {
  currentJobId = job.id;
  jobsProcessing += 1;
  logger.info("Job picked up by worker", {
    workerId: config.worker.id,
    jobId: job.id,
    query: job.data?.query,
    attempt: job.attemptsMade + 1,
  });
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
        updateJobProgress: (patch) => updateJobProgress(job, patch),
        appendJobLog:      (level, message, extra = {}) => appendJobLog(job.id, level, message, extra),
        pushProductLive:   (product) => pushProductLive(job.id, product),
        cacheProduct:      async () => {},
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
    logger.info("Job completed", {
      workerId: config.worker.id,
      jobId: job.id,
      productsSaved,
      durationMs: result?.durationMs,
    });
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
    currentJobId   = null;
    await markWorkerStatus(config.worker.id, "online", {
      current_job: null,
      jobs_processing: jobsProcessing,
    }).catch((err) => {
      logger.warn("Failed to clear worker current_job after completion", { error: err.message });
    });
  }
}
function createBullWorker() {
  const w = new Worker(config.queue.name, workerProcessor, {
    connection: workerConnection,
    concurrency: config.worker.concurrency,
    limiter: {
      max: config.queue.rateLimiterMax,
      duration: config.queue.rateLimiterDurationMs,
    },
    stalledInterval: config.queue.stalledInterval,
    maxStalledCount: config.queue.maxStalledCount,
  });
  w.on("ready", () =>
    logger.info("BullMQ worker ready", {
      workerId: config.worker.id,
      queueName: config.queue.name,
      concurrency: config.worker.concurrency,
    })
  );
  w.on("error", (err) =>
    logger.error("BullMQ worker error", {
      workerId: config.worker.id,
      error: err.message,
      stack: err.stack,
    })
  );
  w.on("stalled", (jobId) =>
    logger.warn("BullMQ job stalled", { workerId: config.worker.id, jobId })
  );
  w.on("failed", (job, err) =>
    logger.error("BullMQ job permanently failed", {
      workerId: config.worker.id,
      jobId: job?.id,
      query: job?.data?.query,
      error: err.message,
    })
  );
  return w;
}
/* =========================
   EXPRESS APP
========================= */
const app = express();
app.set("trust proxy", 1);
app.use(cors({
  origin: config.cors.origin,
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
}));
app.use(express.json({ limit: "1mb" }));
// Minimal security headers
app.use((_req, res, next) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("X-XSS-Protection", "1; mode=block");
  next();
});
// Request logging (info level only)
app.use((req, _res, next) => {
  logger.info("HTTP request", { method: req.method, path: req.path, ip: req.ip });
  next();
});
/* ── Routes ── */
app.get("/", (_req, res) =>
  res.json({
    success: true,
    name: config.app.name,
    version: config.app.version,
    env: config.app.env,
  })
);
app.post("/jobs", async (req, res, next) => {
  try {
    const { query, priority = 0, delay = 0 } = req.body || {};
    const result = await createSearchJob({ query, priority, delay });
    res.status(201).json(result);
  } catch (err) {
    next(err);
  }
});
app.get("/jobs/:jobId", async (req, res, next) => {
  try {
    res.json({ success: true, ...(await getJobStatus(req.params.jobId)) });
  } catch (err) {
    next(err);
  }
});
app.get("/jobs/:jobId/products", async (req, res, next) => {
  try {
    const products = await getProductsByJobId(req.params.jobId);
    res.json({ success: true, count: products.length, products });
  } catch (err) {
    next(err);
  }
});
app.get("/jobs/completed/list", async (req, res, next) => {
  try {
    const limit = toNumber(req.query.limit, 50);
    const jobs = await getCompletedJobs(limit);
    res.json({ success: true, count: jobs.length, jobs });
  } catch (err) {
    next(err);
  }
});
app.get("/jobs/failed/list", async (req, res, next) => {
  try {
    const limit = toNumber(req.query.limit, 50);
    const jobs = await getFailedJobs(limit);
    res.json({ success: true, count: jobs.length, jobs });
  } catch (err) {
    next(err);
  }
});
app.get("/queue", async (req, res, next) => {
  try {
    const snapshot = await getQueueSnapshot();
    res.json({ success: true, queueName: config.queue.name, snapshot });
  } catch (err) {
    next(err);
  }
});
app.post("/queue/pause", async (req, res, next) => {
  try {
    res.json(await pauseQueue());
  } catch (err) {
    next(err);
  }
});
app.post("/queue/resume", async (req, res, next) => {
  try {
    res.json(await resumeQueue());
  } catch (err) {
    next(err);
  }
});
app.get("/workers", async (req, res, next) => {
  try {
    res.json(await listWorkers());
  } catch (err) {
    next(err);
  }
});
app.get("/metrics", async (req, res, next) => {
  try {
    res.json({ success: true, metrics: await getMetrics(queueConnection) });
  } catch (err) {
    next(err);
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
  } catch (err) {
    next(err);
  }
});
/* ── Global Error Handler ── */
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, _next) => {
  const statusCode = err.statusCode || 500;
  const code = err.code || "INTERNAL_SERVER_ERROR";
  logger.error("Request error", {
    path: req.path,
    method: req.method,
    error: err.message,
    code,
    ...(IS_PROD ? {} : { stack: err.stack }),
  });
  res.status(statusCode).json({
    success: false,
    error: {
      code,
      message: err.message || "Internal server error",
      details: IS_PROD ? null : (err.details || null),
    },
  });
});
/* =========================
   GRACEFUL SHUTDOWN
========================= */
function registerShutdown() {
  let shuttingDown = false;
  async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.warn("Shutdown signal received", { signal });
    try {
      if (heartbeatTimer) clearInterval(heartbeatTimer);
      if (sweepTimer)     clearInterval(sweepTimer);
      await markWorkerStatus(config.worker.id, "offline", {
        current_job: null,
        jobs_processing: 0,
      }).catch(() => {});
      // Close Socket.IO / HTTP server
      if (io) {
        await new Promise((resolve) => io.close(resolve));
      } else if (httpServer) {
        await new Promise((resolve, reject) =>
          httpServer.close((err) => (err ? reject(err) : resolve()))
        );
      }
      if (bullWorker) await bullWorker.close();
      await queueEvents.close().catch(() => {});
      await searchQueue.close().catch(() => {});
      await Promise.allSettled([
        queueConnection.quit(),
        workerConnection.quit(),
        eventsConnection.quit(),
      ]);
      logger.info("Graceful shutdown complete", { signal });
      process.exit(0);
    } catch (err) {
      logger.error("Graceful shutdown failed", { signal, error: err.message });
      process.exit(1);
    }
  }
  process.on("SIGINT",  () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("unhandledRejection", (reason) => {
    logger.error("Unhandled promise rejection", {
      error: reason instanceof Error ? reason.message : String(reason),
      stack: reason instanceof Error ? reason.stack  : undefined,
    });
  });
  process.on("uncaughtException", (err) => {
    logger.error("Uncaught exception — terminating", {
      error: err.message,
      stack: err.stack,
    });
    process.exit(1);
  });
}
/* =========================
   BOOTSTRAP
========================= */
async function start() {
  logger.info("Starting application", {
    app: config.app.name,
    version: config.app.version,
    env: config.app.env,
    workerId: config.worker.id,
    redis: {
      host: config.redis.host,
      port: config.redis.port,
      tls: !!config.redis.tls,
    },
  });
  await registerWorker(config.worker.id, config.queue.name, "online");
  bullWorker = createBullWorker();
  // Heartbeat — keep worker row fresh in DB
  heartbeatTimer = setInterval(async () => {
    try {
      await sendHeartbeat(config.worker.id, {
        jobsProcessing,
        currentJob: currentJobId,
        queueName: config.queue.name,
        status: "online",
      });
    } catch (err) {
      logger.error("Worker heartbeat failed", {
        workerId: config.worker.id,
        error: err.message,
      });
    }
  }, config.worker.heartbeatIntervalMs);
  // Offline sweep — detect crashed workers
  sweepTimer = setInterval(async () => {
    try {
      const swept = await sweepOfflineWorkers();
      if (swept > 0) {
        logger.info("Offline worker sweep completed", { sweptCount: swept });
      }
    } catch (err) {
      logger.error("Offline worker sweep failed", { error: err.message });
    }
  }, config.worker.sweepIntervalMs);
  // HTTP + Socket.IO
  httpServer = http.createServer(app);
  io = new SocketIOServer(httpServer, {
    cors: {
      origin: config.cors.origin,
      methods: ["GET", "POST"],
    },
    transports: ["websocket", "polling"],
    pingTimeout: 20_000,
    pingInterval: 10_000,
  });
  io.on("connection", (socket) => {
    logger.debug("Socket connected", { socketId: socket.id });
    socket.on("join", (jobId) => {
      if (typeof jobId === "string" && jobId.length > 0) {
        socket.join(jobId);
        logger.debug("Socket joined job room", { socketId: socket.id, jobId });
      }
    });
    socket.on("error", (err) => {
      logger.warn("Socket error", { socketId: socket.id, error: err.message });
    });
    socket.on("disconnect", (reason) => {
      logger.debug("Socket disconnected", { socketId: socket.id, reason });
    });
  });
  await new Promise((resolve, reject) => {
    httpServer.listen(config.app.port, (err) => {
      if (err) return reject(err);
      resolve();
    });
  });
  logger.info("Server listening", {
    port: config.app.port,
    env: config.app.env,
    workerId: config.worker.id,
  });
}
registerShutdown();
start().catch((err) => {
  logger.error("Application failed to start", { error: err.message, stack: err.stack });
  process.exit(1);
});
