import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const BATCH_LIMIT = Number(process.env.ARCHIVE_BATCH_LIMIT || 100);
const LOOP_SLEEP_MS = Number(process.env.ARCHIVE_POLL_INTERVAL_MS || 2000);
const CONCURRENCY = Number(process.env.ARCHIVE_CONCURRENCY || 10);
const READY_CONNECTION_LIMIT = Number(process.env.ARCHIVE_READY_CONNECTION_LIMIT || 500);
const STALE_MINUTES = Number(process.env.ARCHIVE_STALE_PROCESSING_MINUTES || 10);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getReadyConnectionIds() {
  const { data, error } = await supabase.rpc("get_ready_archive_connection_ids", {
    p_limit: READY_CONNECTION_LIMIT,
  });

  if (error) {
    throw new Error(`get_ready_archive_connection_ids failed: ${error.message}`);
  }

  return (data || [])
    .map((row) => row.connection_id)
    .filter(Boolean);
}

async function claimBatchForConnection(connectionId) {
  const { data, error } = await supabase.rpc(
    "claim_send_queue_archive_batch_for_connection",
    {
      p_connection_id: connectionId,
      p_limit: BATCH_LIMIT,
    }
  );

  if (error) {
    throw new Error(
      `claim_send_queue_archive_batch_for_connection failed for ${connectionId}: ${error.message}`
    );
  }

  return data || [];
}

async function resetStale() {
  const { data, error } = await supabase.rpc("reset_stale_archive_jobs", {
    p_stale_minutes: STALE_MINUTES,
    p_limit: 500,
  });

  if (error) {
    console.error("ARCHIVE_STALE_RESET_ERROR", error.message);
    return;
  }

  if (data > 0) {
    console.log("ARCHIVE_STALE_RESET", { resetCount: data });
  }
}

async function archiveOne(queueId) {
  const { error } = await supabase.rpc("archive_send_queue_row", {
    p_queue_id: queueId,
  });

  if (error) {
    throw new Error(`archive_send_queue_row failed for ${queueId}: ${error.message}`);
  }
}

async function processRows(rows) {
  let index = 0;

  async function worker() {
    while (index < rows.length) {
      const currentIndex = index++;
      const row = rows[currentIndex];
      if (!row) return;

      try {
        await archiveOne(row.queue_id);
      } catch (err) {
        console.error("ARCHIVE_ROW_ERROR", {
          queueId: row.queue_id,
          error: String(err?.message || err),
        });
      }
    }
  }

  const workers = Array.from(
    { length: Math.max(1, Math.min(CONCURRENCY, rows.length)) },
    () => worker()
  );

  await Promise.all(workers);
}

async function processConnection(connectionId) {
  const rows = await claimBatchForConnection(connectionId);

  if (!rows.length) {
    return false;
  }

  await processRows(rows);
  return true;
}

async function loop() {
  while (true) {
    try {
      await resetStale();

      const connectionIds = await getReadyConnectionIds();

      if (!connectionIds.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      let didWork = false;

      for (const connectionId of connectionIds) {
        const worked = await processConnection(connectionId);
        if (worked) didWork = true;
      }

      if (!didWork) {
        await sleep(LOOP_SLEEP_MS);
      }
    } catch (err) {
      console.error("ARCHIVE_LOOP_ERROR", {
        error: String(err?.message || err),
      });
      await sleep(LOOP_SLEEP_MS);
    }
  }
}

loop().catch((err) => {
  console.error("ARCHIVE_FATAL", err);
  process.exit(1);
});
