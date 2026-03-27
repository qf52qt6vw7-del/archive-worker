import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const BATCH_LIMIT = Number(process.env.ARCHIVE_BATCH_LIMIT || 100);
const LOOP_SLEEP_MS = Number(process.env.ARCHIVE_LOOP_SLEEP_MS || 2000);
const CONCURRENCY = Number(process.env.ARCHIVE_CONCURRENCY || 10);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function claimBatch() {
  const { data, error } = await supabase.rpc("claim_send_queue_archive_batch", {
    p_limit: BATCH_LIMIT,
  });

  if (error) throw new Error(`claim_send_queue_archive_batch failed: ${error.message}`);
  return data || [];
}

async function resetStale() {
  const { data, error } = await supabase.rpc("reset_stale_archive_jobs", {
    p_stale_minutes: Number(process.env.ARCHIVE_STALE_MINUTES || 10),
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

// ✅ PARALLEL PROCESSOR (OUTSIDE LOOP)
async function processBatch(rows) {
  let index = 0;

  async function worker() {
    while (index < rows.length) {
      const currentIndex = index++;
      const row = rows[currentIndex];

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

  const workers = Array.from({ length: CONCURRENCY }, () => worker());
  await Promise.all(workers);
}

async function loop() {
  while (true) {
    try {
      await resetStale();

      const rows = await claimBatch();

      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      // ✅ THIS WAS MISSING
      await processBatch(rows);

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
