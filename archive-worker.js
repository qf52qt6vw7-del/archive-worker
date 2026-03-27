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

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function claimBatch() {
  const { data, error } = await supabase.rpc("claim_send_queue_archive_batch", {
    p_limit: BATCH_LIMIT,
  });

  if (error) throw new Error(`claim_send_queue_archive_batch failed: ${error.message}`);
  return data || [];
}

async function archiveOne(queueId) {
  const { error } = await supabase.rpc("archive_send_queue_row", {
    p_queue_id: queueId,
  });

  if (error) {
    throw new Error(`archive_send_queue_row failed for ${queueId}: ${error.message}`);
  }
}

async function loop() {
  while (true) {
    try {
      const rows = await claimBatch();

      if (!rows.length) {
        await sleep(LOOP_SLEEP_MS);
        continue;
      }

      for (const row of rows) {
        try {
          await archiveOne(row.queue_id);
        } catch (err) {
          console.error("ARCHIVE_ROW_ERROR", {
            queueId: row.queue_id,
            error: String(err?.message || err),
          });
        }
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
