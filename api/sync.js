// Redash → Supabase 예약번호 동기화 API
// 5분마다 외부 cron이 GET /api/sync 호출

export default async function handler(req, res) {
  // 간단한 인증 (cron secret)
  const secret = req.headers['x-cron-secret'] || req.query.secret;
  if (secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const REDASH_URL = process.env.REDASH_URL;
  const REDASH_API_KEY = process.env.REDASH_API_KEY;
  const REDASH_QUERY_ID = process.env.REDASH_QUERY_ID;
  const SUPABASE_URL = process.env.SUPABASE_URL;
  const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

  try {
    // 1. Redash 쿼리 실행 (최신 결과 요청, 캐시 0초)
    const redashRes = await fetch(
      `${REDASH_URL}/api/queries/${REDASH_QUERY_ID}/results?max_age=0`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Key ${REDASH_API_KEY}`,
          'Content-Type': 'application/json'
        }
      }
    );

    if (!redashRes.ok) {
      const err = await redashRes.text();
      throw new Error(`Redash error: ${redashRes.status} ${err}`);
    }

    const redashData = await redashRes.json();

    // Redash가 job을 반환하면 (아직 실행 중) 폴링
    let rows;
    if (redashData.job) {
      rows = await pollRedashJob(redashData.job.id, REDASH_URL, REDASH_API_KEY);
    } else {
      rows = redashData.query_result?.data?.rows || [];
    }

    if (rows.length === 0) {
      return res.status(200).json({ message: 'No rows from Redash', synced: 0 });
    }

    // 2. Supabase에 upsert (배치)
    const BATCH_SIZE = 500;
    let totalSynced = 0;

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE).map(r => ({
        order_id: String(r.order_id),
        gid: String(r.gid),
        user_id: r.user_id ? String(r.user_id) : null,
        basis_date: r.basis_date,
        travel_start: r.travel_start || null,
        travel_end: r.travel_end || null,
        synced_at: new Date().toISOString()
      }));

      const sbRes = await fetch(`${SUPABASE_URL}/rest/v1/valid_reservations`, {
        method: 'POST',
        headers: {
          'apikey': SUPABASE_SERVICE_KEY,
          'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
          'Content-Type': 'application/json',
          'Prefer': 'resolution=merge-duplicates'
        },
        body: JSON.stringify(batch)
      });

      if (!sbRes.ok) {
        const err = await sbRes.text();
        throw new Error(`Supabase error: ${sbRes.status} ${err}`);
      }

      totalSynced += batch.length;
    }

    return res.status(200).json({
      message: 'Sync complete',
      synced: totalSynced,
      timestamp: new Date().toISOString()
    });

  } catch (error) {
    console.error('Sync failed:', error);
    return res.status(500).json({ error: error.message });
  }
}

// Redash 쿼리 실행이 아직 안 끝났을 때 폴링
async function pollRedashJob(jobId, redashUrl, apiKey) {
  const MAX_ATTEMPTS = 30; // 최대 60초 대기
  for (let i = 0; i < MAX_ATTEMPTS; i++) {
    await new Promise(r => setTimeout(r, 2000));

    const res = await fetch(`${redashUrl}/api/jobs/${jobId}`, {
      headers: { 'Authorization': `Key ${apiKey}` }
    });
    const data = await res.json();

    if (data.job.status === 3) {
      // 완료 — query_result_id로 결과 가져오기
      const resultRes = await fetch(
        `${redashUrl}/api/query_results/${data.job.query_result_id}`,
        { headers: { 'Authorization': `Key ${apiKey}` } }
      );
      const resultData = await resultRes.json();
      return resultData.query_result?.data?.rows || [];
    }

    if (data.job.status === 4) {
      throw new Error(`Redash job failed: ${data.job.error}`);
    }
  }
  throw new Error('Redash job timeout');
}
