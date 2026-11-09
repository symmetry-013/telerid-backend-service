require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const WebSocket = require('ws');
const schedule = require('node-schedule');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

const PORT = process.env.PORT || 5000;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017';
const DB_NAME = process.env.DB_NAME || 'telerid_db';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'stations';

let db;
let client;

// 1. เชื่อมต่อฐานข้อมูล MongoDB Atlas
async function initDatabase() {
  try {
    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    await db.collection(COLLECTION_NAME).createIndex({ stationCode: 1 }, { unique: true });
    console.log(`✅ [MongoDB Atlas] เชื่อมต่อสำเร็จ: ฐานข้อมูล "${DB_NAME}" คอลเลกชัน "${COLLECTION_NAME}"`);

    // รันการซิงค์ข้อมูลครั้งแรกทันทีที่เซิร์ฟเวอร์เปิดขึ้นมา
    syncTelemetryData();
  } catch (error) {
    console.error('❌ [MongoDB Error]:', error.message);
  }
}

initDatabase();

// 2. ฟังก์ชันดึงข้อมูลจาก TeleRID WebSocket และบันทึกลง MongoDB Atlas
function syncTelemetryData() {
  console.log(`\n⏰ [${new Date().toLocaleString('th-TH')}] เริ่มกระบวนการดึงข้อมูลสดจาก TeleRID WebSocket...`);

  const WS_URL = 'wss://telerid.rid.go.th/ws/public/';
  const ws = new WebSocket(WS_URL, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
  });

  ws.on('open', () => {
    console.log('📡 เชื่อมต่อท่อ TeleRID WebSocket สำเร็จ! กำลังรอรับชุดข้อมูล INIT...');
  });

  ws.on('message', async (data) => {
    try {
      const parsed = JSON.parse(data.toString());
      let payload = parsed;
      if (typeof parsed.message === 'string') {
        try { payload = JSON.parse(parsed.message); } catch (e) { payload = parsed; }
      }

      if (payload.type === 'INIT' && payload.data) {
        const stationsDict = payload.data;
        const keys = Object.keys(stationsDict);
        console.log(`📥 ได้รับข้อมูลแล้ว! ตรวจพบทั้งหมด ${keys.length} สถานี กำลังบันทึกลง Atlas...`);

        const collection = db.collection(COLLECTION_NAME);
        let savedCount = 0;

        for (const k of keys) {
          const item = stationsDict[k];
          if (!item || !item.code) continue;

          const code = item.code.trim();
          const name = item.name ? item.name.trim() : code;
          const basinName = item.basin?.name || item.project?.name || 'ไม่ระบุ';

          // ตรวจสอบสถานะออนไลน์ / ออฟไลน์
          const isOnline = item.is_display !== false && (item.measure?.wl || item.measure?.r);
          const onlineStatus = isOnline ? 'ออนไลน์' : 'ออฟไลน์';

          // ปริมาณน้ำฝน (07:00-ปัจจุบัน และ สะสม 1 วัน)
          const rain7toNow = item.values?.rain_sum_now?.value ?? 0.0;
          const rainSeries = Array.isArray(item.values?.rain_sum?.value) ? item.values.rain_sum.value : [];
          const rain1Day = rainSeries.length > 0 ? rainSeries[rainSeries.length - 1] : rain7toNow;

          // ระดับน้ำ ม.รทก. และ ม.รสม. ทั้งเหนือน้ำ (U/S) และท้ายน้ำ (D/S)
          const wlList = item.values?.water_level_value_list?.value || [];
          const parseVal = (v) => (v === '-' || v === '' || v === undefined || v === null) ? null : parseFloat(v);

          const wlUpRtk = parseVal(wlList[0]) ?? parseVal(item.values?.water_level?.value);
          const wlUpRsm = parseVal(wlList[1]);
          const wlDownRtk = parseVal(wlList[2]);
          const wlDownRsm = parseVal(wlList[3]);

          // เกณฑ์ระดับน้ำ
          const wlCrit = item.water_level_critical ?? null;
          const wlWarn = item.water_level_warning ?? null;
          let alertStatus = 'ปกติ';
          if (wlUpRtk !== null) {
            if (wlCrit !== null && wlUpRtk >= wlCrit) alertStatus = 'วิกฤติ';
            else if (wlWarn !== null && wlUpRtk >= wlWarn) alertStatus = 'เตือนภัย';
          }

          await collection.updateOne(
            { stationCode: code },
            {
              $set: {
                stationCode: code,
                stationName: name,
                basin: basinName,
                province: item.province || '',
                amphur: item.amphur || '',
                tambon: item.tambon || '',
                onlineStatus: onlineStatus,
                alertStatus: alertStatus,
                status: alertStatus,
                rain7toNow: rain7toNow,
                rain1Day: rain1Day,
                waterLevelUp_RTK: wlUpRtk,
                waterLevelUp_RSM: wlUpRsm,
                waterLevelDown_RTK: wlDownRtk,
                waterLevelDown_RSM: wlDownRsm,
                warningThreshold: wlWarn,
                criticalThreshold: wlCrit,
                location: {
                  lat: item.location?.y || null,
                  lng: item.location?.x || null
                },
                recordedAt: new Date().toLocaleString('th-TH'),
                updatedAt: new Date()
              }
            },
            { upsert: true }
          );
          savedCount++;
        }

        console.log(`🎉 [สำเร็จ] อัปเดตข้อมูลสดเข้า MongoDB Atlas เรียบร้อย: ${savedCount} สถานี!`);
        ws.close();
      }
    } catch (err) {
      console.error('❌ เกิดข้อผิดพลาดในการประมวลผลข้อมูล:', err.message);
    }
  });

  ws.on('error', (err) => {
    console.error('❌ ข้อผิดพลาด TeleRID WebSocket:', err.message);
  });
}

// 3. ตั้งเวลาให้ทำงานอัตโนมัติทุก ๆ ต้นชั่วโมง (นาทีที่ 0 ของทุกชั่วโมง เช่น 07:00, 08:00, 09:00...)
schedule.scheduleJob('0 * * * *', () => {
  syncTelemetryData();
});

// --- REST API Endpoints สำหรับหน้า Dashboard ---
app.get('/api/stations', async (req, res) => {
  try {
    const { basin, status } = req.query;
    const filter = {};
    if (basin && basin !== 'ALL') filter.basin = basin;
    if (status && status !== 'ALL') filter.status = status;

    const stations = await db.collection(COLLECTION_NAME).find(filter).sort({ stationCode: 1 }).toArray();
    res.json({ ok: true, count: stations.length, data: stations });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/stations/summary', async (req, res) => {
  try {
    const total = await db.collection(COLLECTION_NAME).countDocuments();
    const online = await db.collection(COLLECTION_NAME).countDocuments({ onlineStatus: 'ออนไลน์' });
    const offline = await db.collection(COLLECTION_NAME).countDocuments({ onlineStatus: 'ออฟไลน์' });
    const critical = await db.collection(COLLECTION_NAME).countDocuments({ status: 'วิกฤติ' });
    const warning = await db.collection(COLLECTION_NAME).countDocuments({ status: 'เตือนภัย' });
    const normal = await db.collection(COLLECTION_NAME).countDocuments({ status: 'ปกติ' });

    res.json({
      ok: true,
      summary: { total, online, offline, normal, warning, critical }
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// สตาร์ทเซิร์ฟเวอร์
app.listen(PORT, () => {
  console.log(`🚀 [API Server] กำลังทำงานที่ Port ${PORT}`);
});
