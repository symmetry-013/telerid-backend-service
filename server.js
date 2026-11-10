require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const WebSocket = require('ws');
const schedule = require('node-schedule');

const app = express();

// 1. ตั้งค่า CORS (ลบ app.options('*', cors()) ออกเพื่อป้องกัน PathError ใน Node รุ่นใหม่)
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'Accept']
}));

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static(__dirname));

const PORT = process.env.PORT || 5000;
const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017';
const DB_NAME = process.env.DB_NAME || 'telerid_db';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'stations';

let db;
let client;

// 2. ฟังก์ชันช่วยทำความสะอาดและแปลงตัวเลข
function cleanNumber(val) {
  if (val === null || val === undefined || val === '' || val === '-' || val === '--') return null;
  const num = parseFloat(val);
  if (isNaN(num) || num === -999 || num === 999) return null; 
  return num;
}

// 3. เชื่อมต่อฐานข้อมูล MongoDB
async function initDatabase() {
  try {
    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    await db.collection(COLLECTION_NAME).createIndex({ stationCode: 1 }, { unique: true });
    console.log(`✅ [MongoDB] เชื่อมต่อสำเร็จ: ฐานข้อมูล "${DB_NAME}" คอลเลกชัน "${COLLECTION_NAME}"`);

    // ลองซิงค์ข้อมูลรอบแรก (ดัก Error ไว้ไม่ให้ Server ล่มหาก Render ถูกบล็อก WebSocket)
    syncTelemetryData().catch(err => {
      console.warn('⚠️ คำเตือน: ไม่สามารถซิงค์อัตโนมัติได้ในสภาพแวดล้อมนี้ (อาจถูกบล็อกพอร์ต/Network Timeout):', err.message);
    });
  } catch (error) {
    console.error('❌ [MongoDB Error]:', error.message);
  }
}

initDatabase();

// 4. Core Sync Function
function syncTelemetryData() {
  return new Promise((resolve, reject) => {
    console.log(`\n⏰ [${new Date().toLocaleString('th-TH')}] เริ่มกระบวนการดึงข้อมูลสดจาก TeleRID WebSocket...`);

    const WS_URL = 'wss://telerid.rid.go.th/ws/public/';
    const ws = new WebSocket(WS_URL, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Origin': 'https://rid.go.th'
      }
    });

    let isResolved = false;

    const timeoutId = setTimeout(() => {
      if (!isResolved) {
        ws.terminate();
        console.error('❌ [Timeout] ไม่ได้รับการตอบกลับจาก TeleRID ภายใน 30 วินาที');
        reject(new Error('WebSocket Connection Timeout (30s)'));
      }
    }, 30000);

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
          isResolved = true;
          clearTimeout(timeoutId);
          ws.close();

          const stationsDict = payload.data;
          const keys = Object.keys(stationsDict);
          console.log(`📥 ได้รับข้อมูลแล้ว! ตรวจพบทั้งหมด ${keys.length} สถานี กำลังเตรียม BulkWrite...`);

          const bulkOps = keys.map(k => {
            const item = stationsDict[k];
            if (!item || !item.code) return null;

            const code = item.code.trim();
            const name = item.name ? item.name.trim() : code;
            const basinName = item.basin?.name || item.project?.name || 'ไม่ระบุ';

            const rainNow = cleanNumber(item.values?.rain_sum_now?.value) ?? 0.0;
            let rain1Day = rainNow;
            if (Array.isArray(item.values?.rain_sum?.value) && item.values.rain_sum.value.length > 0) {
              const arr = item.values.rain_sum.value;
              rain1Day = cleanNumber(arr[arr.length - 1]) ?? rainNow;
            }

            const valList = item.values?.water_level_value_list?.value || [];
            const wlUpRtk = cleanNumber(valList[0]) ?? cleanNumber(item.values?.water_level?.value);
            const wlUpRsm = cleanNumber(valList[1]);
            const wlDownRtk = cleanNumber(valList[2]);
            const wlDownRsm = cleanNumber(valList[3]);

            const hasData = (wlUpRtk !== null) || (rainNow > 0) || (item.values?.water_level?.unixtime > 0);
            const isOnline = item.is_display !== false && hasData;
            const onlineStatus = isOnline ? 'ออนไลน์' : 'ออฟไลน์';

            const wlCrit = cleanNumber(item.water_level_critical);
            const wlWarn = cleanNumber(item.water_level_warning);
            let alertStatus = 'ปกติ';
            if (wlUpRtk !== null) {
              if (wlCrit !== null && wlUpRtk >= wlCrit) alertStatus = 'วิกฤติ';
              else if (wlWarn !== null && wlUpRtk >= wlWarn) alertStatus = 'เตือนภัย';
            }

            return {
              updateOne: {
                filter: { stationCode: code },
                update: {
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
                    rain7toNow: rainNow,
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
                upsert: true
              }
            };
          }).filter(op => op !== null);

          if (bulkOps.length > 0) {
            const collection = db.collection(COLLECTION_NAME);
            const result = await collection.bulkWrite(bulkOps);
            console.log(`🎉 [สำเร็จ] BulkWrite เรียบร้อย: อัปเดต/เพิ่มใหม่รวม ${result.upsertedCount + result.modifiedCount} รายการ`);
            resolve(bulkOps.length);
          } else {
            resolve(0);
          }
        }
      } catch (err) {
        console.error('❌ เกิดข้อผิดพลาดในการประมวลผลข้อมูล:', err.message);
        if (!isResolved) {
          clearTimeout(timeoutId);
          ws.terminate();
          reject(err);
        }
      }
    });

    ws.on('error', (err) => {
      console.error('❌ ข้อผิดพลาด TeleRID WebSocket:', err.message);
      if (!isResolved) {
        clearTimeout(timeoutId);
        reject(err);
      }
    });

    ws.on('close', () => {
      if (!isResolved) {
        console.warn('⚠️ WebSocket ถูกตัดการเชื่อมต่อก่อนได้รับข้อมูล INIT');
        clearTimeout(timeoutId);
        reject(new Error('Connection closed prematurely'));
      }
    });
  });
}

// 5. ตั้งเวลาดึงข้อมูลอัตโนมัติทุก ๆ ต้นชั่วโมง
schedule.scheduleJob('0 * * * *', async () => {
  try {
    await syncTelemetryData();
  } catch (error) {
    console.error('❌ Scheduled Sync Failed:', error.message);
  }
});

// --- REST API Endpoints ---
app.get('/api/stations', async (req, res) => {
  try {
    const { basin, status } = req.query;
    const filter = {};
    if (basin && basin !== 'ALL') filter.basin = basin;
    if (status && status !== 'ALL') {
      filter.$or = [{ status: status }, { alertStatus: status }];
    }

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
    const critical = await db.collection(COLLECTION_NAME).countDocuments({ $or: [{ status: 'วิกฤติ' }, { alertStatus: 'วิกฤติ' }] });
    const warning = await db.collection(COLLECTION_NAME).countDocuments({ $or: [{ status: 'เตือนภัย' }, { alertStatus: 'เตือนภัย' }] });
    const normal = total - (critical + warning);

    res.json({
      ok: true,
      summary: { total, online, offline, normal, warning, critical }
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/sync-now', async (req, res) => {
  try {
    const count = await syncTelemetryData();
    res.json({ ok: true, message: `ซิงค์ข้อมูลสำเร็จจำนวน ${count} สถานี` });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/api/stations', async (req, res) => {
  try {
    const item = req.body;
    if (!item.stationCode) return res.status(400).json({ ok: false, message: 'ต้องระบุ stationCode' });

    await db.collection(COLLECTION_NAME).updateOne(
      { stationCode: item.stationCode },
      { $set: { ...item, updatedAt: new Date() } },
      { upsert: true }
    );
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 [API Server] กำลังทำงานที่ Port ${PORT}`);
});