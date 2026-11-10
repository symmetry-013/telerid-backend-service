require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');
const WebSocket = require('ws');
const schedule = require('node-schedule');

const app = express();

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

function cleanNumber(val) {
  if (val === null || val === undefined || val === '' || val === '-' || val === '--') return null;
  const num = parseFloat(val);
  if (isNaN(num) || num === -999 || num === 999) return null; 
  return num;
}

async function initDatabase() {
  try {
    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    await db.collection(COLLECTION_NAME).createIndex({ stationCode: 1 }, { unique: true });
    console.log(`✅ [MongoDB] เชื่อมต่อสำเร็จ: ฐานข้อมูล "${DB_NAME}"`);

    syncTelemetryData().catch(err => console.warn('⚠️ คำเตือนการซิงค์รอบแรก:', err.message));
  } catch (error) {
    console.error('❌ [MongoDB Error]:', error.message);
  }
}

initDatabase();

function syncTelemetryData() {
  return new Promise((resolve, reject) => {
    console.log(`\n⏰ [${new Date().toLocaleString('th-TH')}] เริ่มกระบวนการดึงข้อมูลสดจาก TeleRID WebSocket...`);

    const WS_URL = 'wss://telerid.rid.go.th/ws/public/';
    const ws = new WebSocket(WS_URL, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows)',
        'Origin': 'https://rid.go.th'
      }
    });

    let isResolved = false;
    const timeoutId = setTimeout(() => {
      if (!isResolved) {
        ws.terminate();
        reject(new Error('WebSocket Connection Timeout'));
      }
    }, 30000);

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

            // 🟢 แก้ไข 1: ตรรกะเช็คสถานะออนไลน์ให้ถูกต้อง (เช็คจากการอนุญาตแสดงผลและเซ็นเซอร์)
            let isOnline = true;
            if (item.hasOwnProperty('is_online')) {
              isOnline = item.is_online;
            } else {
              isOnline = item.is_display !== false && (item.measure?.wl || item.measure?.r);
            }
            const onlineStatus = isOnline ? 'ออนไลน์' : 'ออฟไลน์';

            const wlCrit = cleanNumber(item.water_level_critical);
            const wlWarn = cleanNumber(item.water_level_warning);
            let alertStatus = 'ปกติ';
            if (wlUpRtk !== null) {
              if (wlCrit !== null && wlUpRtk >= wlCrit) alertStatus = 'วิกฤติ';
              else if (wlWarn !== null && wlUpRtk >= wlWarn) alertStatus = 'เตือนภัย';
            }

            // 🟢 แก้ไข 2: ดึงเวลาข้อมูลจริงจากเครื่อง SV
            let stationTime = item.datetime || item.values?.water_level?.datetime || item.values?.rain_sum_now?.datetime;
            // หากระบบใช้ unixtime ให้แปลงเป็นแบบอ่านได้
            if (!stationTime && item.values?.water_level?.unixtime) {
              // x1000 เพื่อแปลงวินาทีเป็นมิลลิวินาที
              stationTime = new Date(item.values.water_level.unixtime * 1000).toLocaleString('th-TH'); 
            }

            return {
              updateOne: {
                filter: { stationCode: code },
                update: {
                  $set: {
                    stationCode: code,
                    stationName: name,
                    basin: basinName,
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
                    stationDatetime: stationTime || '-', // บันทึกเวลาข้อมูลจริง
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
            console.log(`🎉 BulkWrite เรียบร้อย: ${result.upsertedCount + result.modifiedCount} รายการ`);
            resolve(bulkOps.length);
          } else {
            resolve(0);
          }
        }
      } catch (err) {
        if (!isResolved) {
          clearTimeout(timeoutId);
          ws.terminate();
          reject(err);
        }
      }
    });

    ws.on('error', (err) => {
      if (!isResolved) { clearTimeout(timeoutId); reject(err); }
    });
    ws.on('close', () => {
      if (!isResolved) { clearTimeout(timeoutId); reject(new Error('Connection closed')); }
    });
  });
}

// ตั้งเวลารันอัตโนมัติทุกๆ 15 นาที
schedule.scheduleJob('*/15 * * * *', async () => {
  try { await syncTelemetryData(); } catch (error) {}
});

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
    res.json({ ok: true, summary: { total, online, offline, normal, warning, critical } });
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

app.listen(PORT, () => { console.log(`🚀 [API Server] ทำงานที่ Port ${PORT}`); });