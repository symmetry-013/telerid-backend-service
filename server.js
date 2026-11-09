require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { MongoClient } = require('mongodb');

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

// เชื่อมต่อฐานข้อมูล MongoDB
async function initDatabase() {
  try {
    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
    
    // สร้าง Index เพื่อให้ค้นหาด้วยรหัสสถานีได้รวดเร็ว
    await db.collection(COLLECTION_NAME).createIndex({ stationCode: 1 }, { unique: true });
    console.log(`✅ [MongoDB] เชื่อมต่อสำเร็จ: ฐานข้อมูล "${DB_NAME}" คอลเลกชัน "${COLLECTION_NAME}"`);
  } catch (error) {
    console.error('❌ [MongoDB Error]:', error.message);
  }
}

initDatabase();

// --- REST API Endpoints ---

// 1. ตรวจสอบสถานะเซิร์ฟเวอร์
app.get('/api/health', (req, res) => {
  res.json({ ok: true, status: 'Server running', timestamp: new Date() });
});

// 2. ดึงข้อมูลสถานีทั้งหมด (รองรับ Query: ?basin=ชื่อลุ่มน้ำ หรือ ?status=วิกฤติ)
app.get('/api/stations', async (req, res) => {
  try {
    const { basin, status } = req.query;
    const filter = {};

    if (basin) filter.basin = basin;
    if (status) filter.status = status;

    const stations = await db
      .collection(COLLECTION_NAME)
      .find(filter)
      .sort({ updatedAt: -1 })
      .toArray();

    res.json({
      ok: true,
      count: stations.length,
      data: stations
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// 3. ดึงสรุปยอดสถานีและเฉพาะสถานีวิกฤติ/เตือนภัย
app.get('/api/stations/summary', async (req, res) => {
  try {
    const total = await db.collection(COLLECTION_NAME).countDocuments();
    const critical = await db.collection(COLLECTION_NAME).countDocuments({ 
      status: { $in: ['วิกฤติ', 'CRITICAL'] } 
    });
    const warning = await db.collection(COLLECTION_NAME).countDocuments({ 
      status: { $in: ['เตือนภัย', 'WARNING'] } 
    });
    const normal = await db.collection(COLLECTION_NAME).countDocuments({ 
      status: { $in: ['ปกติ', 'NORMAL'] } 
    });

    const criticalList = await db.collection(COLLECTION_NAME)
      .find({ status: { $in: ['วิกฤติ', 'CRITICAL'] } })
      .toArray();

    res.json({
      ok: true,
      summary: {
        total,
        normal,
        warning,
        critical
      },
      criticalStations: criticalList
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// 4. Endpoint สำหรับบันทึก/อัปเดตข้อมูลสถานีรายตัว (Upsert)
app.post('/api/stations', async (req, res) => {
  try {
    const item = req.body;
    if (!item.stationCode) {
      return res.status(400).json({ ok: false, error: 'จำเป็นต้องระบุ stationCode' });
    }

    const result = await db.collection(COLLECTION_NAME).updateOne(
      { stationCode: item.stationCode },
      {
        $set: {
          ...item,
          updatedAt: new Date()
        }
      },
      { upsert: true }
    );

    res.json({ ok: true, result });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// รันเซิร์ฟเวอร์
app.listen(PORT, () => {
  console.log(`🚀 [API Server] กำลังทำงานที่ http://localhost:${PORT}`);
  console.log(`📌 ดูรายการสถานีทั้งหมด: http://localhost:${PORT}/api/stations`);
  console.log(`📌 ดูสรุปยอดสถานี: http://localhost:${PORT}/api/stations/summary`);
});
