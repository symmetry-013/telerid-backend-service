// import-ws.js
const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');

const MONGO_URI = 'mongodb://127.0.0.1:27017';
const DB_NAME = 'telerid_db';
const COLLECTION_NAME = 'stations';

const filePath = path.join(__dirname, 'telerid_ws_init.json');

async function importWsInitData() {
  if (!fs.existsSync(filePath)) {
    console.error('❌ ไม่พบไฟล์ telerid_ws_init.json');
    return;
  }

  console.log('📥 กำลังอ่านและถอดรหัสข้อมูล WebSocket INIT...');
  const fileContent = fs.readFileSync(filePath, 'utf8').trim();

  let rootObj;
  try {
    rootObj = JSON.parse(fileContent);
  } catch (e) {
    console.error('❌ รูปแบบ JSON ผิดพลาด:', e.message);
    return;
  }

  // รองรับทั้งกรณีที่เป็นข้อความครอบ string ซ้ำ ("message": "{\"type\": ...}") หรือ JSON Object ตรงๆ
  let payload = rootObj;
  if (typeof rootObj.message === 'string') {
    try {
      payload = JSON.parse(rootObj.message);
    } catch (e) {
      payload = rootObj;
    }
  }

  const stationsDict = payload.data || payload;
  const stationKeys = Object.keys(stationsDict);
  console.log(`📊 ตรวจพบสถานีในชุดข้อมูลทั้งหมด: ${stationKeys.length} สถานี`);

  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    let count = 0;
    for (const key of stationKeys) {
      const item = stationsDict[key];
      if (!item || !item.code) continue;

      const code = item.code.trim();
      const name = item.name ? item.name.trim() : code;
      const basinName = item.basin?.name || item.project?.name || 'ไม่ระบุ';
      
      // ดึงค่าน้ำปัจจุบัน
      const wlVal = typeof item.values?.water_level?.value === 'number'
        ? item.values.water_level.value
        : parseFloat(item.values?.water_level?.value || 0);

      // ดึงค่าน้ำฝนสะสมล่าสุด
      const rainNow = item.values?.rain_sum_now?.value ?? 0;
      const rainSeries = Array.isArray(item.values?.rain_sum?.value)
        ? item.values.rain_sum.value
        : [];
      const rain24h = rainSeries.length > 0 ? rainSeries[rainSeries.length - 1] : rainNow;

      // ตรวจสอบสถานะ (เทียบกับเกณฑ์วิกฤติ/เตือนภัย)
      const wlWarning = item.water_level_warning ?? null;
      const wlCritical = item.water_level_critical ?? null;

      let status = 'ปกติ';
      if (wlCritical !== null && wlVal >= wlCritical) {
        status = 'วิกฤติ';
      } else if (wlWarning !== null && wlVal >= wlWarning) {
        status = 'เตือนภัย';
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
            parameter: 'WL_UP',
            value: isNaN(wlVal) ? 0 : wlVal,
            rain24h: rain24h,
            warningThreshold: wlWarning,
            criticalThreshold: wlCritical,
            status: status,
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
      count++;
    }

    console.log(`\n🎉 บันทึกสถานีทั้งหมดเข้า MongoDB สำเร็จเรียบร้อย: ${count} สถานี!`);
    console.log('👉 เปิดดูผลลัพธ์สดได้ทันทีที่ index.html หรือกดปุ่ม "รีเฟรชข้อมูล"');
  } catch (err) {
    console.error('💥 เกิดข้อผิดพลาดขณะบันทึกเข้าฐานข้อมูล:', err.message);
  } finally {
    await client.close();
  }
}

importWsInitData();
