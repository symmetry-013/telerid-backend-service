// fetch-all-stations.js
const { MongoClient } = require('mongodb');

const MONGO_URI = 'mongodb://127.0.0.1:27017';
const DB_NAME = 'telerid_db';
const COLLECTION_NAME = 'stations';

// ดึงข้อมูลตรงจาก API หลักของ TeleRID
const API_URL = 'https://rid.go.th';
const TOKEN = 'a14fdebb2885617855f51ed06429d798b7d4e097';

async function syncAllStations() {
  console.log('🚀 กำลังเชื่อมต่อเพื่อดึงข้อมูลสถานีทั้งหมดของ TeleRID...');

  try {
    const res = await fetch(API_URL, {
      method: 'GET',
      headers: {
        'Accept': 'application/json, text/plain, */*',
        'Authorization': `token ${TOKEN}`
      }
    });

    if (!res.ok) {
      console.error(`❌ ดึงข้อมูลไม่สำเร็จ HTTP Status: ${res.status}`);
      return;
    }

    const stations = await res.json();
    const stationList = Array.isArray(stations) ? stations : (stations.results || stations.data || []);
    console.log(`📊 ตรวจพบข้อมูลสถานีทั้งหมด: ${stationList.length} สถานี กำลังบันทึกเข้า MongoDB...`);

    const client = new MongoClient(MONGO_URI);
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    let count = 0;
    for (const item of stationList) {
      const code = item.code || item.station_code || `ST-${item.id}`;
      const name = item.name || item.station_name || code;
      const basinName = item.basin?.name || item.project?.name || 'ไม่ระบุ';
      const wlVal = typeof item.water_level === 'number' ? item.water_level : parseFloat(item.water_level || 0);

      await collection.updateOne(
        { stationCode: code },
        {
          $set: {
            stationCode: code,
            stationName: name,
            basin: basinName,
            parameter: 'WL_UP',
            value: isNaN(wlVal) ? 0 : wlVal,
            status: item.is_critical ? 'วิกฤติ' : (item.is_warning ? 'เตือนภัย' : 'ปกติ'),
            recordedAt: new Date().toLocaleString('th-TH'),
            updatedAt: new Date()
          }
        },
        { upsert: true }
      );
      count++;
    }

    console.log(`\n🎉 บันทึกสถานีทั้งหมดสำเร็จ 100%: ${count} สถานี!`);
    await client.close();
  } catch (err) {
    console.error('💥 เกิดข้อผิดพลาด:', err.message);
  }
}

syncAllStations();
