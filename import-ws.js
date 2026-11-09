// import-ws.js
const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017';
const DB_NAME = process.env.DB_NAME || 'telerid_db';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'stations';

const filePath = path.join(__dirname, 'telerid_ws_init.json');

async function importWsDetailedData() {
  if (!fs.existsSync(filePath)) {
    console.error('❌ ไม่พบไฟล์ telerid_ws_init.json');
    return;
  }

  console.log('📥 กำลังสกัดข้อมูลรายละเอียดโทรมาตรขั้นสูง...');
  const fileContent = fs.readFileSync(filePath, 'utf8').trim();

  let rootObj = JSON.parse(fileContent);
  let payload = rootObj;
  if (typeof rootObj.message === 'string') {
    try { payload = JSON.parse(rootObj.message); } catch (e) { payload = rootObj; }
  }

  const stationsDict = payload.data || payload;
  const stationKeys = Object.keys(stationsDict);
  console.log(`📊 ตรวจพบข้อมูลทั้งหมด: ${stationKeys.length} สถานี`);

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

      // 1. ตรวจสอบสถานะ ออนไลน์ / ออฟไลน์
      // ระบบ TeleRID: หากมี timestamp ล่าสุด หรือ measure/display ทำงาน ปกติถือว่าออนไลน์
      const isOnline = item.is_display !== false && (item.measure?.wl || item.measure?.r);
      const onlineStatus = isOnline ? 'ออนไลน์' : 'ออฟไลน์';

      // 2. ค่าน้ำฝน: 7 โมง - ปัจจุบัน และ ฝนสะสม 1 วัน (24 ชม.)
      const rain7toNow = item.values?.rain_sum_now?.value ?? 0.0;
      const rainSeries = Array.isArray(item.values?.rain_sum?.value) ? item.values.rain_sum.value : [];
      // rain_sum index ล่าสุดคือสะสม 24 ชม. (1 วัน)
      const rain1Day = rainSeries.length > 0 ? rainSeries[rainSeries.length - 1] : rain7toNow;

      // 3. ระดับน้ำ: ม.รทก. และ ม.รสม. ทั้งเหนือน้ำ (U/S) และท้ายน้ำ (D/S)
      // โครงสร้าง TeleRID ใน water_level_value_list.value: [เหนือน้ำ ม.รทก, เหนือน้ำ ม.รสม, ท้ายน้ำ ม.รทก, ท้ายน้ำ ม.รสม]
      const wlList = item.values?.water_level_value_list?.value || [];
      const parseVal = (v) => (v === '-' || v === '' || v === undefined || v === null) ? null : parseFloat(v);

      const wlUpRtk = parseVal(wlList[0]) ?? parseVal(item.values?.water_level?.value);
      const wlUpRsm = parseVal(wlList[1]);
      const wlDownRtk = parseVal(wlList[2]);
      const wlDownRsm = parseVal(wlList[3]);

      // 4. คำนวณเกณฑ์สถานะวิกฤติ/เตือนภัย
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
            onlineStatus: onlineStatus,     // ออนไลน์ / ออฟไลน์
            alertStatus: alertStatus,       // ปกติ / เตือนภัย / วิกฤติ
            rain7toNow: rain7toNow,         // น้ำฝน 07:00 - ปัจจุบัน (มม.)
            rain1Day: rain1Day,             // น้ำฝนสะสม 1 วัน (มม.)
            waterLevelUp_RTK: wlUpRtk,      // เหนือน้ำ ม.รทก.
            waterLevelUp_RSM: wlUpRsm,      // เหนือน้ำ ม.รสม.
            waterLevelDown_RTK: wlDownRtk,  // ท้ายน้ำ ม.รทก.
            waterLevelDown_RSM: wlDownRsm,  // ท้ายน้ำ ม.รสม.
            warningThreshold: wlWarn,
            criticalThreshold: wlCrit,
            location: {
              lat: item.location?.y || null,
              lng: item.location?.x || null
            },
            updatedAt: new Date()
          }
        },
        { upsert: true }
      );
      count++;
    }

    console.log(`✅ อัปเดตข้อมูลละเอียดทั้ง 921 สถานีเข้า MongoDB สำเร็จ: ${count} รายการ!`);
  } catch (err) {
    console.error('💥 เกิดข้อผิดพลาด:', err.message);
  } finally {
    await client.close();
  }
}

importWsDetailedData();
