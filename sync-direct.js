// sync-direct.js
require('dotenv').config();
const { MongoClient } = require('mongodb');
const WebSocket = require('ws');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017';
const DB_NAME = process.env.DB_NAME || 'telerid_db';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'stations';

console.log('🚀 กำลังเชื่อมต่อ TeleRID WebSocket ในเครื่องไทย...');
const ws = new WebSocket('wss://telerid.rid.go.th/ws/public/');

function cleanNumber(val) {
  if (val === null || val === undefined || val === '' || val === '-' || val === '--') return null;
  const num = parseFloat(val);
  return isNaN(num) ? null : num;
}

ws.on('open', () => console.log('📡 เชื่อมต่อ WebSocket สำเร็จ! รอรับข้อมูล...'));

ws.on('message', async (data) => {
  try {
    const parsed = JSON.parse(data.toString());
    let payload = parsed;
    if (typeof parsed.message === 'string') {
      try { payload = JSON.parse(parsed.message); } catch (e) { payload = parsed; }
    }

    if (payload.type === 'INIT' && payload.data) {
      const dict = payload.data;
      const keys = Object.keys(dict);
      console.log(`📥 ได้รับข้อมูลแล้ว ${keys.length} สถานี กำลังบันทึกตรงเข้า MongoDB Atlas...`);

      const client = new MongoClient(MONGO_URI);
      await client.connect();
      const db = client.db(DB_NAME);
      const collection = db.collection(COLLECTION_NAME);

      const ops = [];
      for (const k of keys) {
        const item = dict[k];
        if (!item || !item.code) continue;

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
        const onlineStatus = (item.is_display !== false && hasData) ? 'ออนไลน์' : 'ออฟไลน์';

        const wlCrit = cleanNumber(item.water_level_critical);
        const wlWarn = cleanNumber(item.water_level_warning);
        let alertStatus = 'ปกติ';
        if (wlUpRtk !== null) {
          if (wlCrit !== null && wlUpRtk >= wlCrit) alertStatus = 'วิกฤติ';
          else if (wlWarn !== null && wlUpRtk >= wlWarn) alertStatus = 'เตือนภัย';
        }

        ops.push({
          updateOne: {
            filter: { stationCode: item.code.trim() },
            update: {
              $set: {
                stationCode: item.code.trim(),
                stationName: item.name ? item.name.trim() : item.code,
                basin: item.basin?.name || item.project?.name || 'ไม่ระบุ',
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
        });
      }

      if (ops.length > 0) {
        await collection.bulkWrite(ops);
        console.log(`🎉 [สำเร็จ 100%] บันทึกข้อมูลละเอียด 921 สถานีเข้า MongoDB Atlas เรียบร้อย!`);
      }

      await client.close();
      ws.close();
      process.exit(0);
    }
  } catch (err) {
    console.error('❌ ผิดพลาด:', err);
  }
});
