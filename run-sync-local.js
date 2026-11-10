require('dotenv').config();
const { MongoClient } = require('mongodb');
const WebSocket = require('ws');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017';
const DB_NAME = process.env.DB_NAME || 'telerid_db';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'stations';

function cleanNumber(val) {
  if (val === null || val === undefined || val === '' || val === '-' || val === '--') return null;
  const num = parseFloat(val);
  if (isNaN(num) || num === -999 || num === 999) return null;
  return num;
}

async function syncNow() {
  console.log('🔄 กำลังเชื่อมต่อ WebSocket เพื่อดึงข้อมูลสดจากเครื่อง Local...');
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db(DB_NAME);
  const collection = db.collection(COLLECTION_NAME);

  const ws = new WebSocket('wss://telerid.rid.go.th/ws/public/');

  ws.on('message', async (data) => {
    try {
      const parsed = JSON.parse(data.toString());
      let payload = parsed;
      if (typeof parsed.message === 'string') {
        try { payload = JSON.parse(parsed.message); } catch (e) {}
      }

      if (payload.type === 'INIT' && payload.data) {
        const stationsDict = payload.data;
        const keys = Object.keys(stationsDict);
        console.log(`📥 ดึงข้อมูลสำเร็จ ${keys.length} สถานี กำลังบันทึกลง MongoDB Atlas...`);

        const bulkOps = keys.map(k => {
          const item = stationsDict[k];
          if (!item || !item.code) return null;

          const code = item.code.trim();
          const name = item.name ? item.name.trim() : code;
          const basinName = item.basin?.name || item.project?.name || 'ไม่ระบุ';

          // ฝน
          const rainNow = cleanNumber(item.values?.rain_sum_now?.value) ?? 0.0;
          let rain1Day = rainNow;
          if (Array.isArray(item.values?.rain_sum?.value) && item.values.rain_sum.value.length > 0) {
            const arr = item.values.rain_sum.value;
            rain1Day = cleanNumber(arr[arr.length - 1]) ?? rainNow;
          }

          // ระดับน้ำ
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
                  updatedAt: new Date()
                }
              },
              upsert: true
            }
          };
        }).filter(Boolean);

        if (bulkOps.length > 0) {
          await collection.bulkWrite(bulkOps);
          console.log('🎉 อัปเดตข้อมูลสดลง MongoDB Atlas สำเร็จ 100%!');
        }
        ws.close();
        await client.close();
        process.exit(0);
      }
    } catch (err) {
      console.error('❌ Error:', err.message);
    }
  });
}

syncNow();