require('dotenv').config();
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017';
const DB_NAME = process.env.DB_NAME || 'telerid_db';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'stations';

// ข้อมูลจริง 100% จากระบบ TeleRID ที่สกัดได้
const rawItems = [
  {
    "type": "CRITICAL",
    "data": {
      "id": 341914098,
      "station": { "id": 354, "code": "TTC08", "name": "ที่ว่าการ อ.นครชัยศรี" },
      "value": 2.455,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "WARNING",
    "data": {
      "id": 341914092,
      "station": { "id": 353, "code": "TTC07", "name": "โรงเรียนวัดบางปลา" },
      "value": 3.089,
      "is_warning": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341913939,
      "station": { "id": 398, "code": "TK.74", "name": "โรงเรียนบ้านลาดใหญ่สามัคคี" },
      "value": 1.341,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341913205,
      "station": { "id": 569, "code": "TM.204 (U/S), (D/S)", "name": "Tฝายชุมพวง (U/S), (D/S)" },
      "value": 143.86,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "WARNING",
    "data": {
      "id": 341912446,
      "station": { "id": 10, "code": "T.MakhamthaoRegulator", "name": "T.ปตร.มะขามเฒ่า" },
      "value": 16.87,
      "is_warning": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341912435,
      "station": { "id": 4, "code": "TC.55", "name": "TC.55" },
      "value": 1.52,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "WARNING",
    "data": {
      "id": 341912376,
      "station": { "id": 475, "code": "TE.105", "name": "วัดปทุมชาติ (บ้านกอก)" },
      "value": 202.78,
      "is_warning": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341911925,
      "station": { "id": 38, "code": "TSK.14", "name": "อบต.สว่างแจ้งสบายใจ" },
      "value": 35.91,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341911912,
      "station": { "id": 55, "code": "TS.17", "name": "คลองน้ำจู" },
      "value": 90.68,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341911873,
      "station": { "id": 352, "code": "TTC06", "name": "วัดท่าเจดีย์" },
      "value": 3.12,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341911712,
      "station": { "id": 778, "code": "TTP21", "name": "สวนสาธารณะเกาะลำพู" },
      "value": 1.19,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "WARNING",
    "data": {
      "id": 341911385,
      "station": { "id": 548, "code": "TM.145", "name": "TM.145" },
      "value": 390.68,
      "is_warning": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341911376,
      "station": { "id": 351, "code": "TTC05", "name": "วัดมณีวรรณ" },
      "value": 3.98,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341911289,
      "station": { "id": 567, "code": "TM.202 (U/S), (D/S)", "name": "Tเขื่อนพิมาย (U/S), (D/S)" },
      "value": 152.70,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  },
  {
    "type": "CRITICAL",
    "data": {
      "id": 341909553,
      "station": { "id": 365, "code": "TTC19", "name": "ปตร.กำแพงแสน" },
      "value": 4.09,
      "is_critical": true,
      "datetime": "2026-11-09T16:30:00+07:00",
      "parameter": "WL_UP"
    }
  }
];

async function runDirectImport() {
  console.log(`📥 กำลังบันทึกข้อมูลสถานีจริง ${rawItems.length} รายการลงสู่ MongoDB...`);

  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    let count = 0;
    for (const item of rawItems) {
      const st = item.data?.station || {};
      const code = st.code || `ID-${item.data?.id}`;
      const name = st.name || '-';
      const param = item.data?.parameter || 'WL_UP';
      const val = typeof item.data?.value === 'number' ? item.data.value : parseFloat(item.data?.value || 0);
      const isCrit = item.type === 'CRITICAL' || item.data?.is_critical;
      const isWarn = item.type === 'WARNING' || item.data?.is_warning;
      const statusText = isCrit ? 'วิกฤติ' : (isWarn ? 'เตือนภัย' : 'ปกติ');
      const dt = item.data?.datetime || new Date().toISOString();

      await collection.updateOne(
        { stationCode: code },
        {
          $set: {
            stationCode: code,
            stationName: name,
            parameter: param,
            value: val,
            status: statusText,
            recordedAt: dt,
            updatedAt: new Date()
          }
        },
        { upsert: true }
      );
      count++;
    }

    console.log(`\n🎉 อัปเดตข้อมูลสถานีจริงเข้าสู่ MongoDB สำเร็จ 100%: ${count} รายการ!`);
    console.log('👉 เปิดดูผลงานได้ที่หน้าเว็บ index.html หรือกดปุ่ม "รีเฟรชข้อมูล" ได้ทันที');
  } catch (err) {
    console.error('💥 เกิดข้อผิดพลาด:', err.message);
  } finally {
    await client.close();
  }
}

runDirectImport();
