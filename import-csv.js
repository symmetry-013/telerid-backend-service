require('dotenv').config();
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { MongoClient } = require('mongodb');

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017';
const DB_NAME = process.env.DB_NAME || 'telerid_db';
const COLLECTION_NAME = process.env.COLLECTION_NAME || 'stations';

// พิกัดไฟล์ CSV จาก Downloads
const csvFilePath = path.join(
  process.env.DOWNLOADS_PATH || 'C:/Users/Tele_04/Downloads',
  'telerid_real_alarm.csv'
);

async function runImport() {
  if (!fs.existsSync(csvFilePath)) {
    console.error(`❌ ไม่พบไฟล์ที่พิกัด: ${csvFilePath}`);
    console.error('กรุณาตรวจสอบว่ามีไฟล์ telerid_real_alarm.csv อยู่ในโฟลเดอร์ Downloads หรือไม่');
    return;
  }

  const client = new MongoClient(MONGO_URI);
  try {
    await client.connect();
    const db = client.db(DB_NAME);
    const collection = db.collection(COLLECTION_NAME);

    console.log(`📥 กำลังอ่านไฟล์ CSV จาก: ${csvFilePath} ...`);
    const records = [];

    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', (row) => {
        // แมปข้อมูลจากคอลัมน์ใน CSV
        const code = row.Station_Code || row['Station Code'] || '';
        const name = row.Station_Name || row['Station Name'] || '';
        const param = row.Parameter || '';
        const val = parseFloat(row.Value);
        const statusType = row.Type || 'NORMAL';
        const dt = row.DateTime || new Date().toISOString();

        if (code && code !== 'UNKNOWN') {
          records.push({
            stationCode: code.replace(/"/g, '').trim(),
            stationName: name.replace(/"/g, '').trim(),
            parameter: param.replace(/"/g, '').trim(),
            value: isNaN(val) ? 0 : val,
            status: statusType.replace(/"/g, '').trim(),
            recordedAt: dt.replace(/"/g, '').trim(),
            updatedAt: new Date()
          });
        }
      })
      .on('end', async () => {
        console.log(`📊 อ่านข้อมูลสำเร็จทั้งหมด ${records.length} แถว กำลังบันทึกลง MongoDB...`);
        
        let successCount = 0;
        for (const item of records) {
          await collection.updateOne(
            { stationCode: item.stationCode },
            { $set: item },
            { upsert: true }
          );
          successCount++;
        }

        console.log(`✅ อัปเดตข้อมูลเข้า MongoDB สำเร็จเรียบร้อย: ${successCount} รายการ!`);
        await client.close();
        process.exit(0);
      });
  } catch (error) {
    console.error('💥 เกิดข้อผิดพลาด:', error.message);
    if (client) await client.close();
  }
}

runImport();
