# Stock Volume Break AVG5 (No SQL)

เวอร์ชันนี้เน้นใช้งานผ่าน Streamlit dashboard แบบง่าย ๆ และไม่ต้องพึ่ง SQLite เพื่อเก็บรายงานย้อนหลัง

## แนวคิดหลัก
- Live Snapshot: ดึงราคาล่าสุด + ปริมาณจาก TradingView scanner
- AVG5/AVG10/AVG20/AVG50: คำนวณจากข้อมูลรายวันย้อนหลังผ่าน TradingView WebSocket
- Break AVG5: `vol_today > avg5`
- Backfill ย้อนหลัง: คำนวณ on-demand จาก TradingView แล้วแสดงบน dashboard เท่านั้น (ไม่เขียนลง SQL)

## เก็บข้อมูลไว้ที่ไหน
- Symbols list จะเก็บในไฟล์ `data/symbols.txt`
- ผล Live และ Backfill เก็บใน `st.session_state` ระหว่าง session ของ dashboard

## วิธีรัน
```powershell
cd RRG_TDV-main/stock_volume_alert
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run dashboard/app.py
```

## วิธีใช้งาน
1. ไปแท็บ **Symbols** แล้วแก้รายชื่อหุ้นตามต้องการ
2. ไปแท็บ **Live** แล้วกด **Refresh now**
3. ไปแท็บ **Backfill** แล้วเลือก `Backfill days` (เช่น 60) จากนั้นกด **Run backfill now**

## ทำไมเดิมถึงไม่เห็น Volume Break ย้อนหลัง
- โค้ดเดิมสร้างรายงานย้อนหลังจากตาราง `reports` ใน SQLite
- ถ้าไม่ได้รัน worker ต่อเนื่องในช่วงตลาดเปิด รายงานจะไม่ถูกสร้าง
- เมื่อไม่มีข้อมูลใน `reports` หน้า Reports จะว่าง แม้ symbols จะมีอยู่
- เวอร์ชันนี้แก้โดยคำนวณย้อนหลังตรงจาก TradingView ทุกครั้งที่กด backfill แทน

## หมายเหตุ
- การดึงย้อนหลัง 60 วันสำหรับหุ้นจำนวนมากอาจใช้เวลา
- หากเจอ timeout ให้ลด `Backfill workers` หรือรันซ้ำ
