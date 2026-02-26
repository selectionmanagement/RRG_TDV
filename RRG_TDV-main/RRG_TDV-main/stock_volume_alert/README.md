# Stock Volume Breakout (AVG5)

โมดูลนี้ใช้สำหรับติดตามสัญญาณปริมาณซื้อขายจาก TradingView แบบไม่ต้องพึ่ง SQL สำหรับการใช้งานหลักบนหน้าแดชบอร์ด

## สถานะปัจจุบัน
- แท็บที่ใช้งาน: `Live`, `Chart`, `Backfill`, `Errors`
- ไม่มีแท็บแก้ไขรายชื่อหุ้นในหน้า UI แล้ว
- รายชื่อหุ้นโหลดจาก `stock_volume_alert/data/symbols.txt` (หรือค่าเริ่มต้นใน config หากไฟล์ยังไม่มี)

## ความสามารถหลัก
- `Live Snapshot`: ดึงราคา/วอลุ่มล่าสุด และคำนวณ `AVG5/AVG10/AVG20/AVG50`
- `Break AVG5`: ตรวจเงื่อนไข `vol_today > avg5`
- `Chart`: ดูแท่งเทียน + SMA + Volume/AVG สำหรับหุ้นรายตัว
- `Backfill`: คำนวณย้อนหลังแบบ on-demand จาก TradingView แล้วแสดงผลใน session
- `Errors`: รวมข้อผิดพลาดจาก live/backfill เพื่อวิเคราะห์เร็ว

## ตำแหน่งข้อมูล
- Symbols file: `stock_volume_alert/data/symbols.txt`
- ผลลัพธ์ Live/Backfill เก็บใน `st.session_state` ระหว่าง session

## วิธีรัน (Standalone)
```powershell
cd RRG_TDV-main/stock_volume_alert
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run dashboard/app.py
```

## วิธีใช้งาน
1. ไปที่แท็บ `Live` แล้วกด `Refresh now`
2. ไปที่แท็บ `Chart` เพื่อดูกราฟรายตัว
3. ไปที่แท็บ `Backfill` เลือกจำนวนวัน แล้วกด `Run backfill now`
4. หากมีปัญหา ให้ตรวจที่แท็บ `Errors`

## หมายเหตุ
- หากข้อมูลเยอะและเจอ timeout ให้ลดจำนวน workers แล้วรันใหม่
- Backfill ถูกคำนวณจาก TradingView ทุกครั้งที่สั่งรัน ไม่ได้อ่านรายงานจาก SQL เดิม

---
Updated: 2026-02-26
