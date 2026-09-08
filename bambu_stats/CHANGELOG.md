# Changelog

## 0.1.2 – 2026-09-08
- Nastavení: plochá pole pro jednu tiskárnu (printer_host, printer_serial, access_code…). Seznam `printers` v UI
  add-onu ztrácel hodnoty; zůstává jen jako pokročilá volba pro více tiskáren.

## 0.1.1 – 2026-09-08
- MQTT Discovery: `default_entity_id` místo odstraněného `object_id` (HA 2026.x) – bez toho HA entity nevytvořil.
- CSV export historie do /share/bambu_stats/.

## 0.1.0 – 2026-09-08
- První verze: sběr MQTT z tiskárny, detekce tiskových session, SQLite historie,
  spotřeba filamentu (3MF/USB → ha-bambulab cloud → AMS remain), MQTT Discovery statistik.
