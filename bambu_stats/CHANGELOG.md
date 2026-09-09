# Changelog

## 0.3.1 – 2026-09-09
- Výchozí options obsahují `printers: []` (Supervisor jinak hlásí chybějící klíč).

## 0.3.0 – 2026-09-09
- Synchronizace historie mezi lokalitami přes sdílený privátní git repozitář (`sync_repo`, `sync_token`, `sync_instance`).
  Sloupce `origin`/`synced_ts` v sessions, `git` v image.

## 0.2.1 – 2026-09-09
- Nákladové entity mají jednotku jako symbol měny (Kč) místo ISO kódu.

## 0.2.0 – 2026-09-09
- Náklady na filament: options `filament_prices` (cena/kg podle materiálu) a `currency`; entity `total_cost`,
  `cost_{today,7d,30d}`, `avg_cost_per_print`, `current_cost`; sloupec `cost` v řadách `usage`, `c` v historii.

## 0.1.3 – 2026-09-09
- Nová entita `current_filament_g` – odhad zatím spotřebovaného filamentu běžícího tisku (plán × %).
- Řady pro grafy s volitelným rozsahem: hourly (48 h z telemetrie), daily (365), weekly (104), monthly (vše).
- Začátek session obnovené odhadem se upřesní z ha-bambulab `*_start_time` (Bambu cloud).

## 0.1.2 – 2026-09-08
- Nastavení: plochá pole pro jednu tiskárnu (printer_host, printer_serial, access_code…). Seznam `printers` v UI
  add-onu ztrácel hodnoty; zůstává jen jako pokročilá volba pro více tiskáren.

## 0.1.1 – 2026-09-08
- MQTT Discovery: `default_entity_id` místo odstraněného `object_id` (HA 2026.x) – bez toho HA entity nevytvořil.
- CSV export historie do /share/bambu_stats/.

## 0.1.0 – 2026-09-08
- První verze: sběr MQTT z tiskárny, detekce tiskových session, SQLite historie,
  spotřeba filamentu (3MF/USB → ha-bambulab cloud → AMS remain), MQTT Discovery statistik.
