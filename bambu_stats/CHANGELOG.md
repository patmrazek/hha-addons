# Changelog

## 0.11.0 - 2026-09-12
- Rezim 'jen synchronizace': bez printer_host/access_code instance nesleduje tiskarnu, jen stahuje sdilenou historii
  z git repa a publikuje statistiky. Pro lokalitu, kde tiskarna prave nestoji.

## 0.10.1 - 2026-09-12
- Neznamy obsah slotu ('?', Empty) uz nehlasi neshodu civky (tiskarna slot necte behem tisku / refill bez RFID).

## 0.10.0 - 2026-09-12
- Zmetky: tlacitka 'Posledni tisk byl zmetek' / 'byl v poradku' (prikazy mark_defect[:<id>[:<poznamka>]], mark_ok),
  senzor defect_prints, zmetek se nepocita do uspesnosti ani do uspesnych tisku (filament zustava spotrebovany).

## 0.9.2 - 2026-09-12
- Kontrola filamentu porovnava zbyvajici potrebu tisku (plan minus uz odectene) se zbytkem civky. Drive se cely plan
  porovnaval s civkou uz snizenou prubeznym odectem, takze deficit vychazel prehnany.

## 0.9.1 - 2026-09-12
- Atributy u celkoveho filamentu se konecne publikuji (klic byl total_filament_attrs misto total_filament_kg_attrs):
  podil odhadu, zdroje, evidovane vs. neevidovane kg.

## 0.9.0 - 2026-09-12
- Celkova spotreba a utrata za filament zahrnuji i to, co ubylo z civek mimo evidovane tisky (starsi tisky pred
  zavedenim add-onu). Pocita se ze Spoolmanu: ubytek civky minus odecty nasich session. Nove entity
  filament_untracked_kg a cost_untracked, rozpad v atributech total_filament_kg / total_cost.

## 0.8.1 - 2026-09-10
- OPRAVA: resolve filamentu bezel soubezne z planovace i z retry marku a odecet ze civky se provedl dvakrat.
  Nove je resolve serializovany zamkem.

## 0.8.0 - 2026-09-10
- Prubezny odecet ze civky ve Spoolmanu behem tisku (kazdych 5 min, jen prirustky nad 5 g, strop 90 % odhadu),
  po dokonceni dopocet na plnou spotrebu. Pri revizi planu dolu se prebytek vrati (PATCH remaining_weight).
- Statistiky (kg, pocty, naklady) zustavaji jen za dokoncene tisky.

## 0.7.2 - 2026-09-10
- Kdyz ma stejny slot prirazeno vic civek (Spoolman neumi extra smazat), bere se naposledy pouzita.

## 0.7.1 - 2026-09-10
- Sloty se kontroluji kazdou minutu (zmena prirazeni civky ve Spoolmanu se projevi hned) a po prikazu recompute.

## 0.7.0 - 2026-09-10
- Detekce neshody civky: kdyz tiskarna hlasi u slotu jiny material nez civka prirazena ve Spoolmanu, slot dostane ⚠
  a entita slot_mismatch prepne na on (automatizace posle push). Brani odectu z chybne civky.

## 0.6.1 - 2026-09-10
- Historie vymen silikagelu se doplni i z drive zaznamenaneho casu (pred 0.6.0).

## 0.6.0 - 2026-09-10
- Vlhkost AMS v case: entita ams_humidity_history (48 h po hodinach, 90 dni po dnech min/prum/max z telemetrie),
  historie vymen silikagelu a udrzby v meta (desiccant_changed_history / maintenance_done_history) a jako body v grafu.

## 0.5.2 - 2026-09-10
- 3MF z cache drzi threemf_status=ok (zdroj hmotnosti se po refetch nezobrazoval).

## 0.5.1 - 2026-09-09
- Nahled modelu se uklada i pri dokonceni, refetch a periodicky u bezici session.

## 0.5.0 - 2026-09-09
- Kontrola filamentu pred tiskem (filament_check: plan per slot vs. zbyvajici gramy civky ve Spoolmanu).
- Udrzba: tlacitka maintenance_done / desiccant_changed, senzory hours_since_maintenance, prints_since_maintenance,
  days_since_desiccant, nozzle_wear; options maintenance_every_hours, desiccant_every_days.
- Nahledy modelu do /config/www/bambu_stats/covers (map homeassistant_config), sloupec img v historii; statistiky per model (models).

## 0.4.2 - 2026-09-09
- Entity slot_1..4: co je v AMS slotech podle Spoolmanu (nazev civky, barva, zbyva g, cena), fallback udaje tiskarny.

## 0.4.1 - 2026-09-09
- Oprava: u uzavrene session se uz necte ziva cloud hmotnost (patri dalsimu tisku). Prikaz set_plan:<konec_id>:<g>[:<m>].

## 0.4.0 - 2026-09-09
- Spoolman: odecet spotreby z civky prirazene ke slotu (SpoolmanSync extra.active_tray / RFID extra.tag), cena tisku
  ze skutecne ceny civky; option spoolman_url. Sloupce spool_id, spool_price_per_kg, spool_deducted_g.
- Slot filamentu se zachyti pri prechodu do RUNNING (v PREPARE byva 255 -> material 'ostatni'). Prikaz
  assign_slot:<konec_id>:<tray_global> pro rucni doplneni.

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
