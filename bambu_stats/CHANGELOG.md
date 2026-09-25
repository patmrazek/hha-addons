# Changelog

## 0.17.4 - 2026-09-25

- **Oprava fantomové spotřeby na začátku tisku.** Přípravná session, kterou za pár vteřin nahradí
  skutečná, zdědila číslo vrstvy z předchozího, už dotištěného tisku. Postup se pak počítal jako
  „stará vrstva / nové vrstvy" – 25. 9. 2026 vyšlo 935 / 28 = 3 367 % a 1 269 g spotřeby tisku,
  který se ani nerozběhl; tři takové session srazily plnou cívku ve Spoolmanu na nulu.
  Nově: session, která nedošla do tisku, nespotřebovala nic, a postup nikdy nepřesáhne 100 %.
- Nový příkaz `resolve:<id>` – přepočítá spotřebu uzavřeného tisku; rozdíl proti už odečtenému
  se ve Spoolmanu dorovná, i vrácením.


## 0.17.3 - 2026-09-24

- **Oprava: od 0.16 se nestahoval 3MF z tiskárny.** Stahování si adresu bralo z `printer_host`,
  který je od zavedení hledání tiskárny (`printer_hosts`) prázdný → `Name does not resolve`.
  Bez 3MF add-on nezná rozdělení spotřeby mezi barvy, takže dvoubarevný tisk 24. 9. skončil
  jako jediný řádek bez slotu a neodečetl se nikam. Adresu teď dodává collector podle toho,
  kde tiskárnu našel.
- **Oprava ošetření chyby FTP (od počátku).** `ftplib.all_errors` je sama n-tice a vnořená do
  `except (…)` ji Python 3.12 odmítne — ale až ve chvíli, kdy výjimka opravdu nastane. Místo tichého
  přeskočení nestaženého 3MF to shodilo celý výpočet spotřeby.


## 0.17.2 - 2026-09-24

- **Oprava dělení spotřeby u vícebarevných tisků.** Dělení podle slotů (0.15) počítalo s tiskem
  z jednoho filamentu: každou změnu slotu bralo jako auto-refill a rozpočítalo všechny filamenty
  mezi všechny sloty. U dvoubarevného tisku by tak bílá cívka dostala podíl černé a černé cívky
  podíl bílé. Nově se každý řádek dělí jen mezi sloty, kde byl na začátku stejný materiál a barva
  — černá mezi dvě černé cívky, bílá zůstane na bílé. Slot bez známého obsahu se nepoužije vůbec.
- Když tisk celý běžel z jiného slotu se stejným filamentem, než kam ho přiřadil slicer (dvě stejné
  cívky v AMS), řádek se přesune na skutečný slot, místo aby se odečetl z cívky, která netiskla.


## 0.17.1 - 2026-09-24

- Údržbu i výměnu silikagelu jde zapsat zpětně: `maintenance_done@2026-09-18T20:00`,
  `desiccant_changed@2026-09-18T20:00`. Bez toho by interval běžel od chvíle, kdy se na zápis
  přišlo, ne od skutečné výměny — a právě proto 24. 9. 2026 chybělo datum silikagelu měněného
  18. 9. Bez času se chová jako dřív a zapíše přítomnost.


## 0.17.0 - 2026-09-24

- **Mezi lokalitami se sdílí i provozní stav, nejen tisky.** Dosud se synchronizovaly jen
  session; údržba, výměny silikagelu a deník osazení slotů zůstávaly v té databázi, kde vznikly.
  Po převozu tiskárny tak počítadlo údržby začínalo od nuly, silikagel neměl od čeho počítat
  interval (24. 9. 2026 hlásil `unknown`, i když se měnil 18. 9.) a odečty ze cívek sahaly
  na cívku, o které druhá lokalita nevěděla.
- Slučování: u časů (poslední údržba) vyhrává novější zápis, historie výměn se spojí, deník
  slotů má nově přirozený klíč (tiskárna, slot, od kdy), takže se tatáž výměna neuloží dvakrát.
  Sdílí se i HMS události.
- **Nesdílí se záměrně:** telemetrie `samples` (desítky MB, mimo svou lokalitu bezcenná) a IP
  s TLS otiskem tiskárny (v druhé síti platí jiné).
- Migrace: staré databáze můžou mít v deníku slotů duplicity z doby bez klíče — ty se při
  prvním startu sloučí, ponechá se nejnovější zápis.


## 0.16.1 - 2026-09-23

- **Oprava hledání tiskárny z 0.16.0.** Rozhodnutí „mám ji ve své síti?" se opíralo o adresu
  zvolenou pro spojení, jenže add-on běží v kontejneru s adresou 172.30.x.x — nikdy nesedla
  s podsítí tiskárny, takže i tiskárna ve vlastní LAN vyšla jako „za VPN" a instance přestala
  sbírat uprostřed tisku. Podsítě hostitele se nově berou ze Supervisoru (`/network/info`).
- Oprava schématu `printer_hosts`: položka seznamu musí být `str`, ne `str?` — Supervisor
  seznam s `str?` tiše zahodil a dorazil prázdný.


## 0.16.0 - 2026-09-23

- **Tiskárna se hledá sama, sběr se přepíná bez zásahu.** Nová volba `printer_hosts` bere seznam
  adres, na kterých tiskárna může stát. Add-on je zkouší a podle adresy, kterou si operační systém
  vybere pro spojení, pozná, jestli tiskárna stojí v jeho síti, nebo je vidět jen přes VPN. Sbírá
  vždy jen ta instance, u které tiskárna fyzicky je — přes VPN na ni dosáhnou obě a sbíraly by
  dvakrát. Kontroluje se po dvou minutách, takže po převozu se sběr rozběhne sám; nikdy se
  nepřepíná uprostřed tisku, aby se otevřená session neroztrhla.
- Do stavu collectoru přibyl údaj `umisteni` – kde tiskárna je a která lokalita ji sleduje.
- Odpadá tím ruční přepínání `printer_host` a `spoolman_url` po každém převozu.


## 0.15.1 - 2026-09-22

- **Oprava dělení spotřeby při výměně cívky.** Rozdělení podle slotů (0.15.0) bralo jako přepnutí
  i okamžik, kdy tiskárna během pauzy na výměnu filamentu ohlásila cizí slot — 22. 9. 2026 takhle
  přiskočily 4 g šedého PETG k tisku, který celý běžel z černého PLA. Nově se úsek založí jen
  za běhu tisku, ignoruje se slot hlášený jako prázdný a úsek kratší než 2 % postupu; po odpadnutí
  takového zákmitu se sousední úseky téhož slotu zase spojí v jeden.
- Ruční výměna cívky ve stejném slotu se tím pádem nezaznamená vůbec — tu pozná jen obsluha
  a opraví se ručně. Auto-refill, kvůli kterému dělení vzniklo, mění slot za běhu a zachytí se.


## 0.15.0 - 2026-09-21

- **Auto-refill AMS se konečně zaznamená.** Když ve slotu dojde filament, AMS sáhne po jiné cívce
  stejné barvy a tiskne dál. Dosud se celá spotřeba připsala slotu, ve kterém tisk začal, a cívka,
  ze které se dotisklo, zůstala v evidenci nedotčená (21. 9. 2026 takhle ušlo 142 g a dojetá cívka
  vypadala, že má ještě 237 g). Nově session vede úseky tisku po slotech (`tray_spans`) a spotřeba
  se mezi cívky rozdělí poměrem podle procenta postupu. Dělené řádky nesou `is_estimate=1`
  a `mapping_source='refill_split'` — poměr podle procent je odhad, spotřeba na procento není
  rovnoměrná, ale je to řádově blíž pravdě než všechno na jednu cívku.
- Schéma databáze 7 (přibyl sloupec `sessions.tray_spans`, migrace je aditivní).


## 0.14.2 - 2026-09-20

- **Oprava:** doúčtování do Spoolmanu se nově týká jen vlastních tisků. Tisky naimportované gitem
  z druhé lokality (`origin` jiné instance) se přeskakují – jinak by je odečetly obě instance a
  filament by ze cívky zmizel dvakrát. Platí pravidlo: do Spoolmanu zapisuje ta instance, u které
  tiskárna fyzicky stojí.


## 0.14.1 - 2026-09-20

- **Oprava:** doúčtování fronty do Spoolmanu se zacyklilo. `flush_pending_spools()` se po úspěšném
  kole volal znovu, ale za „úspěch" považoval i tisk, u kterého se odečet schválně odložil (cívka
  ve slotu není ve Spoolmanu). Fronta tím nikdy neubyla, rekurze běžela dál a tytéž tisky se
  odečítaly pořád dokola – 20. 9. 2026 to vyprázdnilo cívky 13 a 17. Nově se rekurze spustí jen
  tehdy, když fronta opravdu ubyla, a nejvýš 5×.

## 0.14.0 - 2026-09-17
- set_slot umi i cas vymeny: set_slot:<slot>:<spool|->:<popis>@<ISO cas>. Dulezite, kdyz se zapisuje zpetne –
  tisky se pak priradi civce, ktera byla ve slotu v dobe tisku.

## 0.13.2 - 2026-09-16
- Slot ukazuje i civku zapsanou jen lokalne (bez ID ve Spoolmanu), s poznamkou 'mimo Spoolman'.

## 0.13.1 - 2026-09-16
- Kdyz je ve slotu civka zapsana jen lokalne (Spoolman ji jeste nezna), odecet se odlozi misto strzeni z predchozi civky.

## 0.13.0 - 2026-09-14
- Lokalni denik osazeni slotu: prikaz set_slot:<slot>:<spool_id>[:<popis>] zapise, co je ve slotu, i kdyz je Spoolman
  nedostupny. Pri doucovani se pouzije civka, ktera byla ve slotu v dobe tisku; po obnoveni spojeni se prirazeni
  propise do Spoolmanu.

## 0.12.0 - 2026-09-12
- Kdyz je Spoolman nedostupny (jina lokalita, vypadek VPN), spotreba se drzi lokalne a po obnoveni spojeni se
  automaticky doucuje (kazdych 5 min). Novy senzor spoolman_pending ukazuje, kolik tisku a gramu ceka.

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
