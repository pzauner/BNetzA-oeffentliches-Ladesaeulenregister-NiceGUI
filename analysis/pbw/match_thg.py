from __future__ import annotations
import csv, json, re, sys, unicodedata, difflib
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from app.data import load_data, get_latest_csv

IN = ROOT / 'analysis/pbw/uba_thg_zertifikate_pbw.csv'
MATCHES = ROOT / 'analysis/pbw/uba_thg_zertifikate_pbw_matches.csv'
UNMATCHED = ROOT / 'analysis/pbw/uba_thg_zertifikate_pbw_unmatched.csv'
SUMMARY = ROOT / 'analysis/pbw/uba_thg_zertifikate_pbw_summary.json'
META_ROOT = ROOT / 'station-context'
SOURCE_LABEL = 'UBA-Auskunft UIG zu THG-Zertifikaten der PBW, PDF ohne Seite 1'

def norm(s: object) -> str:
    s = str(s or '').lower().strip().replace('ß', 'ss')
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'\bstrasse\b|\bstr\.?\b', 'strasse', s)
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

def sanitize_id(station_id: str) -> str:
    return re.sub(r'[^A-Za-z0-9._\-]', '_', str(station_id))

def read_thg() -> list[dict[str, str]]:
    with IN.open('r', encoding='utf-8-sig', newline='') as f:
        return [{k: (v or '').strip() for k, v in row.items()} for row in csv.DictReader(f, delimiter=';')]

def write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    with path.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter=';')
        w.writeheader()
        for row in rows:
            w.writerow({field: row.get(field, '') for field in fields})

def station_meta_path(station_id: str) -> Path:
    return META_ROOT / sanitize_id(station_id) / 'meta.json'

def update_meta(matches: list[dict[str, object]]) -> int:
    years_by_id: dict[str, set[str]] = defaultdict(set)
    rows_by_id: dict[str, list[dict[str, object]]] = defaultdict(list)
    for m in matches:
        sid = str(m['ladeeinrichtungs_id'])
        years_by_id[sid].add(str(m['jahr']))
        rows_by_id[sid].append(m)
    changed = 0
    for sid, years in sorted(years_by_id.items()):
        path = station_meta_path(sid)
        path.parent.mkdir(parents=True, exist_ok=True)
        meta = {}
        if path.exists():
            try:
                meta = json.loads(path.read_text(encoding='utf-8'))
            except Exception:
                meta = {}
        old_years = meta.get('thg_certificate_years', [])
        if not isinstance(old_years, list):
            old_years = []
        merged_years = sorted({str(y) for y in old_years if str(y).strip()} | years)
        compact_matches = []
        seen = set()
        for row in rows_by_id[sid]:
            key = (row['thg_row'], row['jahr'])
            if key in seen:
                continue
            seen.add(key)
            compact_matches.append({
                'thg_row': row['thg_row'],
                'jahr': row['jahr'],
                'adresse_uba': row['adresse_uba'],
                'adresse_lsr': row['adresse_lsr'],
                'score': row['score'],
            })
        meta['thg_certificate_years'] = merged_years
        meta['thg_certificate_source'] = SOURCE_LABEL
        meta['thg_certificate_matches'] = compact_matches
        path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        changed += 1
    return changed

thg = read_thg()
df, err, stats = load_data(get_latest_csv())
if err or df is None:
    raise SystemExit(err or 'Konnte LSR-Daten nicht laden')
pbw = df[df['Betreiber'].astype(str).str.contains('Parkraumgesellschaft Baden', na=False)].copy()
for c in ['Straße', 'Hausnummer', 'Postleitzahl', 'Ort']:
    pbw[c + '_n'] = pbw[c].map(norm)

matches: list[dict[str, object]] = []
unmatched: list[dict[str, object]] = []
for idx, r in enumerate(thg, start=1):
    rnorm = {k + '_n': norm(r.get(k, '')) for k in ['strasse', 'hausnummer', 'plz', 'ort']}
    if not rnorm['hausnummer_n']:
        unmatched.append({**r, 'thg_row': idx, 'grund': 'keine Hausnummer in UBA-Zeile; nicht automatisch gematcht', 'best_score': '', 'best_lsr': ''})
        continue
    cand = pbw[pbw['Postleitzahl_n'].eq(rnorm['plz_n'])] if rnorm['plz_n'] else pbw.copy()
    cand_hn = cand[cand['Hausnummer_n'].eq(rnorm['hausnummer_n'])]
    if len(cand_hn):
        cand = cand_hn
    else:
        unmatched.append({**r, 'thg_row': idx, 'grund': 'keine passende Hausnummer im LSR', 'best_score': '', 'best_lsr': ''})
        continue
    if rnorm['ort_n']:
        cand_city = cand[cand['Ort_n'].eq(rnorm['ort_n'])]
        if len(cand_city):
            cand = cand_city
    exact = cand[cand['Straße_n'].eq(rnorm['strasse_n'])]
    score = 1.0
    if len(exact):
        selected = exact
    else:
        scored = []
        for _, cr in cand.iterrows():
            sc = difflib.SequenceMatcher(None, rnorm['strasse_n'], cr['Straße_n']).ratio()
            if rnorm['strasse_n'] and (rnorm['strasse_n'] in cr['Straße_n'] or cr['Straße_n'] in rnorm['strasse_n']):
                sc = max(sc, 0.92)
            scored.append((sc, cr))
        scored.sort(key=lambda x: x[0], reverse=True)
        if not scored or scored[0][0] < 0.88:
            best = scored[0][1] if scored else None
            unmatched.append({**r, 'thg_row': idx, 'grund': 'Straße nicht sicher gematcht', 'best_score': round(scored[0][0], 3) if scored else '', 'best_lsr': '' if best is None else f"{best['Straße']} {best['Hausnummer']}, {best['Postleitzahl']} {best['Ort']} ({best['Ladeeinrichtungs-ID']})"})
            continue
        score = scored[0][0]
        top = scored[0][1]
        selected = pbw[(pbw['Postleitzahl_n'].eq(top['Postleitzahl_n'])) & (pbw['Straße_n'].eq(top['Straße_n'])) & (pbw['Hausnummer_n'].eq(top['Hausnummer_n']))]
    for _, cr in selected.iterrows():
        matches.append({
            'thg_row': idx,
            'jahr': r.get('jahr_bescheinigung', ''),
            'adresse_uba': f"{r.get('strasse','')} {r.get('hausnummer','')}, {r.get('plz','')} {r.get('ort','')}".strip(),
            'ladeeinrichtungs_id': str(cr['Ladeeinrichtungs-ID']),
            'adresse_lsr': f"{cr['Straße']} {cr['Hausnummer']}, {cr['Postleitzahl']} {cr['Ort']}",
            'lsr_status': str(cr.get('Status', '')),
            'lsr_ladepunkte': str(cr.get('Anzahl Ladepunkte', '')),
            'score': round(float(score), 3),
        })

match_fields = ['thg_row', 'jahr', 'adresse_uba', 'ladeeinrichtungs_id', 'adresse_lsr', 'lsr_status', 'lsr_ladepunkte', 'score']
unmatch_fields = ['thg_row', 'jahr_bescheinigung', 'strasse', 'hausnummer', 'plz', 'ort', 'grund', 'best_score', 'best_lsr']
write_csv(MATCHES, matches, match_fields)
write_csv(UNMATCHED, unmatched, unmatch_fields)
changed = update_meta(matches)
summary = {
    'uba_rows': len(thg),
    'matched_links': len(matches),
    'matched_unique_station_ids': len({m['ladeeinrichtungs_id'] for m in matches}),
    'unmatched_rows': len(unmatched),
    'meta_files_updated': changed,
    'years': sorted({r.get('jahr_bescheinigung', '') for r in thg if r.get('jahr_bescheinigung', '')}),
    'input': str(IN.relative_to(ROOT)),
    'matches_csv': str(MATCHES.relative_to(ROOT)),
    'unmatched_csv': str(UNMATCHED.relative_to(ROOT)),
}
SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(json.dumps(summary, ensure_ascii=False, indent=2))
