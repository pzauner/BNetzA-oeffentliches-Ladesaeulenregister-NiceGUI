from __future__ import annotations
import csv,json,re
from pathlib import Path
from collections import defaultdict
ROOT=Path('/root/BNetzA-oeffentliches-Ladesaeulenregister-NiceGUI')
META_ROOT=ROOT/'station-context'
MANUAL=ROOT/'analysis/pbw/uba_thg_zertifikate_pbw_manual_matches.csv'
REMAIN=ROOT/'analysis/pbw/uba_thg_zertifikate_pbw_still_unmatched.csv'
SUMMARY=ROOT/'analysis/pbw/uba_thg_zertifikate_pbw_summary.json'
SOURCE='UBA-Auskunft UIG zu THG-Zertifikaten der PBW, PDF ohne Seite 1; manuell nachgematcht'

def sid_path(sid):
 return META_ROOT/re.sub(r'[^A-Za-z0-9._\-]','_',str(sid))/'meta.json'
# thg_row, year, UBA address, IDs, LSR address, note
manual = [
 (1,'2025','Anton-Huber-Straße 51, 73430 Aalen',['1118094'],'Anton-Huber-Str. 5, 73430 Aalen','Hausnummer 51 vs. LSR 5; sonst Straße/PLZ/Ort eindeutig PBW'),
 (6,'2025','Baetznerstraße 90, 75323 Bad Wildbad',['1150058','1150059'],'Bätznerstr. 90, 75323 Bad Wildbad','Schreibweise Baetzner/Bätzner'),
 (7,'2025','Flandernstrasse 101a, 73732 Esslingen',['1118100'],'Flandernstr. 101 A, 73732 Esslingen am Neckar','Schreibweise/Leerzeichen 101a/101 A'),
 (8,'2025','Humboldtstrasse 3, 79098 Freiburg',['1150061'],'Humboldtstr. 3, 79098 Freiburg im Breisgau','Schreibweise Strasse/Str.'),
 (10,'2025','Friedrich-Ebert-Anlage 51C, 69117 Heidelberg',['1046849','1046850'],'Friedrich-Ebert-Straße 51 C, 69117 Heidelberg','Anlage vs. Straße, Hausnummer identisch 51 C'),
 (13,'2025','Ritterstr. 20, 76133 Karlsruhe',['1118105'],'Ritterstr. 20, 76137 Karlsruhe','PLZ 76133 vs. 76137; Straße/Hausnummer/Ort identisch'),
 (15,'2025','Zirkel 2, 76131 Karlsruhe',['1062438'],'Zirkel 0, 76131 Karlsruhe','Hausnummer 2 vs. LSR 0; historischer LSR-Treffer Zirkel'),
 (16,'2025','Breisacher Str., 79106 Freiburg',['1061553','1061555','1061560','1061562','1061563','1152630','1152631','1152632','1152633','1152634','1152635','1152636'],'Breisacher Str. 0/113 B, 79106 Freiburg im Breisgau','UBA ohne Hausnummer; alle PBW-LSR-Treffer auf gleicher Straße/PLZ'),
 (19,'2025','Bismarckstraße 10, 68161 Mannheim',['1046862'],'Bismarckstr.aße 12, 68161 Mannheim','Hausnummer 10 vs. 12 und LSR-Schreibfehler str.aße'),
 (20,'2025','Katharinenstrasse, 79104 Freiburg',['1062400'],'Katharinenstr. 0, 79104 Freiburg im Breisgau','UBA ohne Hausnummer; einziger PBW-LSR-Treffer auf Straße/PLZ'),
 (27,'2025','Oskar-Schlemmer-Straße 8A, 70191 Stuttgart',['1150067'],'Oskar-Schlemmer-Str. 8 A, 70191 Stuttgart','Schreibweise 8A/8 A'),
 (30,'2025','Otto-Sander-Strasse 1, 70599 Stuttgart',['1046858'],'Otto-Sander-Straße 0, 70599 Stuttgart','Hausnummer 1 vs. LSR 0; Straße/PLZ eindeutig'),
 (34,'2025','Schlossplatz 12, 76131 Karlsruhe',['1061574','1061575','1061576','1061577','1061578','1061579','1061580','1061581','1061582','1046842','1046843','1046844'],'Schlossplatz 16 / Schloßplatz 0, 76131 Karlsruhe','Hausnummer 12 vs. LSR 16/0; gleicher Platz/PLZ'),
 (42,'2025','Albert-Einstein-Allee 5, 89081 Ulm',['1062309','1062310','1062311','1062312','1062313'],'Albert-Einstein-Allee 0, 89081 Ulm','Hausnummer 5 vs. LSR 0; Straße/PLZ eindeutig'),
 (46,'2024','Engelbergstraße 41, 79106 Freiburg',['1062396'],'Engelbergerstr. 41, 79106 Freiburg im Breisgau','Straßenname verkürzt/abweichend, Hausnummer identisch'),
 (49,'2024','Fritz-Haber-Weg, 76131 Karlsruhe',['1062427'],'Fritz-Haber-Weg 0, 76131 Karlsruhe','UBA ohne Hausnummer; einziger PBW-LSR-Treffer auf Straße/PLZ'),
 (50,'2024','Rudolf-Plank-Straße, 76131 Karlsruhe',['1062430','1062432'],'Rudolf-Plank-Str. 0, 76131 Karlsruhe','UBA ohne Hausnummer; PBW-LSR-Treffer auf Straße/PLZ'),
 (65,'2024','Breisacher Straße, 79106 Freiburg im Breisgau',['1061553','1061555','1061560','1061562','1061563','1152630','1152631','1152632','1152633','1152634','1152635','1152636'],'Breisacher Str. 0/113 B, 79106 Freiburg im Breisgau','UBA ohne Hausnummer; alle PBW-LSR-Treffer auf gleicher Straße/PLZ'),
 (68,'2024','Katharinenstraße, 79104 Freiburg',['1062400'],'Katharinenstr. 0, 79104 Freiburg im Breisgau','UBA ohne Hausnummer; einziger PBW-LSR-Treffer auf Straße/PLZ'),
 (75,'2024','Am Schlossplatz 1-3, 76131 Karlsruhe',['1061574','1061575','1061576','1061577','1061578','1061579','1061580','1061581','1061582','1046842','1046843','1046844'],'Schlossplatz 16 / Schloßplatz 0, 76131 Karlsruhe','Am Schlossplatz 1-3 vs. LSR 16/0; gleicher Platz/PLZ'),
 (89,'2024','Albert-Einstein-Allee, 89081 Ulm',['1062309','1062310','1062311','1062312','1062313'],'Albert-Einstein-Allee 0, 89081 Ulm','UBA ohne Hausnummer; PBW-LSR-Treffer auf Straße/PLZ'),
]
still = [
 (2,'2025','Schloß 4, 97980 Bad Mergentheim','kein plausibler LSR-Treffer; nur Bahnhofplatz 0 in PLZ'),
 (4,'2025','Kaiserallee 1, 76530 Baden-Baden','kein plausibler LSR-Treffer auf Straße; nur Friedrichstr./Friedenstr. in PLZ'),
 (41,'2025','Seidenstraße 23, 70176 Stuttgart','kein plausibler LSR-Treffer; nur Breitscheidstr. 0/Jobstweg in PLZ'),
 (45,'2024','Schloß 4, 97980 Bad Mergentheim','kein plausibler LSR-Treffer; nur Bahnhofplatz 0 in PLZ'),
 (58,'2024','Max-Egon-Straße 18, 78166 Donaueschingen','kein plausibler LSR-Treffer; nur Irmastraße/Bahnhofstr. in PLZ'),
 (61,'2024','Hubertus-Liebrecht-Straße 35, 88400 Biberach an der Riß','kein plausibler LSR-Treffer; nur Aspachstr./Karlstr. in PLZ'),
 (79,'2024','Kinzigstraße 8, 77652 Offenburg','kein plausibler LSR-Treffer; nur Badstr. 24 in PLZ'),
 (85,'2024','Seidenstraße 23, 70176 Stuttgart','kein plausibler LSR-Treffer; nur Breitscheidstr. 0/Jobstweg in PLZ'),
]
with MANUAL.open('w',encoding='utf-8',newline='') as f:
 w=csv.writer(f,delimiter=';'); w.writerow(['thg_row','jahr','adresse_uba','ladeeinrichtungs_id','adresse_lsr','manual_note'])
 for row,year,uba,ids,lsr,note in manual:
  for sid in ids: w.writerow([row,year,uba,sid,lsr,note])
with REMAIN.open('w',encoding='utf-8',newline='') as f:
 w=csv.writer(f,delimiter=';'); w.writerow(['thg_row','jahr','adresse_uba','grund'])
 w.writerows(still)
updates=0
for row,year,uba,ids,lsr,note in manual:
 for sid in ids:
  path=sid_path(sid); path.parent.mkdir(parents=True,exist_ok=True)
  meta={}
  if path.exists():
   try: meta=json.loads(path.read_text(encoding='utf-8'))
   except Exception: meta={}
  years=meta.get('thg_certificate_years',[])
  if not isinstance(years,list): years=[]
  meta['thg_certificate_years']=sorted({str(y) for y in years if str(y).strip()}|{year})
  meta['thg_certificate_source']=SOURCE
  mm=meta.get('thg_certificate_manual_matches',[])
  if not isinstance(mm,list): mm=[]
  rec={'thg_row':row,'jahr':year,'adresse_uba':uba,'adresse_lsr':lsr,'note':note}
  if rec not in mm: mm.append(rec)
  meta['thg_certificate_manual_matches']=mm
  path.write_text(json.dumps(meta,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
  updates+=1
summary=json.loads(SUMMARY.read_text(encoding='utf-8'))
summary['manual_matched_rows']=len(manual)
summary['manual_matched_links']=sum(len(x[3]) for x in manual)
summary['still_unmatched_rows']=len(still)
summary['manual_matches_csv']=str(MANUAL.relative_to(ROOT))
summary['still_unmatched_csv']=str(REMAIN.relative_to(ROOT))
summary['meta_manual_updates_written']=updates
SUMMARY.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(summary,ensure_ascii=False,indent=2))
