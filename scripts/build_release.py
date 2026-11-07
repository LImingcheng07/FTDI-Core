"""Build a reproducible source ZIP with per-file SHA256; excludes market data and state."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import zipfile

ROOT=Path(__file__).resolve().parents[1]
DEST=ROOT.parent/'FTDI-Core-v0.2-local-development-windows.zip'
EXCLUDE_DIRS={'.git','__pycache__','.pytest_cache','.venv','runs','data'}

def main():
    paths=[]
    for p in sorted(ROOT.rglob('*')):
        if not p.is_file() or any(x in EXCLUDE_DIRS for x in p.relative_to(ROOT).parts):
            continue
        if p.suffix in ('.pyc','.pyo') or p.name in ('PACKAGE_MANIFEST.json',):
            continue
        paths.append(p)
    manifest={'release':'FTDI-Core-v0.2','source_data_included':False,
              'source_zip_sha256_expected_when_user_supplies_archival_fixture':
              '78085fc50146fcd2a228bb95495246c539bacc5cd635268bcbb8a641ce90e3c9',
              'files':{str(p.relative_to(ROOT)).replace('\\','/'):{
                  'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in paths}}
    manifest_path=ROOT/'PACKAGE_MANIFEST.json'
    manifest_path.write_text(json.dumps(manifest,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    with zipfile.ZipFile(DEST,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as z:
        for p in [*paths,manifest_path]:
            name=f'{ROOT.name}/'+str(p.relative_to(ROOT)).replace('\\','/')
            zi=zipfile.ZipInfo(name,date_time=(2026,11,6,0,0,0))
            zi.compress_type=zipfile.ZIP_DEFLATED
            zi.external_attr=(0o644<<16)
            z.writestr(zi,p.read_bytes())
    with zipfile.ZipFile(DEST) as z:
        bad=z.testzip()
        if bad:raise ValueError(f'Corrupt packaged member: {bad}')
    print(json.dumps({'zip':str(DEST),'bytes':DEST.stat().st_size,'files':len(paths)+1,
                      'sha256':hashlib.sha256(DEST.read_bytes()).hexdigest()},indent=2))

if __name__=='__main__':main()
