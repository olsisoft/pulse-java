#!/usr/bin/env python3
"""Prepare the explicitly supplied canonical Pulse guest SDK crate for examples.

This does not resolve crates.io or inspect a sibling product checkout. The guest
is a source SDK artifact consumed by Cargo, not historical implementation source.
"""
import argparse,hashlib,json,pathlib,re,tempfile,tarfile,shutil,tomllib
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--crate',type=pathlib.Path,required=True);p.add_argument('--sha256',required=True);p.add_argument('--source-head',required=True);p.add_argument('--source-tree',required=True);p.add_argument('--output',type=pathlib.Path,required=True);a=p.parse_args()
if not re.fullmatch(r'[0-9a-f]{64}',a.sha256) or not all(re.fullmatch(r'[0-9a-f]{40}',v) for v in [a.source_head,a.source_tree]):p.error('Explicit qualified candidate SHA256 and HEAD/tree required')
archive=a.crate.resolve();out=a.output.resolve()
if hashlib.sha256(archive.read_bytes()).hexdigest()!=a.sha256:raise SystemExit('Canonical guest package digest mismatch')
if out.exists() and any(out.iterdir()):raise SystemExit('Output must be fresh/empty; old sibling Source is never a fallback')
out.parent.mkdir(parents=True,exist_ok=True)
with tempfile.TemporaryDirectory(prefix='canonical-pulse-guest-',dir=out.parent) as td:
 stage=pathlib.Path(td);records=[]
 with tarfile.open(archive,'r:gz') as tar:
  members=tar.getmembers();roots=set();names=set()
  for member in members:
   part=pathlib.PurePosixPath(member.name)
   if part.is_absolute() or '..' in part.parts or not part.parts or member.issym() or member.islnk() or not (member.isdir() or member.isfile()):raise SystemExit('Unsafe guest package member')
   roots.add(part.parts[0])
   if member.name in names:raise SystemExit('Duplicate guest package member')
   names.add(member.name)
  if roots!={'pulse-wasm-guest-0.1.0'}:raise SystemExit('Expected canonical pulse-wasm-guest0.1.0 package')
  for member in members:
   rel=pathlib.PurePosixPath(member.name).relative_to('pulse-wasm-guest-0.1.0')
   if member.isdir():continue
   file=tar.extractfile(member)
   if file is None:raise SystemExit('Unreadable guest package member')
   data=file.read();dst=stage/str(rel);dst.parent.mkdir(parents=True,exist_ok=True);dst.write_bytes(data);records.append({'path':str(rel),'sha256':hashlib.sha256(data).hexdigest()})
 package=tomllib.loads((stage/'Cargo.toml').read_text())
 if package['package']['name']!='pulse-wasm-guest' or package['package']['version']!='0.1.0':raise SystemExit('Guest package identity mismatch')
 if package.get('dependencies'):raise SystemExit('Canonical guest SDK must remain dependency-free')
 vcs=stage/'.cargo_vcs_info.json'
 if vcs.exists():
  info=json.loads(vcs.read_text())
  if info.get('git',{}).get('sha1')!=a.source_head or info.get('path_in_vcs')!='pulse-wasm-guest':raise SystemExit('Guest package candidate source identity mismatch')
 manifest={'schema':1,'source_repository':'olsisoft/pulse','source_head':a.source_head,'source_tree':a.source_tree,'crate_sha256':a.sha256,'package':'pulse-wasm-guest','version':'0.1.0','members':records,'scope':'Explicit public guest SDK package; no source checkout/registry substitution'}
 (stage/'intake-source.json').write_text(json.dumps(manifest,indent=2)+'\n')
 if out.exists():out.rmdir()
 shutil.copytree(stage,out)
print(json.dumps({'result':'PASS','members':len(records),'source_head':a.source_head,'source_tree':a.source_tree,'crate_sha256':a.sha256,'output':str(out)}))
