"""Application services that adapt validated S07-004.1 behavior into Core v0.2."""
from __future__ import annotations

from pathlib import Path
import json
from . import __version__
from .adapters.s070041 import multi_lane


def replay(root: Path, lane_definitions: list[str], *, batch=128, max_pending=128,
           max_backlog=1024, max_rounds: int | None = None) -> dict:
    if not lane_definitions or min(batch,max_pending,max_backlog) < 1:
        raise ValueError('At least one lane and positive capacities are required')
    if max_rounds is not None and max_rounds < 1:
        raise ValueError('max_rounds must be positive')
    root = Path(root)
    sources = multi_lane.parse_sources(lane_definitions)
    summary = multi_lane.run(root,sources,batch=batch,max_pending=max_pending,
                             max_backlog=max_backlog,max_rounds=max_rounds)
    summary['core_version'] = __version__
    summary['basis'] = 'S07-004.1 frozen semantics; no live venue source or external acknowledgement'
    summary['complete'] = all(v['file_exhausted'] and v['raw_unprocessed'] == 0 and
                              v['pending'] == 0 and v['integrity'] != 'CONFLICT'
                              for v in summary['lanes'].values())
    (root/'INGEST_REPORT.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    return summary
