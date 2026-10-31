"""Exercise installed-wheel governance and evidence outside the source tree."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import actionlens as al
from actionlens.exporters import export_evidence_bundle


class AllowArtifacts:
    def authorize(self, artifact, *, context):
        return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    assert al.__version__ == args.version
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        lens = al.ActionLens(
            storage_dir=root / "store", artifact_authorizer=AllowArtifacts()
        )
        calls = []

        @lens.tool(
            risk="MUTATION", idempotency="REQUIRED", approval_required=True, max_bytes=8
        )
        def write(value: str):
            calls.append(value)
            return value * 20

        context = al.ToolCallContext(
            project="wheel",
            environment="test",
            tenant_id="tenant",
            actor_id="actor",
            session_id="s",
            run_id="r",
            call_id="c",
            tool_name="write",
        )
        try:
            pending = write("payload", idempotency_key="wheel", __al_ctx=context)
            assert pending.status == "PENDING_APPROVAL"
            lens.approve(ticket_id=pending.result["ticket_id"], approved_by="reviewer")
            output = write("payload", idempotency_key="wheel", __al_ctx=context)
            assert output.status == "SUCCESS"
            assert (
                write("payload", idempotency_key="wheel", __al_ctx=context).status
                == "SUCCESS"
            )
            assert calls == ["payload"]
            assert lens.read_artifact(output.artifact_refs[0], context=context)
            lens.flush()
            export_evidence_bundle(root / "store", root / "bundle")
            rows = [
                json.loads(line)
                for line in (root / "bundle/events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            ref = next(row["output_ref"] for row in rows if row.get("output_ref"))
            assert lens.read_artifact(
                al.ArtifactRef.model_validate(ref), context=context
            )
        finally:
            lens.close()


if __name__ == "__main__":
    main()
