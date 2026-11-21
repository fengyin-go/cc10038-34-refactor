"""批次原文映射重构后的行为测试。

覆盖：
- 原有批次与新批次对照正文及行号，旧批次原文引用继续可用；
- 全链路与直接启动共用同一份批次交接定义，字段/状态语义不变；
- 启动与解析并发（第二条原文到达前先触发解析）时，只把身份完整的集合交给
  一次标准化，已存在编号仍能定位原文，并核对实际消费记录数与原文来源；
- 中断恢复沿用既有引用，不重新编号。
"""
from __future__ import annotations

import pytest

from app.store import store
from app.services.batch_text import (
    BatchHandoff,
    batch_text_index,
)


@pytest.fixture(autouse=True)
def reset_open_batches():
    """重置在途批次登记；raw_records 等表由 conftest 的 reset_store 负责。"""
    batch_text_index._open_batches.clear()
    yield
    batch_text_index._open_batches.clear()


def _prepare_source(pipeline, source_id: str) -> None:
    pipeline.register_source(source_id, "http", "default")
    pipeline.validate_source(source_id)


class TestBatchTextMapping:
    """批次原文映射：正文、行号、记录编号对应关系。"""

    def test_new_batch_mapping_matches_input_bodies_and_lines(self, pipeline, sample_raw_logs):
        _prepare_source(pipeline, "map-src")
        result = pipeline.start_ingest_batch("map-src", "2026Q1", sample_raw_logs)
        batch_no = result["batch"]["批次编号"]

        lines = batch_text_index.lines_for(batch_no)
        assert [int(r["原始行号"]) for r in lines] == [1, 2, 3, 4, 5]
        assert [r["原始正文"] for r in lines] == [log["body"] for log in sample_raw_logs]
        assert batch_text_index.record_count(batch_no) == 5

    def test_old_batch_text_reference_still_resolvable_after_close(self, pipeline, sample_raw_logs):
        _prepare_source(pipeline, "old-ref-src")
        result = pipeline.start_ingest_batch("old-ref-src", "2026Q1", sample_raw_logs[:2])
        batch_no = result["batch"]["批次编号"]
        first_ids = [r["id"] for r in batch_text_index.lines_for(batch_no)]

        pipeline.parse_batch(batch_no)
        # 解析成功后批次关闭，但历史原文映射必须继续可读。
        assert batch_text_index.is_open(batch_no) is False
        lines = batch_text_index.lines_for(batch_no)
        assert len(lines) == 2
        for record_id in first_ids:
            row = batch_text_index.get_line(record_id)
            identity = batch_text_index.line_identity(record_id)
            assert row is not None and row["批次编号"] == batch_no
            assert identity is not None
            assert identity.batch_no == batch_no
            assert identity.record_id == record_id
            assert 1 <= identity.line_no <= 2

    def test_old_and_new_batch_share_identity_semantics(self, pipeline, sample_raw_logs):
        """原有批次与新批次逐条对照：行号口径一致、正文与输入一致、记录编号可定位。"""
        _prepare_source(pipeline, "cmp-src-a")
        old_batch = pipeline.start_ingest_batch("cmp-src-a", "2026Q1", sample_raw_logs[:3])["batch"]["批次编号"]
        pipeline.parse_batch(old_batch)  # 旧批次：已完成

        _prepare_source(pipeline, "cmp-src-b")
        new_batch = pipeline.start_ingest_batch("cmp-src-b", "2026Q1", sample_raw_logs[:3])["batch"]["批次编号"]

        old_lines = batch_text_index.lines_for(old_batch)
        new_lines = batch_text_index.lines_for(new_batch)
        assert len(old_lines) == len(new_lines) == 3
        for old, new, log in zip(old_lines, new_lines, sample_raw_logs[:3]):
            assert old["原始行号"] == new["原始行号"]
            assert old["原始正文"] == new["原始正文"] == log["body"]
            assert set(old.keys()) == set(new.keys())
            # 已存在的编号在两个批次上都能定位回原文
            assert batch_text_index.get_line(old["id"])["原始正文"] == log["body"]
            assert batch_text_index.get_line(new["id"])["原始正文"] == log["body"]


class TestSharedHandoff:
    """全链路与直接启动共用一份批次交接定义。"""

    def test_direct_and_full_pipeline_share_handoff_shape(self, pipeline, sample_raw_logs):
        _prepare_source(pipeline, "direct-src")
        direct = pipeline.start_ingest_batch("direct-src", "2026Q1", sample_raw_logs[:2])
        direct_handoff = batch_text_index.get_handoff(direct["batch"]["批次编号"])
        assert isinstance(direct_handoff, BatchHandoff)
        direct_keys = set(direct_handoff.as_dict().keys())

        full = pipeline.run_full_pipeline("full-hand-off-src", "2026Q1", sample_raw_logs[:2])
        full_batch_no = full["batch_no"]
        full_rows = batch_text_index.lines_for(full_batch_no)
        direct_rows = batch_text_index.lines_for(direct["batch"]["批次编号"])
        # 两条入口落库的原始行字段完全同构，状态语义一致
        assert set(full_rows[0].keys()) == set(direct_rows[0].keys())
        assert full_rows[0]["status"] == "已解析"  # 全链路已解析
        assert direct_rows[0]["status"] == "待解析"  # 直接启动尚未解析
        expected_keys = {
            "batch_no", "source_id", "time_window", "record_count", "status",
            "batch_version", "source_version", "started_at", "raw_record_ids",
        }
        assert direct_keys == expected_keys

    def test_public_io_unchanged_shape(self, pipeline, sample_raw_logs):
        _prepare_source(pipeline, "io-shape-src")
        result = pipeline.start_ingest_batch("io-shape-src", "2026Q1", sample_raw_logs[:2])
        assert set(result.keys()) == {"ok", "message", "batch", "record_count"}
        assert result["record_count"] == 2
        batch = result["batch"]
        assert batch["status"] == "采集中"
        assert batch["记录总数"] == "2"
        # 旧引用字段保留
        import json
        assert len(json.loads(batch["raw_record_ids"])) == 2


class TestConcurrentStartAndParse:
    """启动与解析并发：原文未收齐时解析入口已收到批号。"""

    def test_parse_before_second_line_consumes_only_complete_set(self, pipeline):
        _prepare_source(pipeline, "concurrent-src")
        first_log = {"body": "2026-01-15T10:30:00|INFO|first line"}
        started = pipeline.start_ingest_batch("concurrent-src", "2026Q1", [first_log])
        batch_no = started["batch"]["批次编号"]
        assert batch_text_index.is_open(batch_no)

        # 第二条原文尚未到达，先触发解析
        first_parse = pipeline.parse_batch(batch_no)
        assert first_parse["ok"] is True
        assert first_parse["success"] == 1
        assert first_parse["failure"] == 0

        # 核对实际消费的记录数量与原文来源
        parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(parsed) == 1
        consumed_id = int(parsed[0]["原始记录ID"])
        source_line = batch_text_index.get_line(consumed_id)
        assert source_line["原始正文"] == first_log["body"]
        assert source_line["原始行号"] == "1"

        # 第二条原文到达：行号接续为 2，不重新编号
        second_log = {"body": "2026-01-15T10:31:00|WARN|second line"}
        ingest = pipeline.ingest_raw_lines(batch_no, [second_log])
        assert ingest["ok"] is True
        assert ingest["record_count"] == 2
        lines = batch_text_index.lines_for(batch_no)
        assert [int(r["原始行号"]) for r in lines] == [1, 2]
        assert lines[1]["原始正文"] == second_log["body"]

        # 再次标准化只消费尚未处理的第二条，不产生重复
        pending = batch_text_index.pending_lines_for(batch_no)
        assert len(pending) == 1
        assert pending[0]["原始行号"] == "2"
        second_parse = pipeline.parse_batch(batch_no)
        assert second_parse["success"] == 1
        all_parsed = [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]
        assert len(all_parsed) == 2
        assert {int(p["原始记录ID"]) for p in all_parsed} == {lines[0]["id"], lines[1]["id"]}

    def test_identity_incomplete_rows_are_not_standardized(self, pipeline):
        _prepare_source(pipeline, "partial-src")
        started = pipeline.start_ingest_batch(
            "partial-src", "2026Q1", [{"body": "t1|INFO|complete"}]
        )
        batch_no = started["batch"]["批次编号"]
        complete_id = batch_text_index.lines_for(batch_no)[0]["id"]

        # 模拟并发下尚未收齐的残缺行（缺正文、行号非数字）
        store.rows("raw_records").append({
            "id": 999001,
            "批次编号": batch_no,
            "原始行号": "pending",
            "采集时间戳": "",
            "解析状态": "未解析",
            "status": "待解析",
            "pending": True,
            "abnormal": False,
        })
        store.rows("raw_records").append({
            "id": 999002,
            "批次编号": batch_no,
            "原始行号": "3",
            "采集时间戳": "",
            "解析状态": "未解析",
            "status": "待解析",
            "pending": True,
            "abnormal": False,
        })

        lines = batch_text_index.lines_for(batch_no)
        assert [r["id"] for r in lines] == [complete_id]
        result = pipeline.parse_batch(batch_no)
        assert result["success"] == 1
        # 已存在的完整编号仍可定位原文
        assert batch_text_index.get_line(complete_id)["status"] == "已解析"

    def test_ingest_continuously_appends_line_numbers(self, pipeline, sample_raw_logs):
        _prepare_source(pipeline, "append-src")
        started = pipeline.start_ingest_batch("append-src", "2026Q1", sample_raw_logs[:2])
        batch_no = started["batch"]["批次编号"]
        more = pipeline.ingest_raw_lines(batch_no, sample_raw_logs[2:4])
        assert more["received"] == 2
        lines = batch_text_index.lines_for(batch_no)
        assert [int(r["原始行号"]) for r in lines] == [1, 2, 3, 4]
        assert [r["原始正文"] for r in lines] == [log["body"] for log in sample_raw_logs[:4]]


class TestRecoverKeepsReferences:
    """中断恢复沿用原先的引用。"""

    def test_recover_does_not_renumber_or_recreate(self, pipeline):
        import json
        _prepare_source(pipeline, "recover-map-src")
        logs = [
            {"body": "t1|INFO|first"},
            {"body": "t2|WARN|second"},
        ]
        started = pipeline.start_ingest_batch("recover-map-src", "2026Q1", logs)
        batch_no = started["batch"]["批次编号"]
        before_ids = {r["id"]: r["原始行号"] for r in batch_text_index.lines_for(batch_no)}
        target_id = max(before_ids)

        # 模拟中断现场：第二条解析失败并进入重试队列（引用的仍是原记录编号）。
        failed_row = batch_text_index.get_line(target_id)
        failed_row["status"] = "解析失败"
        failed_row["解析状态"] = "解析失败"
        failed_row["abnormal"] = True
        failed_row["pending"] = True
        store.rows("retry_queue").append({
            "id": 880001,
            "失败记录ID": str(target_id),
            "失败原因": "字段解析失败",
            "重试次数": "0",
            "最大重试": "3",
            "下次重试时间": "2026-01-15T10:00:00",
            "status": "待重试",
            "pending": True,
            "abnormal": False,
            "batch_no": batch_no,
        })

        result = pipeline.recover_batch(batch_no)
        assert result["ok"] is True
        assert result["recovered"] == 1

        after = batch_text_index.lines_for(batch_no)
        after_ids = {r["id"]: r["原始行号"] for r in after}
        assert before_ids == after_ids  # 编号与行号引用完全不变，未重建原文
        recovered_row = batch_text_index.get_line(target_id)
        assert recovered_row["原始行号"] == before_ids[target_id]
        assert recovered_row["status"] == "待解析"  # 恢复后沿用原记录编号
        other_row = batch_text_index.get_line(min(before_ids))
        assert other_row["原始正文"] == "t1|INFO|first"
        # 旧批次引用字段仍保留
        batch_entry = next(b for b in store.rows("ingest_batch") if b["批次编号"] == batch_no)
        assert json.loads(batch_entry["raw_record_ids"]) == sorted(before_ids)
