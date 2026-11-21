"""批次原文映射：收拢采集批次从启动到解析读取之间的编号与正文对应关系。

设计要点
--------
1. 原始行的唯一事实来源仍是 ``raw_records`` 表；本模块在其上维护一份
   “批次 → 原文行（行号/正文/记录编号）”的映射视图，批号与行号保持原有口径。
2. 全链路流水线与直接启动批次共用 :class:`BatchHandoff` 这一份交接定义，
   原始行只允许通过 :meth:`BatchTextIndex.add_line` 落库，写法统一、字段统一。
3. 解析与启动可能并发：批次已登记（解析入口已能收到批号）但原始行尚未收齐时，
   :meth:`BatchTextIndex.lines_for` 只交出身份完整（记录编号、批号、行号、正文
   四要素齐备）的行；不完整的行不会进入标准化处理，而已存在的记录编号始终可以
   通过 :meth:`BatchTextIndex.get_line` 定位回原文，旧批次引用继续可用。
4. 解析等处置结果通过 :meth:`BatchTextIndex.mark_parse_outcome` 回写到解析来源
   清单（``raw_records`` 的状态字段），状态语义与原实现一致。
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _next_raw_id() -> int:
    return max((int(r.get("id", 0)) for r in store.rows("raw_records")), default=0) + 1


@dataclass(frozen=True)
class RawLineIdentity:
    """原始行身份：记录编号 + 批号 + 行号，可唯一定位一条原文。"""

    record_id: int
    batch_no: str
    line_no: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "batch_no": self.batch_no,
            "line_no": self.line_no,
        }


@dataclass(frozen=True)
class BatchHandoff:
    """批次交接定义：全链路流水线与直接启动共用同一份结构与写法。"""

    batch_no: str
    source_id: str
    time_window: str
    record_count: int = 0
    status: str = "采集中"
    batch_version: int = 1
    source_version: int = 1
    started_at: str = field(default_factory=_now_iso)
    raw_record_ids: tuple[int, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_no": self.batch_no,
            "source_id": self.source_id,
            "time_window": self.time_window,
            "record_count": self.record_count,
            "status": self.status,
            "batch_version": self.batch_version,
            "source_version": self.source_version,
            "started_at": self.started_at,
            "raw_record_ids": list(self.raw_record_ids),
        }


class BatchTextIndex:
    """批次原文映射：管理批号 ↔ 原始行号 ↔ 正文 ↔ 记录编号的对应关系。"""

    def __init__(self) -> None:
        # 已登记但可能仍在接收原文的批次（启动与解析并发时的在途交接信息）。
        self._open_batches: dict[str, BatchHandoff] = {}

    # ------------------------------------------------------------------
    # 批次交接
    # ------------------------------------------------------------------
    def register_open_batch(self, handoff: BatchHandoff) -> BatchHandoff:
        """登记一个已启动、可能仍在接收原文的批次交接。"""
        self._open_batches[handoff.batch_no] = handoff
        return handoff

    def get_handoff(self, batch_no: str) -> BatchHandoff | None:
        return self._open_batches.get(batch_no)

    def is_open(self, batch_no: str) -> bool:
        """批次是否仍处在可接收原文的在途状态。"""
        return batch_no in self._open_batches

    def close_batch(self, batch_no: str) -> None:
        """批次进入终态后移出在途登记；历史行仍可经映射继续读取。"""
        self._open_batches.pop(batch_no, None)

    # ------------------------------------------------------------------
    # 原始行身份判定
    # ------------------------------------------------------------------
    @staticmethod
    def _is_identity_complete(row: dict[str, Any]) -> bool:
        """原始行身份是否完整：记录编号、批号、数字行号、正文四要素齐备。"""
        if row.get("id") is None or not str(row.get("批次编号", "")).strip():
            return False
        line_no = str(row.get("原始行号", "")).strip()
        if not line_no.isdigit() or int(line_no) <= 0:
            return False
        if "原始正文" not in row:
            return False
        return True

    # ------------------------------------------------------------------
    # 原文接收（唯一落库入口，全链路与直接启动共用）
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_body(log: dict[str, Any]) -> str:
        return log.get("body", str(log))

    def next_line_no(self, batch_no: str) -> int:
        """返回批次下一条原文应使用的行号（原有行号从 1 开始连续编号）。"""
        line_nos = [
            int(str(r["原始行号"]))
            for r in store.rows("raw_records")
            if r.get("批次编号") == batch_no and str(r.get("原始行号", "")).isdigit()
        ]
        return max(line_nos, default=0) + 1

    def add_line(
        self,
        batch_no: str,
        log: dict[str, Any],
        *,
        source_id: str,
        line_no: int | None = None,
    ) -> RawLineIdentity:
        """把一条原文按统一字段定义写入解析来源清单，并登记进批次原文映射。"""
        if line_no is None:
            line_no = self.next_line_no(batch_no)
        record_id = _next_raw_id()
        now = _now_iso()
        record = {
            "id": record_id,
            "批次编号": batch_no,
            "原始行号": str(line_no),
            "采集时间戳": now,
            "原始正文": self._extract_body(log),
            "解析状态": "未解析",
            "status": "待解析",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_id": source_id,
            "ingested_at": now,
        }
        store.rows("raw_records").append(record)
        return RawLineIdentity(record_id=record_id, batch_no=batch_no, line_no=line_no)

    def ingest_lines(
        self,
        batch_no: str,
        logs: list[dict[str, Any]],
        source_id: str,
    ) -> list[RawLineIdentity]:
        """批量接收原文，行号在批次内连续接续。"""
        identities: list[RawLineIdentity] = []
        for log in logs or []:
            identities.append(self.add_line(batch_no, log, source_id=source_id))
        return identities

    # ------------------------------------------------------------------
    # 映射读取（解析接续读取走这里）
    # ------------------------------------------------------------------
    def lines_for(self, batch_no: str) -> list[dict[str, Any]]:
        """返回批次下身份完整的原文行，按原始行号升序。

        并发场景下尚未收齐、身份不完整的行不会出现在结果里，因此标准化处理
        每次只消费一个身份完整的集合。
        """
        lines = [
            row
            for row in store.rows("raw_records")
            if row.get("批次编号") == batch_no and self._is_identity_complete(row)
        ]
        lines.sort(key=lambda r: int(str(r["原始行号"])))
        return lines

    def pending_lines_for(self, batch_no: str) -> list[dict[str, Any]]:
        """返回身份完整且尚未标准化的原文行（一次标准化只处理这批）。"""
        return [
            row
            for row in self.lines_for(batch_no)
            if row.get("status") == "待解析"
        ]

    def get_line(self, record_id: int) -> dict[str, Any] | None:
        """按记录编号定位原文；已存在的编号（含旧批次）始终可定位。"""
        return store.find("raw_records", record_id)

    def line_identity(self, record_id: int) -> RawLineIdentity | None:
        row = self.get_line(record_id)
        if row is None or not self._is_identity_complete(row):
            return None
        return RawLineIdentity(
            record_id=int(row["id"]),
            batch_no=str(row["批次编号"]),
            line_no=int(str(row["原始行号"])),
        )

    def record_count(self, batch_no: str) -> int:
        return len(self.lines_for(batch_no))

    # ------------------------------------------------------------------
    # 处置结果回写解析来源清单
    # ------------------------------------------------------------------
    def mark_parse_outcome(
        self,
        success_ids: list[int],
        failed_ids: list[int],
    ) -> None:
        """把解析处置结果回写到 raw_records（解析来源清单），状态语义不变。"""
        for record_id in success_ids:
            row = self.get_line(record_id)
            if row is None:
                continue
            row["status"] = "已解析"
            row["pending"] = False
            row["解析状态"] = "已解析"
        for record_id in failed_ids:
            row = self.get_line(record_id)
            if row is None:
                continue
            row["status"] = "解析失败"
            row["解析状态"] = "解析失败"
            row["abnormal"] = True

    def reset_for_retry(self, record_id: int) -> None:
        """恢复重试时把来源清单行置回待解析（恢复后沿用原引用）。"""
        row = self.get_line(record_id)
        if row is None:
            return
        row["status"] = "待解析"
        row["解析状态"] = "待解析"
        row["pending"] = True
        row["abnormal"] = False


# 单例：流水线、路由与测试共用同一份批次原文映射。
batch_text_index = BatchTextIndex()
