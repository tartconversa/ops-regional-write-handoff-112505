#!/usr/bin/env python3
"""本地双地域切流演练的有状态运维控制程序。

仅使用 Python 标准库。核心类 :class:`ControlPlane` 可直接被测试或脚本调用；
命令行入口见 ``python duty_ctl.py --help``。

模型要点：
* 分组约束一次写涉及的全部分片，回执必须包含整组 offsets；
* 无游标历史读按分片比较 min_history，带游标读按回执 offsets 比较 applied；
* fenced 才是旧写权失效证据，probe_down 不是；
* 所有状态和证据写入 JSON 状态文件，进程重启后可继续。
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import tempfile
import time
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable

REGIONS = ("east", "west")
REGION_CN = {"east": "东区", "west": "西区"}
SUBJECT_WRITE = "write"
WRITE_TERMINAL_OK = {"committed"}
WRITE_TERMINAL_NO_EFFECT = {"no_effect", "rejected"}


class ControlError(ValueError):
    """输入或当前状态不允许该控制操作。"""

    def __init__(self, message: str, code: str = "invalid_request", details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


def group_key(tenant: str, group: str) -> str:
    return f"{tenant}/{group}"


def opposite(region: str) -> str:
    if region == "east":
        return "west"
    if region == "west":
        return "east"
    raise ControlError(f"未知地域：{region!r}", "unknown_region")


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _fresh_state(run_id: str | None = None) -> dict[str, Any]:
    now = int(time.time() * 1000)
    return {
        "version": 1,
        "run_id": run_id or f"run-{uuid.uuid4().hex[:12]}",
        "created_ms": now,
        "updated_ms": now,
        "clock_ms": 0,
        "groups": {},
        "shards": {},
        "authority": {},
        "transitions": {},
        "ops": {},
        "requests": {},
        "evidence": [],
        "journal": [],
        "decisions": [],
        "counters": {"seq": 0},
    }


class ControlPlane:
    """切流证据、操作账本和读写路由的一致性状态机。"""

    def __init__(self, path: str | os.PathLike | None = None, state: dict | None = None):
        self.path = Path(path) if path else None
        self._lock_fh = None
        self._state_explicit = state is not None
        if state is not None:
            self.state = state
        elif self.path and self.path.exists():
            with self.path.open("r", encoding="utf-8") as stream:
                self.state = json.load(stream)
            if not isinstance(self.state, dict) or self.state.get("version") != 1:
                raise ControlError("状态文件格式不受支持", "bad_state")
        else:
            self.state = _fresh_state()

    # ---------- 持久化与基础工具 ----------

    def lock(self) -> None:
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self._lock_fh = lock_path.open("a+", encoding="utf-8")
        fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_EX)
        # 锁可能在 __init__ 之后才获取；必须重新读取，避免覆盖其他进程刚提交的状态。
        if not self._state_explicit and self.path.exists():
            with self.path.open("r", encoding="utf-8") as stream:
                latest = json.load(stream)
            if isinstance(latest, dict) and latest.get("version") == 1:
                self.state = latest

    def unlock(self) -> None:
        if self._lock_fh is not None:
            with contextlib.suppress(Exception):
                fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
            self._lock_fh.close()
            self._lock_fh = None

    def save(self) -> None:
        if not self.path:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state["updated_ms"] = int(time.time() * 1000)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(self.state, stream, ensure_ascii=False, indent=2, sort_keys=False)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp_name, self.path)
        except Exception:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_name)
            raise

    def __enter__(self) -> "ControlPlane":
        self.lock()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self.save()
        finally:
            self.unlock()

    def _next_seq(self) -> int:
        self.state["counters"]["seq"] += 1
        return int(self.state["counters"]["seq"])

    def _stamp(self, at: int | None, row: dict | None = None) -> int:
        if at is not None:
            ms = int(at)
        elif row is not None and row.get("observed_ms") is not None:
            ms = int(row["observed_ms"])
        else:
            ms = int(self.state.get("clock_ms", 0)) + 1
        self.state["clock_ms"] = max(int(self.state.get("clock_ms", 0)), ms)
        return ms

    def _journal(self, kind: str, row: dict, ms: int, seq: int | None = None) -> int:
        seq = seq if seq is not None else self._next_seq()
        self.state["journal"].append(
            {"seq": seq, "observed_ms": ms, "kind": kind, "record": deepcopy(row)}
        )
        return seq

    def _group(self, tenant: str, group: str) -> dict:
        key = group_key(tenant, group)
        try:
            return self.state["groups"][key]
        except KeyError as exc:
            known = ", ".join(sorted(self.state["groups"])) or "(尚未加载分组)"
            raise ControlError(
                f"未知业务组 {key}；已知：{known}",
                "unknown_group",
                {"tenant": tenant, "group": group},
            ) from exc

    def _authority(self, tenant: str, group: str) -> dict:
        self._group(tenant, group)
        key = group_key(tenant, group)
        if key not in self.state["authority"]:
            # 只有分组、没有 opening 时的保守兜底；正常演练会先加载 opening。
            self.state["authority"][key] = {
                "epoch": None,
                "baseline_region": None,
                "write_route": None,
                "read_route": None,
                "highest_epoch": None,
            }
        return self.state["authority"][key]

    def _shard_bag(self, tenant: str, group: str) -> dict:
        key = group_key(tenant, group)
        return self.state["shards"].setdefault(key, {})

    def _op(self, tenant: str, op_id: str) -> dict | None:
        return self.state["ops"].get(f"{tenant}\u0000{op_id}")

    def _op_key(self, tenant: str, op_id: str) -> str:
        return f"{tenant}\u0000{op_id}"

    def _put_op(self, op: dict) -> None:
        self.state["ops"][self._op_key(op["tenant"], op["op_id"])] = op

    # ---------- 资料装入 ----------

    def handle(self, row: dict[str, Any], source: str | None = None, at: int | None = None) -> dict:
        """按记录结构分派 groups/opening/observations/traffic 中的一行。"""
        if not isinstance(row, dict):
            raise ControlError("记录必须是 JSON 对象", "not_object")
        if "members" in row and "min_history" in row and "write_nodes" in row:
            return self.load_group(row)
        if "confirmed_offset" in row and "east_applied" in row:
            return self.load_opening(row)
        event = row.get("event")
        if event:
            return self.record_observation(row, at=at)
        kind = row.get("kind")
        if kind == "adjudicate":
            return self.adjudicate_op(
                str(row["tenant"]),
                str(row["op_id"]),
                str(row.get("resolution")),
                note=row.get("note"),
                receipt_seq=row.get("receipt_seq"),
                at=at if at is not None else row.get("observed_ms"),
            )
        if kind == "reconcile":
            return self.reconcile_op(
                str(row["tenant"]),
                str(row["op_id"]),
                state_name=str(row.get("state", "pending_verification")),
                note=row.get("note"),
                at=at if at is not None else row.get("observed_ms"),
            )
        if kind in {"write", "read"}:
            return self.record_request(row, at=at)
        if kind in {"receipt", "no_effect", "rejected"}:
            return self.record_outcome(row, at=at)
        raise ControlError(f"无法识别的记录：{row!r}", "unknown_record")

    def load_group(self, row: dict[str, Any]) -> dict:
        tenant = str(row["tenant"])
        group = str(row["group"])
        key = group_key(tenant, group)
        members = list(row["members"])
        min_history = row["min_history"]
        write_nodes = row["write_nodes"]
        if not members or len(set(members)) != len(members):
            raise ControlError(f"{key}: members 必须是非空且不重复的有序分片集合", "bad_group")
        if set(min_history) != set(members):
            raise ControlError(f"{key}: min_history 必须逐分片给出且不能借用水位", "bad_group")
        for shard in members:
            if not _is_int(min_history[shard]):
                raise ControlError(f"{key}/{shard}: min_history 必须是整数", "bad_group")
        for region in REGIONS:
            nodes = write_nodes.get(region, [])
            if not isinstance(nodes, list) or not nodes:
                raise ControlError(f"{key}: write_nodes.{region} 必须是非空列表", "bad_group")
        self.state["groups"][key] = {
            "tenant": tenant,
            "group": group,
            "members": members,
            "min_history": {s: int(min_history[s]) for s in members},
            "write_nodes": {r: list(write_nodes[r]) for r in REGIONS},
        }
        self.state["shards"].setdefault(key, {s: {} for s in members})
        self._authority(tenant, group)
        seq = self._next_seq()
        ms = self._stamp(0)
        self._journal("group", row, 0, seq)
        return {"ok": True, "loaded": "group", "group": key, "seq": seq, "observed_ms": ms}

    def load_opening(self, row: dict[str, Any]) -> dict:
        tenant = str(row["tenant"])
        group = str(row.get("group", "jobs"))
        # opening 资料没有 group 字段；当前每个租户的初始分片按 groups 归属。
        # 若分片只属于一个组，自动解析；多组同分片时要求行内给 group。
        if "group" not in row:
            candidates = [
                g
                for g, spec in self.state["groups"].items()
                if row["tenant"] == spec["tenant"] and row["shard"] in spec["members"]
            ]
            if len(candidates) == 1:
                group = candidates[0].split("/", 1)[1]
            elif candidates:
                raise ControlError(
                    f"分片 {row['tenant']}/{row['shard']} 属于多个组，opening 行需要 group",
                    "ambiguous_group",
                )
        key = group_key(tenant, group)
        spec = self._group(tenant, group)
        shard = str(row["shard"])
        if shard not in spec["members"]:
            raise ControlError(f"{key}: 分片 {shard} 不在分组成员 {spec['members']} 中", "bad_shard")
        bag = self._shard_bag(tenant, group)
        bag[shard] = {
            "epoch": int(row["epoch"]),
            "owner": str(row["owner"]),
            "confirmed_offset": int(row["confirmed_offset"]),
            "east_applied": int(row["east_applied"]),
            "west_applied": int(row["west_applied"]),
        }
        auth = self._authority(tenant, group)
        epoch = int(row["epoch"])
        owner = str(row["owner"])
        if auth.get("write_route") is None:
            route = {
                "region": owner,
                "epoch": epoch,
                "source": "opening",
                "observed_ms": 0,
                "subject": "write",
            }
            auth["write_route"] = dict(route)
            auth["read_route"] = dict(route, subject="read")
            auth["baseline_region"] = owner
            auth["epoch"] = epoch
            auth["highest_epoch"] = epoch
        else:
            auth["highest_epoch"] = max(int(auth.get("highest_epoch") or epoch), epoch)
        seq = self._next_seq()
        self._journal("opening", row, 0, seq)
        return {"ok": True, "loaded": "opening", "shard": f"{key}/{shard}", "seq": seq}

    # ---------- 校验与水位 ----------

    def _require_offsets(self, tenant: str, group: str, offsets: Any, exact: bool = True) -> dict[str, int]:
        spec = self._group(tenant, group)
        if not isinstance(offsets, dict):
            raise ControlError("offsets 必须是对象", "bad_offsets")
        result: dict[str, int] = {}
        for shard, value in offsets.items():
            if shard not in spec["members"]:
                raise ControlError(f"{group_key(tenant, group)}: 未知分片 {shard}", "bad_offsets")
            if not _is_int(value):
                raise ControlError(f"offset {shard} 必须是整数", "bad_offsets")
            result[str(shard)] = int(value)
        if exact:
            missing = [s for s in spec["members"] if s not in result]
            extra = [s for s in result if s not in spec["members"]]
            if missing or extra:
                raise ControlError(
                    "整组 offsets 必须恰好包含全部成员分片",
                    "partial_group_offsets",
                    {"missing": missing, "extra": extra},
                )
        return result

    def _watermarks(self, tenant: str, group: str) -> dict[str, dict[str, int | None]]:
        spec = self._group(tenant, group)
        bag = self._shard_bag(tenant, group)
        out: dict[str, dict[str, int | None]] = {}
        for shard in spec["members"]:
            item = bag.get(shard, {})
            out[shard] = {
                "confirmed_offset": item.get("confirmed_offset"),
                "east": item.get("east_applied"),
                "west": item.get("west_applied"),
                "min_history": spec["min_history"][shard],
            }
        return out

    def _boundary(self, tenant: str, group: str) -> dict[str, int]:
        wm = self._watermarks(tenant, group)
        return {s: int(v["confirmed_offset"]) for s, v in wm.items() if v["confirmed_offset"] is not None}

    def _bump_applied(self, tenant: str, group: str, region: str, shard: str, offset: int) -> None:
        bag = self._shard_bag(tenant, group)
        item = bag.setdefault(shard, {})
        field = f"{region}_applied"
        old = item.get(field)
        item[field] = offset if old is None else max(int(old), offset)

    def _bump_confirmed(self, tenant: str, group: str, offsets: dict[str, int]) -> None:
        bag = self._shard_bag(tenant, group)
        for shard, offset in offsets.items():
            item = bag.setdefault(shard, {})
            old = item.get("confirmed_offset")
            item["confirmed_offset"] = offset if old is None else max(int(old), offset)

    # ---------- 观察证据与切流指令 ----------

    def record_observation(self, row: dict[str, Any], at: int | None = None) -> dict:
        tenant = str(row["tenant"])
        group = str(row["group"])
        key = group_key(tenant, group)
        self._group(tenant, group)
        ms = self._stamp(at, row)
        seq = self._next_seq()
        event = str(row["event"])
        region = row.get("region")
        subject = row.get("subject")
        epoch = row.get("epoch")
        offset = row.get("offset")
        evidence = {
            "seq": seq,
            "observed_ms": ms,
            "event": event,
            "tenant": tenant,
            "group": group,
            "region": region,
            "subject": subject,
            "epoch": epoch,
            "offset": offset,
        }
        self.state["evidence"].append(evidence)
        self._journal("observation", row, ms, seq)

        if event == "request_handoff":
            decision = self.propose_transition(
                tenant, group, str(region or "west"), int(epoch), "handoff", ms, seq
            )
        elif event == "request_return":
            decision = self.propose_transition(
                tenant, group, str(region or "east"), int(epoch), "return", ms, seq
            )
        elif event == "fenced":
            decision = {"ok": True, "recorded": "fence", "evidence": evidence}
        elif event == "probe_down":
            decision = {
                "ok": True,
                "recorded": "probe",
                "evidence": evidence,
                "fence_proven": False,
                "note": "probe_down 只说明链路健康未知，不能证明旧写权失效",
            }
        elif event in {"replicated", "replication"}:
            if region not in REGIONS:
                raise ControlError("replicated 事件需要 east/west region", "bad_observation")
            if not _is_int(offset) or subject is None:
                raise ControlError("replicated 事件需要 subject 分片和整数 offset", "bad_observation")
            spec = self._group(tenant, group)
            if str(subject) not in spec["members"]:
                raise ControlError(f"复制证据中的分片 {subject} 不属于 {key}", "bad_shard")
            self._bump_applied(tenant, group, str(region), str(subject), int(offset))
            decision = {"ok": True, "recorded": "replication", "evidence": evidence}
        elif event == "route_ack":
            decision = self._apply_route_ack(evidence)
        elif event in {"connection_ok", "connected", "connection_failed", "connection_lost"}:
            decision = {"ok": True, "recorded": "connection", "evidence": evidence}
        elif event in {"gateway_open", "gateway_closed", "gateway_state"}:
            evidence["open"] = row.get("open", event == "gateway_open")
            decision = {"ok": True, "recorded": "gateway", "evidence": evidence}
        else:
            decision = {"ok": True, "recorded": "uninterpreted_evidence", "evidence": evidence}
        decision["seq"] = seq
        decision["observed_ms"] = ms
        self.state["decisions"].append({"seq": seq, "kind": "observation", "decision": decision})
        return decision

    def _apply_route_ack(self, evidence: dict) -> dict:
        tenant, group = evidence["tenant"], evidence["group"]
        auth = self._authority(tenant, group)
        epoch = evidence["epoch"]
        region = evidence["region"]
        subject = str(evidence.get("subject") or "write")
        if not _is_int(epoch) or region not in REGIONS:
            raise ControlError("route_ack 需要整数 epoch 和 east/west region", "bad_observation")
        epoch = int(epoch)
        targets = ("write", "read") if subject in {"all", "both", "traffic"} else (subject,)
        updated = []
        stale = []
        conflicts = []
        for cap in targets:
            if cap not in {"write", "read"}:
                # 未知 subject 仍保留为证据，但不擅自改变已知能力。
                conflicts.append(cap)
                continue
            current = auth.get(f"{cap}_route")
            if current and epoch < int(current["epoch"]):
                stale.append(cap)
                continue
            new_route = {
                "region": region,
                "epoch": epoch,
                "source": "route_ack",
                "observed_ms": evidence["observed_ms"],
                "evidence_seq": evidence["seq"],
                "subject": cap,
            }
            if current and epoch == int(current["epoch"]) and current.get("region") != region:
                conflicts.append(cap)
                continue
            auth[f"{cap}_route"] = new_route
            auth["highest_epoch"] = max(int(auth.get("highest_epoch") or epoch), epoch)
            updated.append(cap)
        return {
            "ok": not conflicts,
            "recorded": "route_ack",
            "evidence": evidence,
            "updated_capabilities": updated,
            "stale_capabilities": stale,
            "conflicts": conflicts,
            "note": "较低代次或冲突的路由回执只保留解释价值，不会重新激活旧授权",
        }

    def propose_transition(
        self,
        tenant: str,
        group: str,
        target_region: str,
        epoch: int,
        kind: str = "handoff",
        at: int | None = None,
        request_seq: int | None = None,
        from_epoch: int | None = None,
    ) -> dict:
        if target_region not in REGIONS:
            raise ControlError("切流目标必须是 east 或 west", "bad_region")
        key = group_key(tenant, group)
        auth = self._authority(tenant, group)
        ms = self._stamp(at)
        tkey = f"{key}@{epoch}"
        if tkey in self.state["transitions"]:
            existing = self.state["transitions"][tkey]
            existing["resend_count"] = int(existing.get("resend_count", 0)) + 1
            return {
                "ok": True,
                "decision": "transition_request_duplicate",
                "transition": tkey,
                "duplicate": True,
                "state": self.evaluate_transition(tenant, group, epoch),
            }
        source_region = opposite(target_region)
        write_route = auth.get("write_route")
        if write_route and write_route.get("region") != target_region:
            source_region = str(write_route["region"])
        if from_epoch is None:
            if write_route and write_route.get("region") == source_region:
                from_epoch = int(write_route["epoch"])
            else:
                # 路由回执可能先于提议到达；此时目标新代次已成为 current route，
                # 旧授权代次按代次单调加一的演练约定取 epoch-1。
                from_epoch = epoch - 1
        transition = {
            "id": tkey,
            "tenant": tenant,
            "group": group,
            "kind": kind,
            "from_region": source_region,
            "to_region": target_region,
            "from_epoch": int(from_epoch),
            "epoch": int(epoch),
            "requested_ms": ms,
            "request_seq": request_seq,
            "resend_count": 0,
        }
        self.state["transitions"][tkey] = transition
        auth["highest_epoch"] = max(int(auth.get("highest_epoch") or epoch), epoch)
        return {
            "ok": True,
            "decision": "transition_requested",
            "transition": tkey,
            "transition_record": transition,
            "state": self.evaluate_transition(tenant, group, epoch),
        }

    # ---------- 写操作账本 ----------

    def record_request(self, row: dict[str, Any], at: int | None = None) -> dict:
        kind = str(row["kind"])
        if kind == "read":
            return self.record_read(row, at=at)
        return self.submit_write(row, at=at)

    def submit_write(self, row: dict[str, Any], at: int | None = None) -> dict:
        tenant = str(row["tenant"])
        group = str(row["group"])
        op_id = str(row["op_id"])
        self._group(tenant, group)
        ms = self._stamp(at, row)
        seq = self._next_seq()
        requested_epoch = row.get("epoch")
        if requested_epoch is not None and not _is_int(requested_epoch):
            raise ControlError("epoch 必须是整数或 null", "bad_epoch")
        requested_epoch = None if requested_epoch is None else int(requested_epoch)
        payload = row.get("payload")
        op_key = self._op_key(tenant, op_id)
        existing = self._op(tenant, op_id)

        # 相同逻辑操作的重送：先做幂等解释，绝不在 unknown 时制造第二个生效尝试。
        if existing is not None:
            if existing.get("group") != group:
                raise ControlError(
                    f"逻辑操作 {op_id} 原来属于 {existing.get('group')}，不能改投其他业务组",
                    "op_group_conflict",
                )
            if payload is not None and existing.get("payload") is not None and payload != existing.get("payload"):
                raise ControlError(
                    f"逻辑操作 {op_id} 重送载荷与首次不同", "idempotency_payload_conflict"
                )
            existing.setdefault("resends", []).append(
                {"seq": seq, "observed_ms": ms, "epoch": requested_epoch, "record": deepcopy(row)}
            )
            advice = self.retry_advice(tenant, op_id)
            if existing["status"] == "committed":
                decision = {
                    "ok": True,
                    "admitted": False,
                    "duplicate": True,
                    "decision": "return_existing_receipt",
                    "op_id": op_id,
                    "receipt": existing.get("receipt"),
                    "cursor": existing.get("receipt", {}).get("offsets") if existing.get("receipt") else None,
                    "advice": advice,
                    "seq": seq,
                }
            elif advice.get("new_attempt_allowed"):
                decision = self._admit_write_attempt(
                    existing, requested_epoch, ms, seq, row, retry=True
                )
            else:
                decision = {
                    "ok": True,
                    "admitted": False,
                    "duplicate": True,
                    "decision": "preserve_in_flight_operation",
                    "op_id": op_id,
                    "status": existing["status"],
                    "advice": advice,
                    "blockers": self._op_blockers(existing),
                    "seq": seq,
                    "observed_ms": ms,
                }
            self.state["requests"][op_key] = existing
            self._journal("traffic_write_resend", row, ms, seq)
            self.state["decisions"].append({"seq": seq, "kind": "write", "decision": decision})
            return decision

        op: dict[str, Any] = {
            "op_id": op_id,
            "tenant": tenant,
            "group": group,
            "payload": payload,
            "status": "new",
            "created_ms": ms,
            "updated_ms": ms,
            "attempts": [],
            "receipt": None,
            "receipts": [],
            "outcomes": [],
            "resends": [],
        }
        self._put_op(op)
        self._journal("traffic_write", row, ms, seq)
        decision = self._admit_write_attempt(op, requested_epoch, ms, seq, row, retry=False)
        self.state["decisions"].append({"seq": seq, "kind": "write", "decision": decision})
        return decision

    def _resolve_epoch_region(
        self, tenant: str, group: str, requested_epoch: int | None
    ) -> tuple[int | None, str | None, str, list[dict]]:
        auth = self._authority(tenant, group)
        route = auth.get("write_route")
        if requested_epoch is None:
            if not route:
                return None, None, "no_write_route", []
            return int(route["epoch"]), str(route["region"]), "current_write_route", []
        t = self.state["transitions"].get(group_key(tenant, group) + f"@{requested_epoch}")
        if t:
            return requested_epoch, t["to_region"], "transition_target", []
        if route and requested_epoch == int(route["epoch"]):
            return requested_epoch, str(route["region"]), "current_write_route", []
        # 可能是 opening 中的旧代次。
        for shard in self._group(tenant, group)["members"]:
            item = self._shard_bag(tenant, group).get(shard)
            if item and int(item.get("epoch")) == requested_epoch:
                return requested_epoch, str(item["owner"]), "opening_epoch", []
        return requested_epoch, None, "unknown_epoch", [
            {"code": "unknown_epoch", "waiting_for": f"代次 {requested_epoch} 的路由或交接记录"}
        ]

    def _admit_write_attempt(
        self,
        op: dict,
        requested_epoch: int | None,
        ms: int,
        seq: int,
        row: dict,
        retry: bool,
    ) -> dict:
        tenant, group, op_id = op["tenant"], op["group"], op["op_id"]
        epoch, region, source, resolve_blockers = self._resolve_epoch_region(
            tenant, group, requested_epoch
        )
        blockers = list(resolve_blockers)
        if region is not None and epoch is not None:
            blockers.extend(self.write_blockers(tenant, group, region, epoch))
        admitted = not blockers and region is not None
        attempt_no = len(op["attempts"]) + 1
        attempt = {
            "attempt": attempt_no,
            "accepted_ms": ms if admitted else None,
            "requested_ms": ms,
            "epoch": epoch,
            "region": region,
            "route_source": source,
            "status": "in_flight" if admitted else "rejected_at_gate",
            "request_seq": seq,
            "blockers": blockers,
            "retry": retry,
        }
        op["attempts"].append(attempt)
        op["updated_ms"] = ms
        if admitted:
            op["status"] = "in_flight"
            op["active_attempt"] = attempt_no
            decision_name = "write_accepted"
        else:
            op["status"] = "blocked" if not op.get("receipt") else op["status"]
            op["last_gate_blockers"] = blockers
            decision_name = "write_blocked"
        advice = self.retry_advice(tenant, op_id)
        decision = {
            "ok": admitted,
            "admitted": admitted,
            "decision": decision_name,
            "op_id": op_id,
            "tenant": tenant,
            "group": group,
            "attempt": attempt_no,
            "epoch": epoch,
            "region": region,
            "authorization": self._authorization_explanation(tenant, group, region, epoch),
            "blockers": blockers,
            "advice": advice,
            "seq": seq,
            "observed_ms": ms,
        }
        if retry:
            decision["retry_of_logical_op"] = True
        return decision

    def _authorization_explanation(
        self, tenant: str, group: str, region: str | None, epoch: int | None
    ) -> dict:
        auth = self._authority(tenant, group)
        return {
            "logical_op_scope": f"{tenant}/{group}",
            "region": region,
            "epoch": epoch,
            "current_write_route": auth.get("write_route"),
            "business_group_members": self._group(tenant, group)["members"],
        }

    def gateway_closed(self, tenant: str, group: str, region: str, capability: str = "write") -> dict | None:
        latest = None
        for ev in self.state["evidence"]:
            if not (
                ev["tenant"] == tenant
                and ev["group"] == group
                and ev.get("region") == region
                and ev["event"] in {"gateway_open", "gateway_closed", "gateway_state"}
            ):
                continue
            subject = ev.get("subject") or ev.get("capability") or "all"
            if subject not in {"all", "both", "traffic", capability}:
                continue
            if latest is None or int(ev["seq"]) > int(latest["seq"]):
                latest = ev
        if not latest:
            return None
        opened = latest.get("open")
        if latest["event"] == "gateway_open":
            opened = True
        elif latest["event"] == "gateway_closed":
            opened = False
        return None if opened else latest

    def write_blockers(self, tenant: str, group: str, region: str, epoch: int) -> list[dict]:
        """判断某代次/地域现在能否受理新写。返回空列表表示可以。"""
        key = group_key(tenant, group)
        auth = self._authority(tenant, group)
        blockers: list[dict] = []
        closed_gateway = self.gateway_closed(tenant, group, region, "write")
        if closed_gateway:
            blockers.append(
                {
                    "code": "gateway_not_accepting_new_writes",
                    "message": f"{REGION_CN[region]} 网关已不接新请求；已排队的旧尝试仍需执行器回执，不能把网关关闭当作在途写已失败",
                    "evidence_seq": closed_gateway.get("seq"),
                }
            )
        route = auth.get("write_route")
        if route and epoch < int(route["epoch"]):
            blockers.append(
                {
                    "code": "stale_epoch",
                    "waiting_for": None,
                    "message": f"写权代次 {epoch} 已低于当前路由代次 {route['epoch']}",
                }
            )
        if route and epoch == int(route["epoch"]) and route.get("region") != region:
            blockers.append(
                {
                    "code": "route_points_elsewhere",
                    "message": f"代次 {epoch} 的写路由实际指向 {route.get('region')}，不是 {region}",
                }
            )
        transition = self.state["transitions"].get(f"{key}@{epoch}")
        if transition:
            # 无论路由是否已经提前翻到该代次，写入门都必须与交接安全检查一致：
            # 不能因为 route_ack 先到就绕过 fence/in-flight/replica 证据。
            view = self.evaluate_transition(tenant, group, epoch)
            for check in view["checks"]:
                if check["status"] != "met" and check["capability"] in {
                    "fence",
                    "in_flight",
                    "replica",
                    "write_route",
                    "old_new_connections",
                }:
                    blockers.append(check)
        else:
            if route and epoch > int(route["epoch"]):
                blockers.append({"code": "future_epoch", "message": f"代次 {epoch} 尚未发出"})
            # 没有交接对象时，仍直接检查当前 authority 是否已有部分/全部 fenced 证据。
            fenced_nodes = self.fenced_nodes(tenant, group, region, epoch)
            all_nodes = set(self._group(tenant, group)["write_nodes"].get(region, []))
            if all_nodes and fenced_nodes:
                if fenced_nodes >= all_nodes:
                    blockers.append(
                        {
                            "code": "authority_fenced",
                            "message": f"{REGION_CN[region]} {epoch} 代写权已由 {sorted(fenced_nodes)} 确认失效",
                            "evidence": sorted(fenced_nodes),
                        }
                    )
                else:
                    blockers.append(
                        {
                            "code": "fence_incomplete",
                            "message": "已有写入端确认失权，但尚未全部确认；为避免组分片半写，暂不受理新写",
                            "fenced": sorted(fenced_nodes),
                            "waiting_for": sorted(all_nodes - fenced_nodes),
                        }
                    )
        return blockers

    def fenced_nodes(self, tenant: str, group: str, region: str, epoch: int) -> set[str]:
        result = set()
        for ev in self.state["evidence"]:
            if (
                ev["tenant"] == tenant
                and ev["group"] == group
                and ev["event"] == "fenced"
                and ev["region"] == region
                and _is_int(ev.get("epoch"))
                and int(ev["epoch"]) == epoch
                and ev.get("subject")
            ):
                result.add(str(ev["subject"]))
        return result

    def _op_blockers(self, op: dict) -> list[dict]:
        blockers = []
        for attempt in reversed(op.get("attempts", [])):
            if attempt.get("status") == "in_flight":
                blockers.append(
                    {
                        "code": "await_executor_outcome",
                        "op_id": op["op_id"],
                        "attempt": attempt.get("attempt"),
                        "epoch": attempt.get("epoch"),
                        "region": attempt.get("region"),
                        "waiting_for": "执行器整组 receipt、no_effect 或 rejected 回执",
                    }
                )
                break
        return blockers

    def retry_advice(self, tenant: str, op_id: str) -> dict:
        op = self._op(tenant, op_id)
        if not op:
            advice = {"retryable": True, "mode": "new_operation", "new_attempt_allowed": True, "message": "未见该逻辑操作，可创建"}
        elif op["status"] == "outcome_conflict" or op.get("conflicting_outcomes"):
            advice = {
                "retryable": False,
                "mode": "reconcile_conflict_first",
                "new_attempt_allowed": False,
                "message": "同时收到互相矛盾的终态证据；继续保留双方回执，可继续接收对端证据或由操作员核对，不应直接重试生效",
            }
        elif op["status"] == "committed" or op.get("receipt"):
            advice = {
                "retryable": False,
                "mode": "use_receipt",
                "new_attempt_allowed": False,
                "message": "操作已成功；重送只会返回原回执，不能再次生效",
                "receipt": op.get("receipt"),
            }
        elif op["status"] == "no_effect":
            advice = {
                "retryable": True,
                "mode": "new_attempt",
                "new_attempt_allowed": True,
                "message": "旧执行器明确本尝试无效果，可作为同一逻辑操作的新尝试重试",
            }
        elif op["status"] == "rejected":
            permanent = bool(op.get("permanent_rejection"))
            advice = {
                "retryable": not permanent,
                "mode": "fix_payload" if permanent else "new_attempt_or_new_epoch",
                "new_attempt_allowed": not permanent,
                "message": op.get("rejection_reason")
                or ("永久性拒绝，修正载荷前不要重试" if permanent else "对端已拒绝且未写入，可按当前代次重试"),
            }
        elif op["status"] == "blocked":
            advice = {
                "retryable": True,
                "mode": "new_attempt_after_gate",
                "new_attempt_allowed": True,
                "message": "该请求此前未通过写入门、并未被执行器受理，不占在途；证据/路由齐备后可用同一 op_id 重新提交",
            }
        elif op["status"] in {"in_flight", "new", "pending_verification"}:
            advice = {
                "retryable": True,
                "mode": "idempotent_only",
                "new_attempt_allowed": False,
                "message": "结果尚不明/待核对：可以用同一 op_id 幂等询问或重送，但控制程序不会受理成第二笔生效写；需继续接收对端回执",
            }
        else:
            advice = {"retryable": False, "mode": "unknown", "new_attempt_allowed": False, "message": f"未知状态 {op['status']}"}
        advice["retry_mode"] = advice["mode"]
        return advice

    def reconcile_op(
        self,
        tenant: str,
        op_id: str,
        state_name: str = "pending_verification",
        note: str | None = None,
        at: int | None = None,
    ) -> dict:
        op = self._op(tenant, op_id)
        if not op:
            raise ControlError(f"找不到操作 {tenant}/{op_id}", "unknown_op")
        row_for_clock = {"observed_ms": at} if at is not None else None
        ms = self._stamp(at, row_for_clock)
        seq = self._next_seq()
        if state_name in {"pending", "checking", "unknown", "pending_verification"}:
            if op.get("outcome_conflict") or op.get("conflicting_outcomes"):
                # “仍待核对”不能把矛盾锁存降成普通 pending；否则终态尝试会从交接检查中消失。
                op["status"] = "outcome_conflict"
                op["conflicting_outcomes"] = True
            else:
                op["status"] = "pending_verification"
            active = op.get("active_attempt")
            for attempt in op.get("attempts", []):
                if active is None or attempt.get("attempt") == active:
                    if attempt.get("status") == "in_flight":
                        attempt["verification"] = "pending"
            op["verification"] = {"state": "pending_verification", "note": note, "ms": ms, "seq": seq}
        elif state_name in {"committed", "no_effect", "rejected"}:
            raise ControlError("终态必须通过 receipt/no-effect/reject 证据写入", "use_outcome_command")
        else:
            raise ControlError(f"未知核对状态 {state_name}", "bad_reconcile_state")
        self._journal(
            "reconcile",
            {"kind": "reconcile", "op_id": op_id, "tenant": tenant, "state": state_name, "note": note, "observed_ms": ms},
            ms,
            seq,
        )
        return {"ok": True, "op_id": op_id, "status": op["status"], "advice": self.retry_advice(tenant, op_id)}

    def adjudicate_op(
        self,
        tenant: str,
        op_id: str,
        resolution: str,
        note: str | None = None,
        receipt_seq: int | None = None,
        at: int | None = None,
    ) -> dict:
        """操作员依据对端补充证据对 outcome_conflict 作显式裁决。"""
        op = self._op(tenant, op_id)
        if not op:
            raise ControlError(f"找不到操作 {tenant}/{op_id}", "unknown_op")
        ms = self._stamp(at)
        seq = self._next_seq()
        if resolution == "receipt":
            candidates = op.get("receipts", [])
            if receipt_seq is not None:
                chosen = next((r for r in candidates if int(r.get("seq", -1)) == int(receipt_seq)), None)
            else:
                chosen = candidates[-1] if candidates else None
            if not chosen:
                raise ControlError("没有可裁决为 committed 的整组 receipt", "receipt_not_found")
            op["receipt"] = chosen
            op["status"] = "committed"
            op["committed_ms"] = chosen.get("observed_ms", ms)
            for attempt in reversed(op.get("attempts", [])):
                if self._attempt_matches_outcome(attempt, chosen.get("epoch"), chosen.get("origin")):
                    attempt["status"] = "committed"
                    attempt["adjudicated_receipt"] = chosen
                    break
        elif resolution in {"no_effect", "rejected"}:
            op["status"] = resolution
            if resolution == "rejected":
                op["permanent_rejection"] = False
        elif resolution == "pending_verification":
            op["status"] = "pending_verification"
        else:
            raise ControlError("resolution 必须是 receipt/no_effect/rejected/pending_verification", "bad_resolution")
        op["conflicting_outcomes"] = False
        op["conflict_resolved"] = {
            "resolution": resolution,
            "note": note,
            "receipt_seq": receipt_seq,
            "observed_ms": ms,
            "seq": seq,
        }
        self._journal(
            "adjudicate",
            {
                "kind": "adjudicate",
                "op_id": op_id,
                "tenant": tenant,
                "resolution": resolution,
                "note": note,
                "receipt_seq": receipt_seq,
                "observed_ms": ms,
            },
            ms,
            seq,
        )
        return {"ok": True, "op_id": op_id, "status": op["status"], "advice": self.retry_advice(tenant, op_id)}

    @staticmethod
    def _attempt_matches_outcome(attempt: dict, epoch: int | None, region: str | None) -> bool:
        # 回执省略 epoch/region 时按通配处理；回执显式给出的字段必须与尝试一致。
        if epoch is not None and attempt.get("epoch") is not None and int(attempt["epoch"]) != int(epoch):
            return False
        if region and attempt.get("region") and attempt["region"] != region:
            return False
        return True

    # ---------- 回执/结果 ----------

    def record_outcome(self, row: dict[str, Any], at: int | None = None) -> dict:
        kind = str(row["kind"])
        if kind == "receipt":
            return self.record_receipt(row, at=at)
        return self.record_negative_outcome(row, at=at)

    def record_receipt(self, row: dict[str, Any], at: int | None = None) -> dict:
        tenant = str(row["tenant"])
        group = str(row["group"])
        op_id = str(row["op_id"])
        self._group(tenant, group)
        ms = self._stamp(at, row)
        seq = self._next_seq()
        offsets = self._require_offsets(tenant, group, row.get("offsets"), exact=True)
        epoch = int(row["epoch"]) if _is_int(row.get("epoch")) else None
        origin = str(row.get("origin") or row.get("region") or "unknown")
        if origin not in REGIONS:
            raise ControlError("receipt 需要 origin=east/west", "bad_receipt")
        op = self._op(tenant, op_id)
        if op is None:
            op = {
                "op_id": op_id,
                "tenant": tenant,
                "group": group,
                "payload": row.get("payload"),
                "status": "new",
                "created_ms": ms,
                "updated_ms": ms,
                "attempts": [],
                "receipt": None,
                "receipts": [],
                "outcomes": [],
                "resends": [],
            }
            self._put_op(op)
        if op.get("group") != group:
            raise ControlError(f"回执业务组 {group} 与操作原组 {op.get('group')} 冲突", "op_group_conflict")

        receipt = {
            "receipt_id": f"rcpt-{tenant}-{op_id}",
            "op_id": op_id,
            "tenant": tenant,
            "group": group,
            "epoch": epoch,
            "origin": origin,
            "offsets": offsets,
            "observed_ms": ms,
            "seq": seq,
        }
        op["receipts"].append(receipt)
        op["outcomes"].append({"kind": "receipt", **receipt})
        prior = op.get("receipt")
        prior_negative = op.get("status") in WRITE_TERMINAL_NO_EFFECT
        offset_conflict = bool(prior and prior.get("offsets") != offsets)
        outcome_conflict = bool(op.get("conflicting_outcomes") or prior_negative or offset_conflict)
        if not prior and not prior_negative:
            op["receipt"] = receipt
            op["status"] = "committed"
            op["committed_ms"] = ms
        elif prior and not offset_conflict and not prior_negative:
            # 同一成功回执的对端/重发副本：保留所有证据，主回执不变。。
            pass
        else:
            op["conflicting_receipts"] = offset_conflict or op.get("conflicting_receipts", False)
            op["conflicting_outcomes"] = True
            op["status"] = "outcome_conflict"
        # 把对应 in-flight 尝试标记为提交；找不到也保留为历史回执。
        matched_attempt = False
        for attempt in reversed(op.get("attempts", [])):
            if attempt.get("status") == "in_flight" and self._attempt_matches_outcome(attempt, epoch, origin):
                attempt["status"] = "committed"
                attempt["outcome_seq"] = seq
                attempt["receipt"] = receipt
                matched_attempt = True
                break
        if not matched_attempt:
            # 可能是在收到 no_effect/rejected 后又到达的冲突回执：挂到同代次尝试上。
            for attempt in reversed(op.get("attempts", [])):
                if self._attempt_matches_outcome(attempt, epoch, origin):
                    attempt["conflicting_receipt"] = receipt
                    break
            op.setdefault("historical_outcomes", []).append(receipt)

        # 整组 receipt 本身就是存储水位证据；即使操作账本出现矛盾，也推进 watermark，
        # 矛盾通过 outcome_conflict 要求人工/对端进一步核对。
        self._bump_confirmed(tenant, group, offsets)
        for shard, offset in offsets.items():
            self._bump_applied(tenant, group, origin, shard, offset)
        op["updated_ms"] = ms
        self._journal("traffic_receipt", row, ms, seq)
        advice = self.retry_advice(tenant, op_id)
        decision = {
            "ok": not outcome_conflict,
            "decision": "write_committed" if not outcome_conflict else "receipt_conflicts_with_prior_outcome",
            "op_id": op_id,
            "receipt": receipt,
            "canonical_receipt": op.get("receipt"),
            "cursor": op.get("receipt", {}).get("offsets") if op.get("receipt") else None,
            "served_write_from": origin,
            "region": origin,
            "origin": origin,
            "business_group": group,
            "tenant": tenant,
            "matched_in_flight_attempt": matched_attempt,
            "authorization": self._authorization_explanation(tenant, group, origin, epoch),
            "conflict": outcome_conflict,
            "advice": advice,
            "seq": seq,
            "observed_ms": ms,
            "visibility_requirement": "后续成功的带游标读必须选择 applied >= 回执 offsets 的副本",
        }
        self.state["decisions"].append({"seq": seq, "kind": "receipt", "decision": decision})
        return decision

    def record_negative_outcome(self, row: dict[str, Any], at: int | None = None) -> dict:
        kind = str(row["kind"])
        tenant = str(row["tenant"])
        group = str(row["group"])
        op_id = str(row["op_id"])
        self._group(tenant, group)
        ms = self._stamp(at, row)
        seq = self._next_seq()
        region = str(row.get("origin") or row.get("region") or "")
        epoch = int(row["epoch"]) if _is_int(row.get("epoch")) else None
        op = self._op(tenant, op_id)
        if op is None:
            op = {
                "op_id": op_id,
                "tenant": tenant,
                "group": group,
                "status": "new",
                "created_ms": ms,
                "updated_ms": ms,
                "attempts": [],
                "receipt": None,
                "receipts": [],
                "outcomes": [],
                "resends": [],
            }
            self._put_op(op)
        outcome = {
            "kind": kind,
            "op_id": op_id,
            "region": region or None,
            "epoch": epoch,
            "observed_ms": ms,
            "seq": seq,
            "reason": row.get("reason"),
            "permanent": bool(row.get("permanent", False)),
        }
        op["outcomes"].append(outcome)
        matched = False
        for attempt in reversed(op.get("attempts", [])):
            if attempt.get("status") == "in_flight" and self._attempt_matches_outcome(attempt, epoch, region or None):
                attempt["status"] = kind
                attempt["outcome_seq"] = seq
                attempt["outcome"] = outcome
                matched = True
                break
        active_in_flight = any(a.get("status") == "in_flight" for a in op.get("attempts", []))
        if op.get("status") == "committed":
            # 成功回执已是强终态；迟到的负结果不能覆盖它，只能形成矛盾证据。
            op["conflicting_outcomes"] = True
            op["status"] = "outcome_conflict"
        elif op.get("conflicting_outcomes") or op.get("status") == "outcome_conflict":
            # 矛盾尚未经显式裁决清除前，更多负回执不能把锁存状态降回单一终态，
            # 否则可能重新放行同一逻辑操作的第二笔生效写。
            op["conflicting_outcomes"] = True
            op["status"] = "outcome_conflict"
            outcome["held_as"] = "additional_conflicting_evidence"
        elif matched or not active_in_flight:
            op["status"] = kind
            if kind == "rejected":
                op["permanent_rejection"] = outcome["permanent"]
                op["rejection_reason"] = row.get("reason")
        else:
            # 负结果显式指向别的代次/地域，不能错误结束当前在途尝试。
            outcome["not_applied_reason"] = "epoch_or_region_mismatch"
        op["updated_ms"] = ms
        self._journal(f"traffic_{kind}", row, ms, seq)
        advice = self.retry_advice(tenant, op_id)
        decision = {
            "ok": op.get("status") != "outcome_conflict",
            "decision": f"op_{kind}",
            "op_id": op_id,
            "region": region or None,
            "epoch": epoch,
            "matched_in_flight_attempt": matched,
            "active_in_flight_remains": active_in_flight and not matched,
            "advice": advice,
            "seq": seq,
            "observed_ms": ms,
        }
        self.state["decisions"].append({"seq": seq, "kind": kind, "decision": decision})
        return decision

    # ---------- 读规划 ----------

    def resolve_cursor(self, tenant: str, after: str | None, cursor: dict | None) -> tuple[dict | None, dict | None]:
        if after:
            op = self._op(tenant, after)
            if not op or not op.get("receipt"):
                raise ControlError(
                    f"找不到 {after} 的成功写回执，不能构造保证可见该回执的游标",
                    "unknown_receipt",
                    {"op_id": after},
                )
            return dict(op["receipt"]["offsets"]), op["receipt"]
        if cursor:
            return dict(cursor), None
        return None, None

    def plan_read(
        self,
        tenant: str,
        group: str,
        cursor: dict[str, int] | None = None,
        after: str | None = None,
        prefer: str | None = None,
        at: int | None = None,
        op_id: str | None = None,
        record: bool = True,
        allow_direct: bool = False,
        seq: int | None = None,
    ) -> dict:
        spec = self._group(tenant, group)
        ms = self._stamp(at)
        if prefer is not None and prefer not in REGIONS:
            raise ControlError("--prefer 必须是 east 或 west", "bad_region")
        receipt = None
        if after:
            cursor, receipt = self.resolve_cursor(tenant, after, None)
        if cursor:
            cursor = self._require_offsets(tenant, group, cursor, exact=True)
            mode = "cursor"
            required = cursor
        else:
            mode = "history"
            required = {s: int(spec["min_history"][s]) for s in spec["members"]}

        views: dict[str, dict] = {}
        eligible: list[str] = []
        routable: list[str] = []
        gateway_waiting: list[str] = []
        for region in REGIONS:
            shard_views = {}
            missing = []
            total_applied = 0
            known = True
            for shard in spec["members"]:
                wm = self._watermarks(tenant, group)[shard]
                applied = wm[region]
                need = required[shard]
                if applied is None:
                    known = False
                    item = {
                        "applied": None,
                        "required": need,
                        "eligible": False,
                        "waiting_for": f"{REGION_CN[region]} {shard} 的初始水位或 replicated 证据到 {need}",
                    }
                    missing.append({"shard": shard, **item})
                else:
                    total_applied += int(applied)
                    ok = int(applied) >= need
                    item = {
                        "applied": int(applied),
                        "required": need,
                        "eligible": ok,
                        "shortfall": max(0, need - int(applied)),
                    }
                    if not ok:
                        item["waiting_for"] = (
                            f"等待 {REGION_CN[region]} {shard} replicated/applied 至少 {need}"
                            f"（当前 {applied}，差 {need - int(applied)}）；该分片不能借用其他分片水位"
                        )
                        missing.append({"shard": shard, **item})
                shard_views[shard] = item
            is_eligible = not missing and known
            gateway_closed = self.gateway_closed(tenant, group, region, "read")
            if is_eligible:
                eligible.append(region)
                if not gateway_closed:
                    routable.append(region)
                else:
                    gateway_waiting.append(
                        f"{REGION_CN[region]}读网关已关闭；需网关放开 read，或在调用端明确允许直连副本(--allow-direct)"
                    )
            views[region] = {
                "region": region,
                "eligible": is_eligible,
                "routable": is_eligible and not gateway_closed,
                "read_gateway_closed": bool(gateway_closed),
                "shards": shard_views,
                "missing": missing,
                "freshness_score": total_applied,
            }

        auth = self._authority(tenant, group)
        read_route = auth.get("read_route")
        routed_region = read_route.get("region") if read_route else None
        chosen = None
        selection_reason = None
        gateway_warning = None
        if prefer:
            if prefer in eligible:
                chosen = prefer
                selection_reason = "operator_preference"
                if views[prefer]["read_gateway_closed"]:
                    gateway_warning = "指定地域的读网关有关闭证据；本次按操作员显式偏好/直连副本规划"
            else:
                selection_reason = "preferred_region_not_eligible"
        elif routed_region in routable:
            chosen = routed_region
            selection_reason = "current_read_route"
        elif routable:
            chosen = sorted(routable, key=lambda r: (-views[r]["freshness_score"], REGIONS.index(r)))[0]
            selection_reason = "failover_to_visible_replica"
        elif eligible and allow_direct:
            chosen = sorted(eligible, key=lambda r: (-views[r]["freshness_score"], REGIONS.index(r)))[0]
            selection_reason = "direct_replica_with_read_gateway_closed"
        elif eligible:
            selection_reason = "eligible_replicas_have_closed_read_gateways"

        alternatives = []
        if mode == "cursor":
            history_required = spec["min_history"]
            for region in REGIONS:
                wm = self._watermarks(tenant, group)
                if all(
                    wm[s][region] is not None and int(wm[s][region]) >= int(history_required[s])
                    for s in spec["members"]
                ):
                    alternatives.append(
                        {
                            "scope": "history",
                            "region": region,
                            "min_history": history_required,
                            "note": "带游标暂不可见时，可改为无游标历史读",
                        }
                    )
        result = {
            "ok": chosen is not None,
            "decision": "read_ready" if chosen else "read_waiting_for_evidence",
            "op_id": op_id,
            "tenant": tenant,
            "group": group,
            "mode": mode,
            "cursor": cursor,
            "after_receipt": receipt,
            "required": required,
            "served_by": chosen,
            "region": chosen,
            "source": chosen,
            "source_region": chosen,
            "read_source": chosen,
            "selection_reason": selection_reason,
            "route_matches_execution": bool(chosen and routed_region == chosen),
            "gateway_reroute_required": bool(chosen and routed_region and routed_region != chosen),
            "gateway_warning": gateway_warning,
            "current_read_route": read_route,
            "replicas": views,
            "eligible_replicas": eligible,
            "routable_replicas": routable,
            "waiting_for": (
                []
                if chosen is not None
                else [m["waiting_for"] for r in REGIONS for m in views[r]["missing"]] + gateway_waiting
            ),
            "alternative_scopes": alternatives,
            "observed_ms": ms,
        }
        if chosen:
            result["read_from"] = f"{REGION_CN[chosen]} {chosen}"
            result["source_watermarks"] = {
                s: views[chosen]["shards"][s]["applied"] for s in spec["members"]
            }
        if record and op_id:
            self.state["requests"][self._op_key(tenant, op_id)] = {
                "op_id": op_id,
                "tenant": tenant,
                "group": group,
                "kind": "read",
                "planned": result,
                "observed_ms": ms,
            }
            seq = seq if seq is not None else self._next_seq()
            result["seq"] = seq
            self.state["decisions"].append({"seq": seq, "kind": "read", "decision": result})
        return result

    def record_read(self, row: dict[str, Any], at: int | None = None) -> dict:
        tenant = str(row["tenant"])
        group = str(row["group"])
        op_id = str(row["op_id"])
        offsets = row.get("offsets") or {}
        cursor = self._require_offsets(tenant, group, offsets, exact=bool(offsets)) if offsets else None
        ms = self._stamp(at, row)
        seq = self._next_seq()
        self._journal("traffic_read", row, ms, seq)
        prefer = row.get("prefer")
        decision = self.plan_read(
            tenant,
            group,
            cursor=cursor,
            after=row.get("after"),
            prefer=prefer,
            at=ms,
            op_id=op_id,
            record=True,
            allow_direct=bool(row.get("allow_direct")),
            seq=seq,
        )
        return decision

    # ---------- 交接状态/总览 ----------

    def evaluate_transition(self, tenant: str, group: str, epoch: int) -> dict:
        key = group_key(tenant, group)
        tkey = f"{key}@{epoch}"
        t = self.state["transitions"].get(tkey)
        if not t:
            raise ControlError(f"找不到交接记录 {tkey}", "unknown_transition")
        source, target = t["from_region"], t["to_region"]
        from_epoch = t["from_epoch"]
        spec = self._group(tenant, group)
        checks: list[dict] = []

        source_nodes = set(spec["write_nodes"].get(source, []))
        fenced = self.fenced_nodes(tenant, group, source, from_epoch)
        missing_nodes = sorted(source_nodes - fenced)
        probes = [
            ev
            for ev in self.state["evidence"]
            if ev["tenant"] == tenant
            and ev["group"] == group
            and ev["event"] == "probe_down"
            and ev["region"] == source
            and ev.get("epoch") == from_epoch
        ]
        probe_subjects = {str(ev.get("subject")) for ev in probes}
        fence_check = {
            "capability": "fence",
            "name": "旧写权全部写入端失效",
            "status": "met" if not missing_nodes else "waiting",
            "source_region": source,
            "from_epoch": from_epoch,
            "required_subjects": sorted(source_nodes),
            "fenced_subjects": sorted(fenced & source_nodes),
            "missing_subjects": missing_nodes,
            "probe_down_subjects": sorted(probe_subjects),
            "probe_is_not_fence": True,
        }
        if missing_nodes:
            waited = []
            for node in missing_nodes:
                if node in probe_subjects:
                    waited.append(f"{node}: 只有 probe_down，需要有约束力的 fenced {from_epoch}")
                else:
                    waited.append(f"等待 {source} {node} fenced epoch={from_epoch}")
            fence_check["waiting_for"] = waited
        checks.append(fence_check)

        pending_ops = []
        conflict_ops = []
        for op in self.state["ops"].values():
            if op["tenant"] != tenant or op["group"] != group:
                continue
            for attempt in op.get("attempts", []):
                if (
                    attempt.get("region") == source
                    and attempt.get("epoch") == from_epoch
                    and attempt.get("status") == "in_flight"
                ):
                    pending_ops.append(
                        {
                            "op_id": op["op_id"],
                            "attempt": attempt.get("attempt"),
                            "accepted_ms": attempt.get("accepted_ms"),
                            "waiting_for": "receipt / no_effect / rejected，或显式标记仍待核对",
                        }
                    )
            if op.get("status") == "outcome_conflict" or (
                op.get("conflicting_outcomes") and not op.get("conflict_resolved")
            ):
                conflict_ops.append(op["op_id"])
        inflight_check = {
            "capability": "in_flight",
            "name": "在途写形成公认接管界线",
            "status": "met" if not pending_ops and not conflict_ops else "waiting",
            "pending": pending_ops,
            "outcome_conflicts": sorted(set(conflict_ops)),
            "settled": not pending_ops and not conflict_ops,
            "note": "已提交写以整组 receipt 为准；no_effect/rejected 后可重试；unknown 不能重复生效受理；矛盾回执需核对",
        }
        waiting = [p["waiting_for"] + f"：{p['op_id']}" for p in pending_ops]
        waiting.extend(f"矛盾终态证据需核对：{op_id}" for op_id in sorted(set(conflict_ops)))
        if waiting:
            inflight_check["waiting_for"] = waiting
        checks.append(inflight_check)

        boundary = self._boundary(tenant, group)
        wm = self._watermarks(tenant, group)
        replica_missing = []
        for shard in spec["members"]:
            applied = wm[shard][target]
            need = boundary.get(shard)
            if need is None:
                replica_missing.append({"shard": shard, "waiting_for": "缺少 confirmed_offset 接管界线"})
            elif applied is None or int(applied) < int(need):
                replica_missing.append(
                    {
                        "shard": shard,
                        "applied": applied,
                        "required": need,
                        "waiting_for": f"等待 {target} {shard} replicated 到 {need}（当前 {applied}）",
                    }
                )
        replica_check = {
            "capability": "replica",
            "name": "目标副本达到公认接管界线",
            "status": "met" if not replica_missing else "waiting",
            "boundary": boundary,
            "missing": replica_missing,
        }
        if replica_missing:
            replica_check["waiting_for"] = [m["waiting_for"] for m in replica_missing]
        checks.append(replica_check)

        target_route_acks = [
            ev
            for ev in self.state["evidence"]
            if ev["tenant"] == tenant
            and ev["group"] == group
            and ev["event"] == "route_ack"
            and ev["region"] == target
            and ev.get("epoch") == epoch
            and str(ev.get("subject")) in {"write", "all", "both", "traffic"}
        ]
        route_check = {
            "capability": "write_route",
            "name": "写路由确认指向目标地域的新代次",
            "status": "met" if target_route_acks else "waiting",
            "target": target,
            "epoch": epoch,
            "acks": target_route_acks,
            "waiting_for": [] if target_route_acks else [f"等待 route_ack region={target} subject=write epoch={epoch}"],
        }
        checks.append(route_check)

        source_route_acks = [
            ev
            for ev in self.state["evidence"]
            if ev["tenant"] == tenant
            and ev["group"] == group
            and ev["event"] == "route_ack"
            and ev["region"] == source
            and ev.get("epoch") == from_epoch
            and str(ev.get("subject")) in {"write", "all", "both", "traffic"}
        ]
        def connection_events(region: str) -> list[dict]:
            return [
                ev
                for ev in self.state["evidence"]
                if ev["tenant"] == tenant
                and ev["group"] == group
                and ev.get("region") == region
                and ev["event"] in {"connection_ok", "connected", "connection_failed", "connection_lost"}
            ]

        def latest_connectivity_failed(conn_events: list[dict], route_acks: list[dict]) -> bool:
            evidence = list(conn_events) + list(route_acks)
            if not evidence:
                return False
            latest = max(evidence, key=lambda ev: int(ev.get("seq", -1)))
            return latest.get("event") in {"connection_failed", "connection_lost"}

        source_connections = connection_events(source)
        target_connections = connection_events(target)
        source_conn_failed = latest_connectivity_failed(source_connections, source_route_acks)
        target_conn_failed = latest_connectivity_failed(target_connections, target_route_acks)
        source_confirmed = bool(source_route_acks) or any(
            e["event"] in {"connection_ok", "connected"} for e in source_connections
        )
        target_confirmed = bool(target_route_acks) or any(
            e["event"] in {"connection_ok", "connected"} for e in target_connections
        )
        if t["kind"] == "handoff":
            # 首次接管时 opening owner 已是旧连接基线；旧 route_ack 迟到只作解释证据，
            # 不应阻止新地域在 fence/replica/route 已齐时先恢复写。
            connection_status = "met" if target_confirmed and not target_conn_failed else "waiting"
        else:
            connection_status = (
                "met"
                if source_confirmed
                and target_confirmed
                and not source_conn_failed
                and not target_conn_failed
                else "waiting"
            )
        connection_check = {
            "capability": "old_new_connections",
            "name": "新旧连接/路由均可确认",
            "status": connection_status,
            "source_route_acks": source_route_acks,
            "target_route_acks": target_route_acks,
            "source_connection_events": source_connections,
            "target_connection_events": target_connections,
            "note": "撤回东区前，旧西区代次和新东区代次的连接/路由确认都应保留；复制状态见 replica 检查",
        }
        if connection_check["status"] != "met":
            waiting = []
            if target_conn_failed:
                waiting.append(f"{target} 新连接最近状态为失败，需要 connection_ok 或 route_ack")
            if t["kind"] == "return" and not source_confirmed:
                waiting.append(f"等待旧当前地域 {source} epoch={from_epoch} 的 route_ack/connection_ok")
            if t["kind"] == "return" and source_conn_failed:
                waiting.append(f"{source} 旧连接最近状态为失败，撤回前需确认连接/复制通道")
            if not target_confirmed:
                waiting.append(f"等待新目标地域 {target} epoch={epoch} 的 route_ack/connection_ok")
            connection_check["waiting_for"] = waiting
        checks.append(connection_check)

        history_missing = []
        for shard in spec["members"]:
            applied = wm[shard][target]
            need = spec["min_history"][shard]
            if applied is None or int(applied) < int(need):
                history_missing.append(
                    f"{target}/{shard} 历史水位 {applied} < min_history {need}"
                )
        history_check = {
            "capability": "history_read",
            "name": "目标副本可承担无游标历史读",
            "status": "met" if not history_missing else "waiting",
            "waiting_for": history_missing,
        }
        checks.append(history_check)

        required_for_write = {"fence", "in_flight", "replica", "write_route", "old_new_connections"}
        write_ready = all(c["status"] == "met" for c in checks if c["capability"] in required_for_write)
        auth = self._authority(tenant, group)
        route = auth.get("write_route")
        effective = bool(route and route.get("region") == target and int(route["epoch"]) == epoch)
        current_epoch = int(route["epoch"]) if route and _is_int(route.get("epoch")) else None
        if write_ready and effective:
            status = "writable_at_target"
        elif effective:
            status = "route_effective_but_write_blocked"
        elif write_ready and current_epoch is not None and current_epoch > epoch:
            status = "ready_then_superseded_by_higher_generation"
        elif write_ready:
            status = "ready_waiting_for_route_ack"
        elif any(c["status"] == "met" for c in checks):
            status = "evidence_partial"
        else:
            status = "requested"
        return {
            **t,
            "status": status,
            "effective_route_matches": effective,
            "write_ready": write_ready,
            "can_write": write_ready,
            "history_read_ready": history_check["status"] == "met",
            "checks": checks,
            "blockers": [
                item
                for c in checks
                if c["status"] != "met" and c["capability"] in required_for_write
                for item in c.get("waiting_for", [])
            ],
        }

    def group_status(self, tenant: str, group: str) -> dict:
        key = group_key(tenant, group)
        spec = self._group(tenant, group)
        auth = self._authority(tenant, group)
        transitions = [
            self.evaluate_transition(tenant, group, int(tid.rsplit("@", 1)[1]))
            for tid, t in self.state["transitions"].items()
            if tid.startswith(key + "@")
        ]
        transitions.sort(key=lambda x: x["epoch"])
        wm = self._watermarks(tenant, group)
        read_capabilities = {}
        boundary = self._boundary(tenant, group)
        for region in REGIONS:
            history_ok = all(
                wm[s][region] is not None and int(wm[s][region]) >= spec["min_history"][s]
                for s in spec["members"]
            )
            cursor_ok = all(
                boundary.get(s) is not None
                and wm[s][region] is not None
                and int(wm[s][region]) >= int(boundary[s])
                for s in spec["members"]
            )
            read_capabilities[region] = {"history": history_ok, "latest_confirmed_cursor": cursor_ok}
        unresolved = []
        for op in self.state["ops"].values():
            if op["tenant"] == tenant and op["group"] == group and op["status"] in {"in_flight", "new", "blocked", "pending_verification", "outcome_conflict"}:
                unresolved.append(
                    {"op_id": op["op_id"], "status": op["status"], "advice": self.retry_advice(tenant, op["op_id"])}
                )
        next_evidence = []
        if transitions:
            latest = transitions[-1]
            next_evidence = latest.get("blockers", [])
        write_route = auth.get("write_route") or {}
        read_route = auth.get("read_route") or {}
        write_blockers = (
            self.write_blockers(tenant, group, str(write_route.get("region")), int(write_route["epoch"]))
            if write_route.get("region") and _is_int(write_route.get("epoch"))
            else [{"code": "no_write_route", "waiting_for": "缺少初始 owner 或 route_ack"}]
        )
        history_regions = [r for r, cap in read_capabilities.items() if cap["history"]]
        cursor_regions = [r for r, cap in read_capabilities.items() if cap["latest_confirmed_cursor"]]
        flow = {
            "write": {
                "region": write_route.get("region"),
                "epoch": write_route.get("epoch"),
                "admissible": not write_blockers,
                "blockers": write_blockers,
                "summary": "write_open" if not write_blockers else "write_stopped_pending_evidence",
            },
            "read": {
                "route_region": read_route.get("region"),
                "history_regions": history_regions,
                "latest_cursor_regions": cursor_regions,
                "current_route_history_ready": read_route.get("region") in history_regions,
                "current_route_latest_cursor_ready": read_route.get("region") in cursor_regions,
                "summary": "history_readable" + ("_and_cursor_readable" if cursor_regions else ""),
            },
        }
        return {
            "tenant": tenant,
            "group": group,
            "key": key,
            "members": spec["members"],
            "min_history": spec["min_history"],
            "watermarks": wm,
            "write_route": auth.get("write_route"),
            "read_route": auth.get("read_route"),
            "read_capabilities": read_capabilities,
            "effective_flow": flow,
            "transitions": transitions,
            "unresolved_ops": unresolved,
            "next_evidence": next_evidence,
        }

    def tenant_status(self, tenant: str) -> dict:
        groups = [g["group"] for g in self.state["groups"].values() if g["tenant"] == tenant]
        if not groups:
            raise ControlError(f"未知租户：{tenant}", "unknown_tenant")
        return {
            "tenant": tenant,
            "groups": [self.group_status(tenant, g) for g in groups],
            "run_id": self.state["run_id"],
            "clock_ms": self.state["clock_ms"],
        }

    def all_status(self) -> dict:
        tenants = sorted({g["tenant"] for g in self.state["groups"].values()})
        return {
            "run_id": self.state["run_id"],
            "clock_ms": self.state["clock_ms"],
            "tenants": [self.tenant_status(t) for t in tenants],
        }

    def status_at(self, observed_ms: int) -> dict:
        """从 journal 重建截至 observed_ms 的状态，用于定位某一时刻卡在哪条证据。"""
        plane = ControlPlane()
        for entry in sorted(self.state["journal"], key=lambda e: (e["observed_ms"], e["seq"])):
            if entry["observed_ms"] <= observed_ms:
                plane.handle(deepcopy(entry["record"]))
        return plane.all_status()

    # ---------- 批量输入 ----------

    def replay_records(self, records: Iterable[dict], source: str | None = None) -> list[dict]:
        return [self.handle(row, source=source) for row in records]


def load_jsonl(path: str | Path) -> list[dict]:
    rows = []
    with Path(path).open("r", encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ControlError(f"{path} 第 {line_no} 行不是有效 JSON：{exc}", "bad_jsonl") from exc
            if not isinstance(row, dict):
                raise ControlError(f"{path} 第 {line_no} 行必须是对象", "bad_jsonl")
            rows.append(row)
    return rows


def load_materials(ctl: ControlPlane, directory: str | Path) -> None:
    root = Path(directory)
    for row in load_jsonl(root / "groups.jsonl"):
        ctl.load_group(row)
    opening_path = root / "opening.jsonl"
    if opening_path.exists():
        for row in load_jsonl(opening_path):
            ctl.load_opening(row)


def run_demo(directory: str | Path, state_path: str | Path | None = None) -> dict:
    """按 observed_ms 合并回放本地资料，返回完整决策报告。

    demo 每次都从空运行开始，避免重复执行时把同一批演练事件重复累加；
    需要在同一运行中追加事件时，请使用 init + replay/各控制命令。
    """
    ctl = ControlPlane(state_path, state=_fresh_state())
    if state_path:
        ctl.lock()
    try:
        load_materials(ctl, directory)
        timed: list[tuple[int, int, str, dict]] = []
        order = 0
        for feed in ("observations", "traffic"):
            path = Path(directory) / f"{feed}.jsonl"
            if path.exists():
                for row in load_jsonl(path):
                    timed.append((int(row.get("observed_ms", 0)), order, feed, row))
                    order += 1
        timed.sort(key=lambda x: (x[0], x[1]))
        timeline = []
        for _, _, feed, row in timed:
            decision = ctl.handle(row, source=feed)
            timeline.append({"feed": feed, "record": row, "decision": decision})
        if state_path:
            ctl.save()
        report = {"state": ctl.all_status(), "timeline": timeline}
    finally:
        ctl.unlock()
    return report


# ============================ 命令行 ============================


def _print(payload: Any, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    print(render_text(payload))


def render_text(payload: Any) -> str:
    """少量关键视图的中文文本渲染；其他载荷退化为易读 JSON。"""
    if isinstance(payload, dict) and "timeline" in payload and "state" in payload:
        lines = ["演练回放时间线："]
        for item in payload["timeline"]:
            row = item["record"]
            d = item["decision"]
            ms = row.get("observed_ms", "?")
            if row.get("kind") == "read" or d.get("decision") == "read_ready" or d.get("decision") == "read_waiting_for_evidence":
                lines.append(
                    f"[{ms}] 读 {row.get('op_id')} {row.get('tenant')}/{row.get('group')} "
                    f"-> {d.get('served_by') or '等待'} ({d.get('selection_reason')})"
                )
                for w in d.get("waiting_for", []):
                    lines.append(f"      等待：{w}")
            elif row.get("kind") in {"write", "receipt", "no_effect", "rejected"}:
                lines.append(
                    f"[{ms}] {row.get('kind')} {row.get('op_id')} "
                    f"-> {d.get('decision')} {d.get('region') or d.get('served_write_from') or ''}"
                )
            elif row.get("event"):
                lines.append(f"[{ms}] 证据 {row.get('event')} {row.get('tenant')}/{row.get('group')} {row.get('subject') or ''}")
        lines.append("")
        lines.append(render_text(payload["state"]))
        return "\n".join(lines)
    if isinstance(payload, dict) and "tenants" in payload:
        lines = [f"运行 {payload.get('run_id')}，逻辑时钟 {payload.get('clock_ms')}"]
        for tenant in payload["tenants"]:
            lines.append(render_text(tenant))
        return "\n".join(lines)
    if isinstance(payload, dict) and "watermarks" in payload and "write_route" in payload:
        lines = [f"业务组 {payload.get('key')}："]
        wr = payload.get("write_route") or {}
        rr = payload.get("read_route") or {}
        lines.append(
            f"  写路由：{REGION_CN.get(wr.get('region'), wr.get('region'))}@{wr.get('epoch')}；"
            f"读路由：{REGION_CN.get(rr.get('region'), rr.get('region'))}@{rr.get('epoch')}"
        )
        flow = payload.get("effective_flow")
        if flow:
            wf, rf = flow["write"], flow["read"]
            write_text = "可受理新写" if wf["admissible"] else "写已停（证据未齐）"
            lines.append(
                f"  实际能力：{write_text}；历史读候选={rf['history_regions']}，"
                f"最新游标读候选={rf['latest_cursor_regions']}"
            )
        for shard, wm in payload["watermarks"].items():
            lines.append(
                f"    - {shard}: confirmed={wm['confirmed_offset']} "
                f"东={wm['east']} 西={wm['west']} min_history={wm['min_history']}"
            )
        for t in payload.get("transitions", []):
            lines.append(f"    交接 {t['id']} [{t['status']}] write_ready={t['write_ready']}")
            for blocker in t.get("blockers", []):
                lines.append(f"      卡证：{blocker}")
        for op in payload.get("unresolved_ops", []):
            lines.append(f"    在途/待核 {op['op_id']}：{op['status']}；{op['advice']['message']}")
        if payload.get("next_evidence"):
            lines.append("    下一步证据：")
            for e in payload["next_evidence"]:
                lines.append(f"      * {e}")
        return "\n".join(lines)
    if isinstance(payload, dict) and "groups" in payload and "tenant" in payload:
        lines = [f"租户 {payload['tenant']}："]
        for g in payload["groups"]:
            wr = g.get("write_route") or {}
            rr = g.get("read_route") or {}
            lines.append(
                f"  组 {g['group']}：写={REGION_CN.get(wr.get('region'), wr.get('region'))}@{wr.get('epoch')} "
                f"读={REGION_CN.get(rr.get('region'), rr.get('region'))}@{rr.get('epoch')}"
            )
            for shard, wm in g["watermarks"].items():
                lines.append(
                    f"    - {shard}: confirmed={wm['confirmed_offset']} "
                    f"东={wm['east']} 西={wm['west']} min_history={wm['min_history']}"
                )
            for t in g["transitions"]:
                lines.append(f"    交接 {t['id']} [{t['status']}] write_ready={t['write_ready']}")
                for blocker in t.get("blockers", []):
                    lines.append(f"      卡证：{blocker}")
            for op in g.get("unresolved_ops", []):
                lines.append(f"    在途/待核 {op['op_id']}：{op['status']}；{op['advice']['message']}")
            if g.get("next_evidence"):
                lines.append("    下一步证据：")
                for e in g["next_evidence"]:
                    lines.append(f"      * {e}")
        return "\n".join(lines)
    return json.dumps(payload, ensure_ascii=False, indent=2)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="duty_ctl",
        description="双地域切流演练控制程序：按租户切流、记录证据、追踪读写实际流向",
    )
    parser.add_argument("--state", default=None, help="持久化状态 JSON（默认 $DUTY_CTL_STATE 或 ./.dutyctl-state.json；demo 用 ./.dutyctl-demo.json）")
    parser.add_argument("--json", action="store_true", help="输出机器可解析 JSON")
    sub = parser.add_subparsers(dest="command", required=False)

    p_init = sub.add_parser("init", help="从 materials 目录装入 groups/opening")
    p_init.add_argument("--materials", default=str(Path(__file__).resolve().parent / "materials"))
    p_init.add_argument("--run-id")
    p_init.add_argument("--force", action="store_true", help="丢弃状态文件中的旧运行，开始新一轮")

    p_demo = sub.add_parser("demo", help="合并回放自带演练资料并输出决策")
    p_demo.add_argument("--materials", default=str(Path(__file__).resolve().parent / "materials"))

    p_replay = sub.add_parser("replay", help="回放一个或多个 JSONL/JSON 记录文件")
    p_replay.add_argument("files", nargs="+")
    p_replay.add_argument("--sort", choices=["observed", "input"], default="observed", help="按 observed_ms 合并或严格按输入顺序")

    p_h = sub.add_parser("handoff", help="提出东区到西区切流；省略 group 时覆盖租户全部组")
    p_h.add_argument("tenant")
    p_h.add_argument("group", nargs="?", default=None)
    p_h.add_argument("--epoch", type=int, required=True)
    p_h.add_argument("--at", type=int)

    p_r = sub.add_parser("return", aliases=["withdraw", "giveback"], help="提出撤回东区；省略 group 时覆盖租户全部组")
    p_r.add_argument("tenant")
    p_r.add_argument("group", nargs="?", default=None)
    p_r.add_argument("--epoch", type=int, required=True)
    p_r.add_argument("--at", type=int)

    p_gw = sub.add_parser("gateway", help="更新某地域网关是否还接新读/写请求")
    p_gw.add_argument("tenant")
    p_gw.add_argument("group", nargs="?", default="jobs")
    p_gw.add_argument("region", choices=REGIONS)
    p_gw.add_argument("--write", choices=["open", "closed"], help="写网关状态")
    p_gw.add_argument("--read", choices=["open", "closed"], help="读网关状态")
    p_gw.add_argument("--at", type=int)

    p_reconcile = sub.add_parser("reconcile", help="把结果不明操作标记为仍待核对；矛盾证据不会被降级")
    p_reconcile.add_argument("tenant")
    p_reconcile.add_argument("op_id")
    p_reconcile.add_argument("--note")
    p_reconcile.add_argument("--at", type=int)

    p_resolve = sub.add_parser("resolve-conflict", help="操作员显式裁决互相矛盾的终态证据")
    p_resolve.add_argument("tenant")
    p_resolve.add_argument("op_id")
    p_resolve.add_argument("--accept", required=True, choices=["receipt", "no_effect", "rejected", "pending_verification"])
    p_resolve.add_argument("--receipt-seq", type=int)
    p_resolve.add_argument("--note")
    p_resolve.add_argument("--at", type=int)

    p_conn = sub.add_parser("connection", help="记录新旧连接检查证据 connection_ok/connection_failed")
    p_conn.add_argument("tenant")
    p_conn.add_argument("group", nargs="?", default="jobs")
    p_conn.add_argument("region", choices=REGIONS)
    p_conn.add_argument("state_name", metavar="state", choices=["ok", "failed", "lost"])
    p_conn.add_argument("--subject", default="write-route")
    p_conn.add_argument("--epoch", type=int)
    p_conn.add_argument("--at", type=int)

    p_fence = sub.add_parser("fence", help="记录有约束力的 fenced 失权回执")
    _obs_args(p_fence)
    p_fence.add_argument("--epoch", type=int, required=True)
    p_fence.add_argument("--region", default="east")

    p_probe = sub.add_parser("probe-down", help="记录 probe_down；不能当作 fence")
    _obs_args(p_probe)
    p_probe.add_argument("--epoch", type=int, required=True)
    p_probe.add_argument("--region", default="east")

    p_rep = sub.add_parser("replicate", help="记录分片复制水位")
    _obs_args(p_rep)
    p_rep.add_argument("--region", required=True, choices=REGIONS)
    p_rep.add_argument("--shard", required=False, help="也可直接使用位置参数 subject")
    p_rep.add_argument("--offset", type=int, required=True)
    p_rep.add_argument("--epoch", type=int)

    p_route = sub.add_parser("route-ack", help="记录路由确认")
    _obs_args(p_route)
    p_route.add_argument("--region", required=True, choices=REGIONS)
    p_route.add_argument("--epoch", type=int, required=True)
    p_route.add_argument("--subject", default="write", choices=["write", "read", "all", "both", "traffic"])

    p_write = sub.add_parser("write", aliases=["create-task", "create"], help="创建或幂等重送逻辑写/任务")
    p_write.add_argument("op_id")
    p_write.add_argument("tenant")
    p_write.add_argument("group", nargs="?", default="jobs")
    p_write.add_argument("--epoch", type=int)
    p_write.add_argument("--at", type=int)
    p_write.add_argument("--payload-json")

    p_read = sub.add_parser("read", help="规划/记录一次读；无 --cursor/--after 即历史读")
    p_read.add_argument("op_id")
    p_read.add_argument("tenant")
    p_read.add_argument("group", nargs="?", default="jobs")
    p_read.add_argument("--cursor", help='JSON，如 {"task":119,"ack":42}')
    p_read.add_argument("--after", help="必须可见的成功写 op_id/回执")
    p_read.add_argument("--prefer", choices=REGIONS)
    p_read.add_argument("--allow-direct", action="store_true", help="目标地域读网关关闭时仍允许直连合格副本")
    p_read.add_argument("--at", type=int)

    p_rcpt = sub.add_parser("receipt", aliases=["confirm-task", "confirm"], help="记录执行器整组成功回执")
    p_rcpt.add_argument("op_id")
    p_rcpt.add_argument("tenant")
    p_rcpt.add_argument("group", nargs="?", default="jobs")
    p_rcpt.add_argument("--origin", required=True, choices=REGIONS)
    p_rcpt.add_argument("--epoch", type=int, required=True)
    p_rcpt.add_argument("--offsets", required=True, help='JSON，如 {"task":119,"ack":42}')
    p_rcpt.add_argument("--at", type=int)

    p_no = sub.add_parser("no-effect", help="记录旧执行器明确无效果承诺")
    p_no.add_argument("op_id")
    p_no.add_argument("tenant")
    p_no.add_argument("group", nargs="?", default="jobs")
    p_no.add_argument("--origin", required=True, choices=REGIONS)
    p_no.add_argument("--epoch", type=int)
    p_no.add_argument("--at", type=int)

    p_rej = sub.add_parser("reject", help="记录对端拒绝")
    p_rej.add_argument("op_id")
    p_rej.add_argument("tenant")
    p_rej.add_argument("group", nargs="?", default="jobs")
    p_rej.add_argument("--origin", choices=REGIONS)
    p_rej.add_argument("--epoch", type=int)
    p_rej.add_argument("--reason")
    p_rej.add_argument("--permanent", action="store_true")
    p_rej.add_argument("--at", type=int)

    p_status = sub.add_parser("status", help="查看租户/组证据和卡点")
    p_status.add_argument("tenant", nargs="?")
    p_status.add_argument("group_pos", nargs="?", metavar="group")
    p_status.add_argument("--group", dest="group_opt")
    p_status.add_argument("--at", type=int, help="重放到该 observed_ms 的历史状态")

    p_op = sub.add_parser("op", help="查看逻辑操作、尝试和重试建议")
    p_op.add_argument("tenant")
    p_op.add_argument("op_id")

    p_journal = sub.add_parser("journal", help="输出已持久化的原始事件日志")
    return parser


def _obs_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("tenant")
    parser.add_argument("group", nargs="?", default="jobs")
    parser.add_argument("subject_pos", nargs="?", metavar="subject")
    parser.add_argument("--at", type=int)


def _normalize_global_args(argv: list[str] | None) -> list[str]:
    """允许把 --state/--json 放在子命令前后。"""
    if argv is None:
        argv = __import__("sys").argv[1:]
    global_args: list[str] = []
    rest: list[str] = []
    i = 0
    while i < len(argv):
        token = argv[i]
        if token == "--state":
            if i + 1 < len(argv):
                global_args.extend(["--state", argv[i + 1]])
                i += 2
                continue
        elif token.startswith("--state="):
            global_args.extend(["--state", token.split("=", 1)[1]])
            i += 1
            continue
        elif token == "--json":
            global_args.append("--json")
            i += 1
            continue
        rest.append(token)
        i += 1
    return global_args + rest


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    normalized = _normalize_global_args(argv)
    if not normalized:
        parser.print_help()
        return 0
    args = parser.parse_args(normalized)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    if not args.state:
        env_state = os.environ.get("DUTY_CTL_STATE")
        if env_state:
            args.state = env_state
        elif args.command == "demo":
            args.state = ".dutyctl-demo.json"
        else:
            args.state = ".dutyctl-state.json"
    try:
        if args.command == "demo":
            report = run_demo(args.materials, args.state)
            _print(report, args.json)
            return 0

        with ControlPlane(args.state) as ctl:
            payload: Any
            if args.command == "init":
                if args.force:
                    ctl.state = _fresh_state(args.run_id)
                elif args.run_id:
                    ctl.state["run_id"] = args.run_id
                load_materials(ctl, args.materials)
                payload = {"ok": True, "initialized": args.state, "status": ctl.all_status()}
            elif args.command == "replay":
                loaded = []
                for file in args.files:
                    path = Path(file)
                    rows = load_jsonl(path) if path.suffix == ".jsonl" else json.loads(path.read_text(encoding="utf-8"))
                    if isinstance(rows, dict):
                        rows = [rows]
                    for row in rows:
                        loaded.append((str(path), row))
                if args.sort == "observed":
                    # Python 排序稳定：同 observed_ms 时保留命令行文件与文件内行序。
                    loaded.sort(key=lambda item: (item[1].get("observed_ms") or 0))
                out = []
                for source, row in loaded:
                    out.append(ctl.handle(row, source=source))
                payload = {"ok": True, "handled": len(out), "order": args.sort, "decisions": out}
            elif args.command == "handoff":
                groups = [args.group] if args.group else [g["group"] for g in ctl.state["groups"].values() if g["tenant"] == args.tenant]
                if not groups:
                    raise ControlError(f"未知租户或租户下没有业务组：{args.tenant}", "unknown_tenant")
                payload = [ctl.propose_transition(args.tenant, g, "west", args.epoch, "handoff", at=args.at) for g in groups]
                if len(payload) == 1:
                    payload = payload[0]
            elif args.command in {"return", "withdraw", "giveback"}:
                groups = [args.group] if args.group else [g["group"] for g in ctl.state["groups"].values() if g["tenant"] == args.tenant]
                if not groups:
                    raise ControlError(f"未知租户或租户下没有业务组：{args.tenant}", "unknown_tenant")
                payload = [ctl.propose_transition(args.tenant, g, "east", args.epoch, "return", at=args.at) for g in groups]
                if len(payload) == 1:
                    payload = payload[0]
            elif args.command == "gateway":
                events = []
                for capability, value in (("write", args.write), ("read", args.read)):
                    if value:
                        events.append(ctl.record_observation({
                            "observed_ms": args.at,
                            "event": f"gateway_{value}",
                            "tenant": args.tenant,
                            "group": args.group,
                            "region": args.region,
                            "subject": capability,
                            "epoch": None,
                            "offset": None,
                        }, at=args.at))
                payload = {"ok": True, "events": events}
            elif args.command == "reconcile":
                payload = ctl.reconcile_op(args.tenant, args.op_id, note=args.note, at=args.at)
            elif args.command == "resolve-conflict":
                payload = ctl.adjudicate_op(
                    args.tenant,
                    args.op_id,
                    args.accept,
                    note=args.note,
                    receipt_seq=args.receipt_seq,
                    at=args.at,
                )
            elif args.command == "connection":
                event = {
                    "ok": "connection_ok",
                    "failed": "connection_failed",
                    "lost": "connection_lost",
                }[args.state_name]
                payload = ctl.record_observation({
                    "observed_ms": args.at,
                    "event": event,
                    "tenant": args.tenant,
                    "group": args.group,
                    "region": args.region,
                    "subject": args.subject,
                    "epoch": args.epoch,
                    "offset": None,
                }, at=args.at)
            elif args.command == "fence":
                payload = ctl.record_observation(_obs_row(args, "fenced"), at=args.at)
            elif args.command == "probe-down":
                payload = ctl.record_observation(_obs_row(args, "probe_down"), at=args.at)
            elif args.command == "replicate":
                row = _obs_row(args, "replicated")
                shard = args.shard or getattr(args, "subject_pos", None)
                if not shard:
                    raise ControlError("replicate 需要 --shard 或位置参数 shard", "missing_shard")
                row.update({"subject": shard, "offset": args.offset, "epoch": args.epoch})
                payload = ctl.record_observation(row, at=args.at)
            elif args.command == "route-ack":
                row = _obs_row(args, "route_ack")
                subject = getattr(args, "subject_pos", None) or args.subject
                row.update({"region": args.region, "epoch": args.epoch, "subject": subject, "offset": None})
                payload = ctl.record_observation(row, at=args.at)
            elif args.command in {"write", "create-task", "create"}:
                row = {
                    "observed_ms": args.at,
                    "kind": "write",
                    "op_id": args.op_id,
                    "tenant": args.tenant,
                    "group": args.group,
                    "epoch": args.epoch,
                    "origin": None,
                    "offsets": {},
                }
                if args.payload_json:
                    row["payload"] = json.loads(args.payload_json)
                payload = ctl.submit_write(row, at=args.at)
            elif args.command == "read":
                cursor = json.loads(args.cursor) if args.cursor else None
                payload = ctl.record_read(
                    {
                        "observed_ms": args.at,
                        "kind": "read",
                        "op_id": args.op_id,
                        "tenant": args.tenant,
                        "group": args.group,
                        "offsets": cursor or {},
                        "after": args.after,
                        "prefer": args.prefer,
                        "allow_direct": args.allow_direct,
                    },
                    at=args.at,
                )
            elif args.command in {"receipt", "confirm-task", "confirm"}:
                payload = ctl.record_receipt(
                    {
                        "observed_ms": args.at,
                        "kind": "receipt",
                        "op_id": args.op_id,
                        "tenant": args.tenant,
                        "group": args.group,
                        "epoch": args.epoch,
                        "origin": args.origin,
                        "offsets": json.loads(args.offsets),
                    },
                    at=args.at,
                )
            elif args.command == "no-effect":
                payload = ctl.record_negative_outcome(
                    {
                        "observed_ms": args.at,
                        "kind": "no_effect",
                        "op_id": args.op_id,
                        "tenant": args.tenant,
                        "group": args.group,
                        "epoch": args.epoch,
                        "origin": args.origin,
                    },
                    at=args.at,
                )
            elif args.command == "reject":
                payload = ctl.record_negative_outcome(
                    {
                        "observed_ms": args.at,
                        "kind": "rejected",
                        "op_id": args.op_id,
                        "tenant": args.tenant,
                        "group": args.group,
                        "epoch": args.epoch,
                        "origin": args.origin,
                        "reason": args.reason,
                        "permanent": args.permanent,
                    },
                    at=args.at,
                )
            elif args.command == "status":
                group_name = getattr(args, "group_opt", None) or getattr(args, "group_pos", None)
                if args.at is not None:
                    payload = ctl.status_at(args.at)
                    if args.tenant and group_name:
                        tenant_view = next((t for t in payload["tenants"] if t["tenant"] == args.tenant), None)
                        payload = next((g for g in (tenant_view or {}).get("groups", []) if g["group"] == group_name), {"ok": False, "error": "not_found"})
                    elif args.tenant:
                        payload = next((t for t in payload["tenants"] if t["tenant"] == args.tenant), payload)
                elif args.tenant and group_name:
                    payload = ctl.group_status(args.tenant, group_name)
                elif args.tenant:
                    payload = ctl.tenant_status(args.tenant)
                else:
                    payload = ctl.all_status()
            elif args.command == "op":
                op = ctl._op(args.tenant, args.op_id)
                if not op:
                    raise ControlError(f"找不到操作 {args.tenant}/{args.op_id}", "unknown_op")
                payload = {**op, "advice": ctl.retry_advice(args.tenant, args.op_id)}
            elif args.command == "journal":
                payload = ctl.state["journal"]
            else:  # pragma: no cover
                parser.error(f"未实现命令 {args.command}")
            # 先持久化控制状态，再渲染输出；即使输出管道关闭也不丢操作员已确认的证据。
            ctl.save()
            _print(payload, args.json)
            if isinstance(payload, dict) and payload.get("ok") is False:
                return 3
        return 0
    except (ControlError, json.JSONDecodeError, OSError) as exc:
        err = {
            "ok": False,
            "error": str(exc),
            "code": getattr(exc, "code", "io_or_json_error"),
            "details": getattr(exc, "details", {}),
        }
        print(json.dumps(err, ensure_ascii=False, indent=2), file=__import__("sys").stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - CLI 边界，避免 malformed input 打 traceback
        err = {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "code": "unexpected_input_or_state",
        }
        if os.environ.get("DUTY_CTL_DEBUG"):
            import traceback

            err["traceback"] = traceback.format_exc()
        print(json.dumps(err, ensure_ascii=False, indent=2), file=__import__("sys").stderr)
        return 2


def _obs_row(args: argparse.Namespace, event: str) -> dict:
    subject = getattr(args, "subject_pos", None)
    if subject is None:
        subject = getattr(args, "subject", None)
    return {
        "observed_ms": args.at,
        "event": event,
        "tenant": args.tenant,
        "group": args.group,
        "region": getattr(args, "region", None),
        "subject": subject,
        "epoch": getattr(args, "epoch", None),
        "offset": getattr(args, "offset", None),
    }


if __name__ == "__main__":
    raise SystemExit(main())
