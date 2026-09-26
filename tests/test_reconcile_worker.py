"""app.reconcile_worker 的单元测试（仅依赖标准库，可直接 `python -m unittest discover`）。

覆盖：成功结果与业务错误的搬运、满额拒收、排队 / 执行中超时的名额释放语义、
取消语义、迟到结果丢弃、事件循环不被计算阻塞、随生命周期回收；
以及工作进程异常退出后的快速失败、名额 / 计时器清理、旧代消息隔离、
自动重建与重建窗口拒收、关闭时停止恢复。
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
import unittest

from app.reconcile import reconcile_runs
from app.reconcile_worker import (
    ReconcileTimeout,
    ReconcileWorker,
    ReconcileWorkerUnavailable,
)


def make_payload(n, tolerance=2):
    """构造 n 计划 × n 实播的单通道对账负载（n≈450 时计算约 1 秒）。"""
    return {
        "commands": [
            {
                "id": f"c{i}",
                "channel": "main",
                "anchor": "START",
                "offset_ms": i * 10,
                "duration_ms": 5,
            }
            for i in range(n)
        ],
        "records": [
            {
                "id": f"r{i}",
                "channel": "main",
                "actual_start_ms": i * 10,
                "actual_end_ms": i * 10 + 5,
            }
            for i in range(n)
        ],
        "tolerance_ms": tolerance,
        "miss_penalty": 100,
        "extra_penalty": 100,
    }


SMALL = make_payload(2)
SLOW = make_payload(450)
LONG_TIMEOUT = 60.0


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.worker = ReconcileWorker()
        await self.worker.start()

    async def asyncTearDown(self):
        await self.worker.stop()

    async def wait_done(self, future):
        return await asyncio.wait_for(future, timeout=30)

    async def submit_when_slot_available(self, payload=SMALL, tries=600):
        """轮询直至出现空闲名额（用于等待被弃置任务实际结束）。"""
        for _ in range(tries):
            future = self.worker.submit(payload, LONG_TIMEOUT)
            if future is not None:
                return future
            await asyncio.sleep(0.05)
        self.fail("等待名额释放超时")

    async def test_success_result_matches_direct_call(self):
        future = self.worker.submit(SMALL, LONG_TIMEOUT)
        self.assertIsNotNone(future)
        outcome = await self.wait_done(future)
        self.assertEqual(outcome[0], "ok")
        self.assertEqual(outcome[1], reconcile_runs(**SMALL))

    async def test_business_error_carried_back(self):
        payload = make_payload(1)
        payload["records"] = [
            {
                "id": "r1",
                "channel": "main",
                "actual_start_ms": 500,
                "actual_end_ms": 400,
            }
        ]
        future = self.worker.submit(payload, LONG_TIMEOUT)
        outcome = await self.wait_done(future)
        self.assertEqual(outcome[0], "error")
        self.assertEqual(outcome[1], "INVALID_INTERVAL")
        self.assertEqual(outcome[3], {"ids": ["r1"]})

    async def test_busy_limit_rejects_third_task(self):
        f1 = self.worker.submit(SLOW, LONG_TIMEOUT)  # 执行中
        f2 = self.worker.submit(SMALL, LONG_TIMEOUT)  # 排队
        self.assertIsNotNone(f1)
        self.assertIsNotNone(f2)
        # 满额：立即拒收，不提交计算
        self.assertIsNone(self.worker.submit(SMALL, LONG_TIMEOUT))
        self.assertEqual((await self.wait_done(f1))[0], "ok")
        self.assertEqual((await self.wait_done(f2))[0], "ok")
        # 名额释放后可再接纳
        f3 = self.worker.submit(SMALL, LONG_TIMEOUT)
        self.assertIsNotNone(f3)
        self.assertEqual((await self.wait_done(f3))[0], "ok")

    async def test_queued_timeout_releases_slot_immediately(self):
        f1 = self.worker.submit(SLOW, LONG_TIMEOUT)  # 执行中（慢）
        f2 = self.worker.submit(SMALL, timeout_sec=0.2)  # 排队，很快到时
        with self.assertRaises(ReconcileTimeout):
            await self.wait_done(f2)
        # 排队任务撤销后名额立即释放：慢任务仍在执行也能再接纳一个
        f3 = self.worker.submit(SMALL, LONG_TIMEOUT)
        self.assertIsNotNone(f3)
        self.assertEqual((await self.wait_done(f1))[0], "ok")
        self.assertEqual((await self.wait_done(f3))[0], "ok")

    async def test_running_timeout_holds_slot_until_actual_end(self):
        f1 = self.worker.submit(SLOW, timeout_sec=0.2)  # 执行中超时
        with self.assertRaises(ReconcileTimeout):
            await self.wait_done(f1)
        # 名额保留到实际结束：此时只能再接纳 1 个
        f2 = self.worker.submit(SMALL, LONG_TIMEOUT)
        self.assertIsNotNone(f2)
        self.assertIsNone(self.worker.submit(SMALL, LONG_TIMEOUT))
        # 慢任务实际结束后名额释放（迟到结果已被丢弃，不报错）
        f3 = await self.submit_when_slot_available()
        self.assertEqual((await self.wait_done(f2))[0], "ok")
        self.assertEqual((await self.wait_done(f3))[0], "ok")

    async def test_cancel_queued_releases_slot_immediately(self):
        f1 = self.worker.submit(SLOW, LONG_TIMEOUT)
        f2 = self.worker.submit(SMALL, LONG_TIMEOUT)
        f2.cancel()
        self.worker.cancel(f2)
        # 排队任务撤销、名额立即释放
        f3 = self.worker.submit(SMALL, LONG_TIMEOUT)
        self.assertIsNotNone(f3)
        self.assertIsNone(self.worker.submit(SMALL, LONG_TIMEOUT))
        self.assertEqual((await self.wait_done(f1))[0], "ok")
        self.assertEqual((await self.wait_done(f3))[0], "ok")

    async def test_cancel_running_abandons_and_discards_late_result(self):
        f1 = self.worker.submit(SLOW, LONG_TIMEOUT)
        await asyncio.sleep(0.1)  # 确保已进入执行
        f1.cancel()
        self.worker.cancel(f1)
        # 执行中任务弃置：名额保留，只能再接纳 1 个
        f2 = self.worker.submit(SMALL, LONG_TIMEOUT)
        self.assertIsNotNone(f2)
        self.assertIsNone(self.worker.submit(SMALL, LONG_TIMEOUT))
        # 实际结束后名额释放、迟到结果丢弃不报错
        f3 = await self.submit_when_slot_available()
        self.assertEqual((await self.wait_done(f2))[0], "ok")
        self.assertEqual((await self.wait_done(f3))[0], "ok")

    async def test_event_loop_not_blocked_during_compute(self):
        future = self.worker.submit(SLOW, LONG_TIMEOUT)
        ticks = 0
        deadline = time.monotonic() + 0.6
        while time.monotonic() < deadline and not future.done():
            await asyncio.sleep(0.02)
            ticks += 1
        # 计算期间事件循环持续响应（若被阻塞，ticks 会远小于该值）
        self.assertGreaterEqual(ticks, 10)
        self.assertEqual((await self.wait_done(future))[0], "ok")

    async def test_stop_waits_for_running_task(self):
        self.worker.submit(SLOW, LONG_TIMEOUT)
        started = time.monotonic()
        await self.worker.stop()
        elapsed = time.monotonic() - started
        # 等执行中任务实际结束才回收（计算约 1 秒）
        self.assertGreaterEqual(elapsed, 0.3)
        self.assertIsNone(self.worker._process)

    async def test_stop_without_tasks(self):
        await self.worker.stop()
        self.assertIsNone(self.worker._process)
        # 重复停止安全
        await self.worker.stop()

    # ------------------------------------------------------------------
    # 工作进程异常退出 / 自动重建
    # ------------------------------------------------------------------

    async def _kill_worker_process(self):
        """SIGKILL 当前工作进程（模拟计算中被终止），返回 (pid, 代际号)。"""
        process = self.worker._process
        self.assertIsNotNone(process)
        old_pid = process.pid
        old_gen = self.worker._session.generation
        os.kill(process.pid, signal.SIGKILL)
        return old_pid, old_gen

    async def _wait_worker_available(self, old_gen, timeout=30):
        """等待**新的一代**工作进程就绪（而非旧代崩溃后尚未被检出的窗口）。"""
        deadline = time.monotonic() + timeout
        while True:
            session = self.worker._session
            if (
                self.worker.is_available
                and session is not None
                and session.generation > old_gen
            ):
                return
            if time.monotonic() > deadline:
                self.fail("等待工作进程重建就绪超时")
            await asyncio.sleep(0.02)

    async def test_crash_fails_running_and_queued_within_a_second(self):
        f1 = self.worker.submit(SLOW, LONG_TIMEOUT)  # 执行中
        f2 = self.worker.submit(SMALL, LONG_TIMEOUT)  # 排队（两名额占满）
        await asyncio.sleep(0.2)  # 确保 f1 已进入执行
        await self._kill_worker_process()
        started = time.monotonic()
        for future in (f1, f2):
            with self.assertRaises(ReconcileWorkerUnavailable):
                await future
        # 退出被检测后 1 秒内，仍等待的执行中 / 排队任务全部失败
        self.assertLess(time.monotonic() - started, 1.0)

    async def test_crash_clears_slots_and_busy_flag(self):
        self.worker.submit(SLOW, LONG_TIMEOUT)
        self.worker.submit(SMALL, LONG_TIMEOUT)
        await asyncio.sleep(0.2)
        self.assertIsNone(self.worker.submit(SMALL, LONG_TIMEOUT))  # 两名额占满
        _, old_gen = await self._kill_worker_process()
        await self._wait_worker_available(old_gen)
        # 重建成功后名额与忙碌标记彻底清空：两名额立即可用
        g1 = self.worker.submit(SMALL, LONG_TIMEOUT)
        g2 = self.worker.submit(SMALL, LONG_TIMEOUT)
        self.assertIsNotNone(g1)
        self.assertIsNotNone(g2)
        self.assertIsNone(self.worker.submit(SMALL, LONG_TIMEOUT))
        self.assertEqual((await self.wait_done(g1))[0], "ok")
        self.assertEqual((await self.wait_done(g2))[0], "ok")

    async def test_already_timed_out_task_not_completed_again(self):
        # 执行中超时的任务：Future 已以 ReconcileTimeout 完成
        f1 = self.worker.submit(SLOW, timeout_sec=0.2)
        with self.assertRaises(ReconcileTimeout):
            await self.wait_done(f1)
        await asyncio.sleep(0.1)
        _, old_gen = await self._kill_worker_process()
        await self._wait_worker_available(old_gen)
        # 进程退出清理不得再次完成该 Future（结果保持超时语义）
        with self.assertRaises(ReconcileTimeout):
            f1.result()
        self.assertTrue(f1.done())

    async def test_new_requests_rejected_until_rebuild_ready(self):
        f1 = self.worker.submit(SLOW, LONG_TIMEOUT)
        await asyncio.sleep(0.2)
        old_pid, old_gen = await self._kill_worker_process()
        with self.assertRaises(ReconcileWorkerUnavailable):
            await f1
        # 重建就绪前：新请求立即失败，不排队（受影响任务不自动重放）
        with self.assertRaises(ReconcileWorkerUnavailable):
            self.worker.submit(SMALL, LONG_TIMEOUT)
        await self._wait_worker_available(old_gen)
        self.assertNotEqual(self.worker._process.pid, old_pid)
        # 就绪后恢复正常对账
        g = self.worker.submit(SMALL, LONG_TIMEOUT)
        self.assertEqual((await self.wait_done(g))[0], "ok")

    async def test_no_replay_after_rebuild(self):
        payload = make_payload(200)  # 约 0.2 秒，便于在执行中途被杀
        f1 = self.worker.submit(payload, LONG_TIMEOUT)
        await asyncio.sleep(0.05)
        _, old_gen = await self._kill_worker_process()
        with self.assertRaises(ReconcileWorkerUnavailable):
            await f1
        await self._wait_worker_available(old_gen)
        # 重建后不主动派发旧任务：直接提交一个小任务应立即成功，
        # 结果严格等于小负载（若旧任务被重放并先行执行，这里会观察到额外忙碌）
        g = self.worker.submit(SMALL, LONG_TIMEOUT)
        outcome = await self.wait_done(g)
        self.assertEqual(outcome, ("ok", reconcile_runs(**SMALL)))

    async def test_old_generation_messages_are_ignored(self):
        old_session = self.worker._session
        f1 = self.worker.submit(SLOW, LONG_TIMEOUT)
        await asyncio.sleep(0.2)
        _, old_gen = await self._kill_worker_process()
        with self.assertRaises(ReconcileWorkerUnavailable):
            await f1
        await self._wait_worker_available(old_gen)
        new_session = self.worker._session
        self.assertIsNot(new_session, old_session)

        # 旧代 pump 迟到（重建完成后才转发）的 free：不得清掉新代忙碌标记。
        # 先让新代占住一个执行中名额（busy=True），再投递旧代 free。
        n1 = self.worker.submit(SLOW, LONG_TIMEOUT)
        self.assertIsNotNone(n1)
        await asyncio.sleep(0.2)
        self.assertTrue(new_session.busy)
        self.worker._on_message(old_gen, ("free",))
        self.assertTrue(new_session.busy)  # 旧代 free 被隔离
        # 旧代 done 即使 task_id 与新代任务相同，也不得完成 / 释放新代任务
        (n1_id,) = new_session.tasks.keys()
        self.worker._on_message(
            old_gen, ("done", n1_id, ("ok", {"stale": True}))
        )
        self.assertFalse(n1.done())
        self.assertIn(n1_id, new_session.tasks)
        self.assertEqual((await self.wait_done(n1))[0], "ok")  # 新代真实结果正常

        # 旧代 down 同样无效：新代依旧可用
        self.worker._on_message(old_gen, ("down",))
        self.assertTrue(self.worker.is_available)

    async def test_stop_during_recovery_aborts_rebuild_and_reaps(self):
        f1 = self.worker.submit(SLOW, LONG_TIMEOUT)
        await asyncio.sleep(0.2)
        await self._kill_worker_process()
        with self.assertRaises(ReconcileWorkerUnavailable):
            await f1
        # 不等待重建，直接关闭：停止一切恢复尝试并回收进程与泵线程
        await self.worker.stop()
        self.assertIsNone(self.worker._process)
        self.assertTrue(self.worker._supervisor is None)
        session = self.worker._session
        if session is not None:
            self.assertFalse(session.pump_thread.is_alive())

    async def test_crash_when_idle_rebuilds_automatically(self):
        # 没有任务时进程退出也能自动重建
        _, old_gen = await self._kill_worker_process()
        await self._wait_worker_available(old_gen)
        g = self.worker.submit(SMALL, LONG_TIMEOUT)
        self.assertEqual((await self.wait_done(g))[0], "ok")


if __name__ == "__main__":
    unittest.main()
