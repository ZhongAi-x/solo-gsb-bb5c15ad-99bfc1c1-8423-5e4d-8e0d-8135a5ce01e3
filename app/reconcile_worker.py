"""``POST /runs/reconcile`` 的专属单工作进程执行器（仅依赖标准库）。

设计要点
--------
* 计算在**独立子进程**中执行（spawn 启动，随应用生命周期创建与回收），
  事件循环只负责排队、计时与收发结果；长对账期间 ``/health`` 等接口照常响应。
* 单工作进程**串行**执行任务；未结束任务（执行中 + 排队中）上限为
  ``MAX_INFLIGHT``，满额时 ``submit`` 返回 ``None``，由 HTTP 层转成 503，
  不提交计算。
* 每个被接纳的任务从**接纳时刻**起算超时（排队时间计入）。超时或客户端取消时：
  仍在排队的任务直接撤销并立即释放名额，永远不会被执行；已在执行的任务无法
  打断，名额保留到其**实际结束**才释放，迟到的结果直接丢弃。
* 所有状态迁移（接纳 / 派发 / 完成 / 超时 / 取消 / 释放）只发生在事件循环
  线程内，同一任务的名额只会释放一次；子进程只负责执行与回报，是否派发的
  决策权始终在父进程，因此排队任务被撤销后不存在「已被取走执行」的竞态。

崩溃检测与自动重建
------------------
* 每代工作进程附带一个**监控线程**（``Process.join`` 的唯一调用点，避免多线程
  并发 waitpid）。子进程一退出，监控线程立即把接纳窗口关闭并通知事件循环：
  仍等待的执行中 / 排队任务在退出被检测后 1 秒内以
  ``ReconcileWorkerUnavailable`` 失败（HTTP 层映射为 503 +
  ``RECONCILE_WORKER_UNAVAILABLE``），该进程占用的全部名额与计时器一并清理；
  已取消或已超时的 Future 不会再次被完成。
* 崩溃处理后**自动重建**一个单工作进程（全新队列、泵线程与监控线程，代数 +1）；
  重建就绪前 ``submit`` 抛出 ``ReconcileWorkerUnavailable``，新请求立即收到
  同样的 503；子进程发出就绪信号后恢复正常对账。受影响任务**不自动重放**。
* 进程间消息与退出回调都携带**代数**：旧进程的迟到消息（``free`` / ``done``）
  与迟到的退出回调一律忽略，不会释放新任务的名额，也不会触发重复派发。
* 应用关闭时停止自动重建，并回收工作进程、结果泵线程与监控线程。

进程间消息
----------
父 → 子（任务队列）：``("run", task_id, payload)`` / ``("stop",)``。
子 → 父（结果队列）：``("free",)`` 空闲信号、``("done", task_id, outcome)``
任务结束、``("down",)`` 退出前最后一条消息。
``outcome`` 为 ``("ok", result)`` / ``("error", code, message, details)`` /
``("crash", 摘要)``。
"""

from __future__ import annotations

import asyncio
import multiprocessing
import queue
import threading
from collections import deque
from typing import Any

from .compiler import CompileError
from .reconcile import reconcile_runs

# 未结束任务（执行中 + 排队中）上限
MAX_INFLIGHT = 2


class ReconcileTimeout(Exception):
    """对账任务在时限（含排队时间）内未完成。"""


class ReconcileWorkerUnavailable(Exception):
    """对账工作进程异常退出、正在自动重建；任务未完成且不自动重放。"""


def _worker_main(task_queue: multiprocessing.Queue, result_queue: multiprocessing.Queue) -> None:
    """子进程入口：串行消费任务，执行前后各发一次空闲信号向父进程要任务。"""
    result_queue.put(("free",))
    while True:
        message = task_queue.get()
        if message[0] == "stop":
            break
        _, task_id, payload = message
        try:
            outcome = ("ok", reconcile_runs(**payload))
        except CompileError as exc:
            # 业务错误原样搬运 code / message / details，由父进程还原为 422
            outcome = ("error", exc.code, exc.message, exc.details)
        except BaseException as exc:  # 业务逻辑之外的异常不拖垮工作进程
            outcome = ("crash", f"{type(exc).__name__}: {exc}")
        result_queue.put(("done", task_id, outcome))
        result_queue.put(("free",))
    result_queue.put(("down",))


class ReconcileWorker:
    """应用生命周期管理的单工作进程执行器（详见模块 docstring）。"""

    MAX_INFLIGHT = MAX_INFLIGHT
    _START_TIMEOUT_SEC = 30.0
    _STOP_TIMEOUT_SEC = 10.0
    _THREAD_JOIN_TIMEOUT_SEC = 2.0
    _POLL_INTERVAL_SEC = 0.05
    _PUMP_POLL_SEC = 0.1

    def __init__(self) -> None:
        self._ctx = multiprocessing.get_context("spawn")
        self._loop: asyncio.AbstractEventLoop | None = None
        # 当前代工作进程及其配套资源（每次崩溃重建时整体更换，代数 +1）
        self._generation = 0
        self._process: multiprocessing.Process | None = None
        self._process_dead: threading.Event | None = None
        self._task_queue: multiprocessing.Queue | None = None
        self._result_queue: multiprocessing.Queue | None = None
        self._pump_thread: threading.Thread | None = None
        self._pump_stop: threading.Event | None = None
        self._monitor_thread: threading.Thread | None = None
        self._last_exitcode: int | None = None
        self._ready: asyncio.Event | None = None
        self._available = False
        self._tasks: dict[int, dict[str, Any]] = {}
        self._queue: deque[int] = deque()
        self._next_task_id = 0
        self._worker_busy = False
        self._started = False

    # ------------------------------------------------------------------
    # 生命周期（随应用启动 / 关闭调用）
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动工作进程并等待其就绪（确认子进程已成功导入并贯通队列）。"""
        self._loop = asyncio.get_running_loop()
        self._ready = asyncio.Event()
        try:
            self._spawn_worker()
            deadline = self._loop.time() + self._START_TIMEOUT_SEC
            while not self._ready.is_set():
                if self._process_dead.is_set():
                    raise RuntimeError(
                        "对账工作进程启动即退出"
                        f"（exitcode={self._last_exitcode}）"
                    )
                if self._loop.time() >= deadline:
                    raise TimeoutError("等待对账工作进程就绪超时")
                await asyncio.sleep(self._POLL_INTERVAL_SEC)
        except BaseException:
            # 启动失败：按正常关闭路径回收已创建的进程与线程，不留残余
            self._started = True
            await self.stop()
            raise
        self._started = True

    async def stop(self) -> None:
        """回收工作进程：停止自动重建，撤销排队任务，等执行中任务实际结束后让子进程退出。"""
        if not self._started:
            return
        self._started = False  # 不再接纳新任务、不再派发、退出回调不再触发重建
        self._queue.clear()
        for record in self._tasks.values():
            record["state"] = "closed"
            record["timeout_handle"].cancel()
        process = self._process
        dead = self._process_dead
        if process is not None:
            if process.pid is not None:  # 已成功启动的进程才发送停止标记并等待退出
                if self._task_queue is not None and not (
                    dead is not None and dead.is_set()
                ):
                    try:
                        self._task_queue.put_nowait(("stop",))
                    except Exception:
                        pass
                if dead is not None:
                    # 子进程跑完当前任务后读到停止标记自行退出；逾期强制结束。
                    # 进程退出统一由监控线程 join 并置位 dead（避免多线程并发 waitpid）
                    await asyncio.to_thread(dead.wait, self._STOP_TIMEOUT_SEC)
                    if not dead.is_set():
                        process.terminate()
                        await asyncio.to_thread(dead.wait, self._STOP_TIMEOUT_SEC)
                    if not dead.is_set():
                        process.kill()
                        await asyncio.to_thread(dead.wait, self._STOP_TIMEOUT_SEC)
            try:
                process.close()
            except ValueError:
                pass  # 极端情况下进程未能确认退出：守护进程随应用退出被回收
            self._process = None
        # 停止并回收结果泵线程与监控线程，随后关闭队列
        if self._pump_stop is not None:
            self._pump_stop.set()
            self._pump_stop = None
        if self._pump_thread is not None:
            await asyncio.to_thread(
                self._pump_thread.join, self._THREAD_JOIN_TIMEOUT_SEC
            )
            self._pump_thread = None
        if self._monitor_thread is not None:
            await asyncio.to_thread(
                self._monitor_thread.join, self._THREAD_JOIN_TIMEOUT_SEC
            )
            self._monitor_thread = None
        for q in (self._task_queue, self._result_queue):
            if q is not None:
                try:
                    q.cancel_join_thread()
                except Exception:
                    pass
                q.close()
        self._task_queue = self._result_queue = None

    # ------------------------------------------------------------------
    # 任务接纳 / 取消（仅事件循环线程调用）
    # ------------------------------------------------------------------

    def submit(self, payload: dict[str, Any], timeout_sec: float) -> asyncio.Future | None:
        """接纳一个对账任务；满额（未结束任务达上限）时返回 ``None``。

        返回的 Future 以 ``("ok", result)`` / ``("error", code, message,
        details)`` / ``("crash", 摘要)`` 形式完成；超时以 ``ReconcileTimeout``
        异常完成；工作进程异常退出以 ``ReconcileWorkerUnavailable`` 异常完成。
        排队时间计入 ``timeout_sec``。

        工作进程不可用（异常退出后重建中）时抛出 ``ReconcileWorkerUnavailable``，
        任务不被接纳、不进入队列。
        """
        if not self._started:
            return None
        if not self._available:
            raise ReconcileWorkerUnavailable(
                "对账工作进程不可用：异常退出后正在自动重建"
            )
        if len(self._tasks) >= self.MAX_INFLIGHT:
            return None
        self._next_task_id += 1
        task_id = self._next_task_id
        future = self._loop.create_future()
        self._tasks[task_id] = {
            "payload": payload,
            "future": future,
            "state": "queued",
            "timeout_handle": self._loop.call_later(
                timeout_sec, self._on_timeout, task_id
            ),
        }
        self._queue.append(task_id)
        self._dispatch()
        return future

    def cancel(self, future: asyncio.Future) -> None:
        """客户端断开等导致的取消：与超时相同的撤销 / 弃置语义。"""
        for task_id, record in self._tasks.items():
            if record["future"] is future:
                self._release_or_abandon(task_id)
                return

    # ------------------------------------------------------------------
    # 内部状态迁移（仅事件循环线程执行）
    # ------------------------------------------------------------------

    def _dispatch(self) -> None:
        """工作进程空闲且队列非空时，把队首任务派发给子进程。"""
        if (
            not self._started
            or not self._available
            or self._worker_busy
            or not self._queue
        ):
            return
        task_id = self._queue.popleft()
        record = self._tasks[task_id]
        record["state"] = "running"
        self._worker_busy = True
        try:
            self._task_queue.put_nowait(("run", task_id, record["payload"]))
        except Exception:
            # 队列写入失败（如管道已关闭）：任务退回队首，等待下次派发
            record["state"] = "queued"
            self._worker_busy = False
            self._queue.appendleft(task_id)

    def _release_or_abandon(self, task_id: int) -> None:
        """超时 / 取消共用：排队任务撤销并立即释放名额；执行中任务弃置、保留名额。"""
        record = self._tasks.get(task_id)
        if record is None:
            return
        record["timeout_handle"].cancel()
        if record["state"] == "queued":
            # 任务尚未派发（父进程持有唯一派发权），直接撤销，永远不会被执行
            self._tasks.pop(task_id)
            try:
                self._queue.remove(task_id)
            except ValueError:
                pass
            record["state"] = "expired"
        elif record["state"] == "running":
            # 无法打断子进程内的计算：名额保留到 ("done", ...) 到达才释放
            record["state"] = "abandoned"

    def _on_timeout(self, task_id: int) -> None:
        """任务到时（自接纳起算，含排队时间）。"""
        record = self._tasks.get(task_id)
        if record is None:
            return
        self._release_or_abandon(task_id)
        future = record["future"]
        if not future.done():
            future.set_exception(ReconcileTimeout())

    def _on_done(self, task_id: int, outcome: tuple) -> None:
        """任务实际结束：释放名额；已超时 / 取消的任务结果直接丢弃。"""
        record = self._tasks.pop(task_id, None)
        if record is None:
            return  # 重复或未知完成通知：名额只释放一次
        record["timeout_handle"].cancel()
        if record["state"] == "running":
            future = record["future"]
            if not future.done():
                future.set_result(outcome)
        # 其余状态（abandoned / closed）：迟到结果丢弃

    def _on_message(self, generation: int, message: tuple) -> None:
        """处理子进程消息（由泵线程经 call_soon_threadsafe 转发到事件循环）。"""
        if generation != self._generation:
            return  # 旧进程的迟到消息：忽略，不释放名额、不触发派发
        kind = message[0]
        if kind == "free":
            self._available = True  # 子进程已就绪（含崩溃重建后的新一代）
            if self._ready is not None and not self._ready.is_set():
                self._ready.set()  # 首次空闲信号 = 首次启动就绪
            self._worker_busy = False
            self._dispatch()
        elif kind == "done":
            _, task_id, outcome = message
            self._on_done(task_id, outcome)
        # ("down",)：子进程已按序退出，无需处理（退出由监控线程感知）

    # ------------------------------------------------------------------
    # 崩溃检测与自动重建
    # ------------------------------------------------------------------

    def _spawn_worker(self) -> None:
        """创建新一代工作进程及其配套队列、泵线程与监控线程（仅事件循环线程调用）。"""
        self._generation += 1
        generation = self._generation
        self._available = False  # 就绪信号到达前不接纳新任务
        self._worker_busy = False
        self._task_queue = self._ctx.Queue()
        self._result_queue = self._ctx.Queue()
        # 子进程异常退出后，任务队列的写入线程可能对断裂的管道写入；
        # 忽略 EPIPE，避免向 stderr 打印无意义的异常堆栈（concurrent.futures 同款用法）
        self._task_queue._ignore_epipe = True
        self._process_dead = threading.Event()
        self._pump_stop = threading.Event()
        self._process = self._ctx.Process(
            target=_worker_main,
            args=(self._task_queue, self._result_queue),
            name=f"reconcile-worker-{generation}",
            daemon=True,
        )
        self._process.start()
        self._pump_thread = threading.Thread(
            target=self._pump_results,
            args=(generation, self._result_queue, self._pump_stop),
            name=f"reconcile-result-pump-{generation}",
            daemon=True,
        )
        self._pump_thread.start()
        self._monitor_thread = threading.Thread(
            target=self._monitor_process,
            args=(generation, self._process, self._process_dead),
            name=f"reconcile-process-monitor-{generation}",
            daemon=True,
        )
        self._monitor_thread.start()

    def _on_worker_exit(self, generation: int) -> None:
        """工作进程异常退出：失败全部未结束任务，回收旧代资源并自动重建。"""
        if generation != self._generation or not self._started:
            return  # 旧代的迟到回调，或应用正在关闭（正常退出不触发重建）
        # 1) 仍等待的执行中 / 排队任务立即失败（退出被检测后 1 秒内返回 503）；
        #    已取消或已超时的 Future 不再次被完成；受影响任务不自动重放
        for record in self._tasks.values():
            record["timeout_handle"].cancel()
            future = record["future"]
            if not future.done():
                future.set_exception(
                    ReconcileWorkerUnavailable("对账工作进程异常退出，任务未完成")
                )
        # 2) 清理该进程占用的全部名额与计时器
        self._tasks.clear()
        self._queue.clear()
        self._worker_busy = False
        # 3) 回收崩溃代的进程、泵线程、监控线程与队列
        self._reclaim_crashed_generation()
        # 4) 自动重建单工作进程；就绪前 submit 继续拒绝新任务
        self._spawn_worker()

    def _reclaim_crashed_generation(self) -> None:
        """回收崩溃代的全部资源（仅事件循环线程调用）。

        子进程已被监控线程 join（``_process_dead`` 置位），``close`` 立即返回；
        泵线程最多再阻塞一个轮询周期（0.1s），整体停顿远小于 1 秒预算。
        """
        process = self._process
        if process is not None:
            try:
                process.close()
            except ValueError:
                pass
            self._process = None
        if self._pump_stop is not None:
            self._pump_stop.set()
            self._pump_stop = None
        if self._pump_thread is not None:
            self._pump_thread.join(timeout=self._THREAD_JOIN_TIMEOUT_SEC)
            self._pump_thread = None
        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=self._THREAD_JOIN_TIMEOUT_SEC)
            self._monitor_thread = None
        for q in (self._task_queue, self._result_queue):
            if q is not None:
                try:
                    q.cancel_join_thread()
                except Exception:
                    pass
                q.close()
        self._task_queue = self._result_queue = None

    # ------------------------------------------------------------------
    # 监控线程：阻塞等待子进程退出（Process.join 的唯一调用点）
    # ------------------------------------------------------------------

    def _monitor_process(
        self,
        generation: int,
        process: multiprocessing.Process,
        dead: threading.Event,
    ) -> None:
        """等待子进程退出，置位退出标记并通知事件循环做崩溃处理。"""
        process.join()
        self._last_exitcode = process.exitcode
        dead.set()
        if generation != self._generation:
            return  # 旧代进程的退出已由崩溃处理 / 关闭流程处理
        # 立即关闭接纳窗口，避免事件循环把新任务派发给已退出的进程
        self._available = False
        loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(self._on_worker_exit, generation)
        except RuntimeError:  # 事件循环已关闭
            pass

    # ------------------------------------------------------------------
    # 泵线程：阻塞读结果队列，不占用事件循环
    # ------------------------------------------------------------------

    def _pump_results(
        self,
        generation: int,
        result_queue: multiprocessing.Queue,
        stop_event: threading.Event,
    ) -> None:
        """把结果队列的消息逐条转发到事件循环线程处理（携带代数以隔离旧进程）。"""
        while not stop_event.is_set():
            try:
                message = result_queue.get(timeout=self._PUMP_POLL_SEC)
            except queue.Empty:
                continue
            except (EOFError, OSError):
                break
            loop = self._loop
            if loop is None:
                break
            try:
                loop.call_soon_threadsafe(self._on_message, generation, message)
            except RuntimeError:  # 事件循环已关闭
                break
            if message[0] == "down":
                break
