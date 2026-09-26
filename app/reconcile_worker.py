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

工作进程异常退出与自动重建
--------------------------
工作进程在计算中被杀（段错误、SIGKILL 等）时，看门狗在
``_WATCHDOG_POLL_SEC`` 轮询内（不超过 1 秒）检测到退出，随后：

1. 该**代（generation）**会话内仍在等待的执行中 / 排队任务立即以
   ``ReconcileWorkerUnavailable`` 失败（HTTP 层映射为 503 +
   ``RECONCILE_WORKER_UNAVAILABLE``），全部名额与计时器一次性清理；
   已超时 / 已取消的任务 Future 早已完成，不会被再次完成。
2. 旧进程的队列、泵线程被回收；泵线程向事件循环转发的每条消息都带**代际标记**，
   旧代消息（``free`` / ``done`` / …）在新代就绪后一律忽略：既不会错误释放
   新任务的忙碌标记与名额，也不会触发重复派发。
3. 自动重建一个全新的单工作进程（新队列、新泵线程）；重建期间（含连续启动
   失败后的退避重试）``submit`` 立即抛出 ``ReconcileWorkerUnavailable``，
   不排队、不重放受影响任务；新进程就绪后恢复正常对账。
4. 应用关闭时取消看门狗并停止一切重建尝试，回收（仍在执行则优雅等待，
   否则强制结束）当前进程与结果读取线程。

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
import queue as queue_module
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from .compiler import CompileError
from .reconcile import reconcile_runs

# 未结束任务（执行中 + 排队中）上限
MAX_INFLIGHT = 2


class ReconcileTimeout(Exception):
    """对账任务在时限（含排队时间）内未完成。"""


class ReconcileWorkerUnavailable(Exception):
    """对账工作进程不可用（异常退出后正在重建，或尚未就绪）。

    由 ``submit`` 在接纳前直接抛出，或在任务等待期间以该异常完成 Future，
    HTTP 层统一映射为 ``503`` + ``RECONCILE_WORKER_UNAVAILABLE``。
    """


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


@dataclass
class _Session:
    """一代工作进程的全部资源与任务状态；代际切换后旧实例不再被任何路径修改。"""

    generation: int
    task_queue: Any = None
    result_queue: Any = None
    process: multiprocessing.Process | None = None
    pump_thread: threading.Thread | None = None
    pump_stop: threading.Event = field(default_factory=threading.Event)
    ready: asyncio.Event | None = None
    # starting：进程已拉起、等待首个 free；ready：可接纳任务；down：进程已退出
    state: str = "starting"
    busy: bool = False
    tasks: dict[int, dict[str, Any]] = field(default_factory=dict)
    queue: deque[int] = field(default_factory=deque)
    cleaned: bool = False


class ReconcileWorker:
    """应用生命周期管理的单工作进程执行器（详见模块 docstring）。"""

    MAX_INFLIGHT = MAX_INFLIGHT
    _START_TIMEOUT_SEC = 30.0
    _STOP_TIMEOUT_SEC = 10.0
    _PUMP_JOIN_TIMEOUT_SEC = 2.0
    _PUMP_GET_TIMEOUT_SEC = 0.2
    _POLL_INTERVAL_SEC = 0.05
    # 看门狗轮询间隔：进程退出后最迟该间隔内被检测（要求 1 秒内）
    _WATCHDOG_POLL_SEC = 0.1
    # 重建失败后的退避：0.1s 起翻倍，上限 1s
    _REBUILD_BACKOFF_SEC = 0.1
    _REBUILD_BACKOFF_MAX_SEC = 1.0

    def __init__(self) -> None:
        self._ctx = multiprocessing.get_context("spawn")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._session: _Session | None = None
        self._supervisor: asyncio.Task | None = None
        self._next_task_id = 0
        self._next_generation = 1
        self._started = False

    # ------------------------------------------------------------------
    # 兼容只读访问（旧测试 / 排障用）：当前代的工作进程
    # ------------------------------------------------------------------

    @property
    def _process(self) -> multiprocessing.Process | None:
        session = self._session
        return session.process if session is not None else None

    @property
    def is_available(self) -> bool:
        """工作进程是否就绪、可立即接纳对账任务。"""
        session = self._session
        return (
            self._started
            and session is not None
            and session.state == "ready"
        )

    # ------------------------------------------------------------------
    # 生命周期（随应用启动 / 关闭调用）
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """启动工作进程并等待其就绪（确认子进程已成功导入并贯通队列）。"""
        self._loop = asyncio.get_running_loop()
        self._started = True
        # 首次启动失败（启动即退出 / 就绪超时）直接抛出，不进入重建循环
        await self._open_new_session()
        self._supervisor = self._loop.create_task(
            self._supervise(), name="reconcile-worker-supervisor"
        )

    async def stop(self) -> None:
        """回收工作进程：停止看门狗与重建，撤销排队任务并回收进程、泵线程。"""
        if not self._started:
            return
        self._started = False  # 不再接纳新任务、不再重建
        # 先停看门狗，避免优雅回收期间把正常退出误判为崩溃重建
        if self._supervisor is not None:
            self._supervisor.cancel()
            try:
                await self._supervisor
            except (asyncio.CancelledError, Exception):
                pass
            self._supervisor = None
        session = self._session
        self._session = None
        if session is not None:
            await self._reap_for_stop(session)

    async def _supervise(self) -> None:
        """看门狗：周期性确认就绪代进程存活；退出则失败其任务并自动重建。"""
        while self._started:
            await asyncio.sleep(self._WATCHDOG_POLL_SEC)
            session = self._session
            if (
                session is not None
                and session.state == "ready"
                and not session.process.is_alive()
            ):
                await self._handle_exit(session)

    async def _open_new_session(self) -> _Session:
        """拉起一代新进程与泵线程并等待就绪；失败时自行回收并抛出。"""
        session = _Session(
            generation=self._next_generation,
            ready=asyncio.Event(),
        )
        self._next_generation += 1
        self._session = session
        session.task_queue = self._ctx.Queue()
        session.result_queue = self._ctx.Queue()
        session.process = self._ctx.Process(
            target=_worker_main,
            args=(session.task_queue, session.result_queue),
            name=f"reconcile-worker-gen{session.generation}",
            daemon=True,
        )
        session.process.start()
        session.pump_thread = threading.Thread(
            target=self._pump_results,
            args=(session,),
            name=f"reconcile-result-pump-gen{session.generation}",
            daemon=True,
        )
        session.pump_thread.start()
        try:
            await self._wait_ready(session)
        except BaseException:
            await self._force_reap(session)
            if self._session is session:
                self._session = None
            raise
        session.state = "ready"
        return session

    async def _wait_ready(self, session: _Session) -> None:
        """等待子进程首个 ``free``；启动即退出或超时则抛出。"""
        deadline = self._loop.time() + self._START_TIMEOUT_SEC
        while not session.ready.is_set():
            if not self._started:
                raise RuntimeError("对账工作进程已随应用关闭而停止启动")
            if not session.process.is_alive():
                raise RuntimeError(
                    f"对账工作进程启动即退出（exitcode={session.process.exitcode}）"
                )
            if self._loop.time() >= deadline:
                raise TimeoutError("等待对账工作进程就绪超时")
            await asyncio.sleep(self._POLL_INTERVAL_SEC)

    async def _handle_exit(self, session: _Session) -> None:
        """就绪代进程退出：失败其全部在等任务、回收旧代并自动重建。"""
        if self._session is not session or session.state != "ready":
            return  # 已被其他路径（看门狗 / 泵 EOF / 关闭）处理
        session.state = "down"
        self._fail_session(session)
        try:
            await self._reap_dead(session)
        except Exception:
            # 旧代回收失败也不能阻断恢复：资源为 daemon，不影响新代启动
            session.cleaned = True
        if self._session is session:
            self._session = None
        if not self._started:
            return  # 应用正在关闭：不恢复
        await self._restart()

    async def _restart(self) -> None:
        """反复尝试重建单工作进程直至就绪或应用关闭（退避，不重放旧任务）。"""
        backoff = self._REBUILD_BACKOFF_SEC
        while self._started:
            try:
                await self._open_new_session()
                return
            except (asyncio.CancelledError, RuntimeError, TimeoutError, OSError):
                if not self._started:
                    return
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._REBUILD_BACKOFF_MAX_SEC)

    # ------------------------------------------------------------------
    # 任务接纳 / 取消（仅事件循环线程调用）
    # ------------------------------------------------------------------

    def submit(self, payload: dict[str, Any], timeout_sec: float) -> asyncio.Future | None:
        """接纳一个对账任务。

        * 工作进程不可用（异常退出后重建中 / 未就绪 / 已关闭）：抛出
          ``ReconcileWorkerUnavailable``，**立即失败、不排队**；
        * 进程就绪但满额（未结束任务达上限）：返回 ``None``；
        * 否则返回 Future，以 ``("ok", result)`` / ``("error", ...)`` /
          ``("crash", 摘要)`` 完成，超时以 ``ReconcileTimeout`` 异常完成，
          等待期间进程退出则以 ``ReconcileWorkerUnavailable`` 异常完成。
          排队时间计入 ``timeout_sec``。
        """
        if not self._started:
            return None
        session = self._session
        if (
            session is None
            or session.state != "ready"
            or session.process is None
            or not session.process.is_alive()
        ):
            # 含进程刚退出、看门狗尚未轮询到的短暂窗口：立即失败、不排队
            raise ReconcileWorkerUnavailable("对账工作进程不可用，正在自动重建")
        if len(session.tasks) >= self.MAX_INFLIGHT:
            return None
        self._next_task_id += 1
        task_id = self._next_task_id
        future = self._loop.create_future()
        session.tasks[task_id] = {
            "payload": payload,
            "future": future,
            "state": "queued",
            "timeout_handle": self._loop.call_later(
                timeout_sec, self._on_timeout, session.generation, task_id
            ),
        }
        session.queue.append(task_id)
        self._dispatch(session)
        return future

    def cancel(self, future: asyncio.Future) -> None:
        """客户端断开等导致的取消：与超时相同的撤销 / 弃置语义。"""
        session = self._session
        if session is None:
            return
        for task_id, record in session.tasks.items():
            if record["future"] is future:
                self._release_or_abandon(session, task_id)
                return

    # ------------------------------------------------------------------
    # 内部状态迁移（仅事件循环线程执行）
    # ------------------------------------------------------------------

    def _dispatch(self, session: _Session) -> None:
        """工作进程空闲且队列非空时，把队首任务派发给该代子进程。"""
        if session.state != "ready" or session.busy or not session.queue:
            return
        task_id = session.queue.popleft()
        record = session.tasks[task_id]
        record["state"] = "running"
        session.busy = True
        try:
            session.task_queue.put_nowait(("run", task_id, record["payload"]))
        except Exception:
            # 队列写入失败（如管道已关闭）：任务退回队首，等待下次派发；
            # 若进程其实已退出，看门狗会在轮询间隔内检测到并失败全部任务
            record["state"] = "queued"
            session.busy = False
            session.queue.appendleft(task_id)

    def _release_or_abandon(self, session: _Session, task_id: int) -> None:
        """超时 / 取消共用：排队任务撤销并立即释放名额；执行中任务弃置、保留名额。"""
        record = session.tasks.get(task_id)
        if record is None:
            return
        record["timeout_handle"].cancel()
        if record["state"] == "queued":
            # 任务尚未派发（父进程持有唯一派发权），直接撤销，永远不会被执行
            session.tasks.pop(task_id)
            try:
                session.queue.remove(task_id)
            except ValueError:
                pass
            record["state"] = "expired"
        elif record["state"] == "running":
            # 无法打断子进程内的计算：名额保留到 ("done", ...) 到达才释放
            record["state"] = "abandoned"

    def _on_timeout(self, generation: int, task_id: int) -> None:
        """任务到时（自接纳起算，含排队时间）；旧代计时器一律忽略。"""
        session = self._session
        if session is None or session.generation != generation:
            return
        record = session.tasks.get(task_id)
        if record is None:
            return
        self._release_or_abandon(session, task_id)
        future = record["future"]
        if not future.done():
            future.set_exception(ReconcileTimeout())

    def _fail_session(self, session: _Session) -> None:
        """进程退出：一次性失败该代所有仍在等待的任务并清空名额 / 队列 / 计时器。"""
        for record in session.tasks.values():
            record["timeout_handle"].cancel()
            future = record["future"]
            # 仅排队 / 执行中的任务仍在等待；abandoned 等已以超时 / 取消完成，
            # 不再次完成
            if record["state"] in ("queued", "running") and not future.done():
                future.set_exception(
                    ReconcileWorkerUnavailable("对账工作进程异常退出，任务未完成")
                )
        session.tasks.clear()
        session.queue.clear()
        session.busy = False

    def _on_done(self, session: _Session, task_id: int, outcome: tuple) -> None:
        """任务实际结束：释放名额；已超时 / 取消的任务结果直接丢弃。"""
        record = session.tasks.pop(task_id, None)
        if record is None:
            return  # 重复或未知完成通知：名额只释放一次
        record["timeout_handle"].cancel()
        if record["state"] == "running":
            future = record["future"]
            if not future.done():
                future.set_result(outcome)
        # 其余状态（abandoned / expired / closed）：迟到结果丢弃

    def _on_message(self, generation: int, message: tuple) -> None:
        """处理子进程消息（由泵线程经 call_soon_threadsafe 转发到事件循环）。

        消息带代际标记：只有来自当前代的消息有效，旧进程的迟到消息全部隔离丢弃，
        不会释放新任务名额或触发重复派发。
        """
        session = self._session
        if session is None or session.generation != generation:
            return
        kind = message[0]
        if kind == "free":
            if session.state == "starting" and session.ready is not None:
                session.ready.set()  # 首次空闲信号 = 该代子进程已就绪
                return
            if session.state != "ready":
                return
            session.busy = False
            self._dispatch(session)
        elif kind == "done":
            _, task_id, outcome = message
            self._on_done(session, task_id, outcome)
        # ("down",)：子进程已按序退出，由看门狗 / stop 路径统一回收

    def _on_pump_exit(self, generation: int) -> None:
        """泵线程结束（EOF / 队列关闭）：若进程确已退出则尽快触发退出处理。"""
        session = self._session
        if (
            session is None
            or session.generation != generation
            or session.state != "ready"
        ):
            return
        if not session.process.is_alive():
            # 与看门狗走同一幂等入口；状态已置 down 后重复调用直接返回
            self._loop.create_task(self._handle_exit(session))

    # ------------------------------------------------------------------
    # 进程 / 线程 / 队列回收
    # ------------------------------------------------------------------

    async def _reap_for_stop(self, session: _Session) -> None:
        """应用关闭时回收当前代：等执行中任务跑完，逾期逐级强杀，并回收泵线程。"""
        if session.cleaned:
            return
        process = session.process
        if process is not None and process.is_alive():
            session.queue.clear()
            for record in session.tasks.values():
                record["state"] = "closed"
                record["timeout_handle"].cancel()
            try:
                session.task_queue.put_nowait(("stop",))
            except Exception:
                pass
            # 子进程跑完当前任务后读到停止标记自行退出；逾期强制结束
            await asyncio.to_thread(process.join, self._STOP_TIMEOUT_SEC)
            if process.is_alive():
                process.terminate()
                await asyncio.to_thread(process.join, self._STOP_TIMEOUT_SEC)
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join, self._STOP_TIMEOUT_SEC)
        session.state = "down"
        await self._reap_resources(session)

    async def _reap_dead(self, session: _Session) -> None:
        """进程已（异常）退出后的回收：确认退出码被收割，再回收队列与泵线程。"""
        if session.cleaned:
            return
        process = session.process
        if process is not None:
            await asyncio.to_thread(process.join, self._STOP_TIMEOUT_SEC)
            if process.is_alive():  # 极端情况下仍存活则强制结束
                process.terminate()
                await asyncio.to_thread(process.join, self._STOP_TIMEOUT_SEC)
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join, self._STOP_TIMEOUT_SEC)
        await self._reap_resources(session)

    async def _force_reap(self, session: _Session) -> None:
        """无条件强制结束（用于启动失败 / 重建中途关闭）。"""
        if session.cleaned:
            return
        session.state = "down"
        process = session.process
        if process is not None and process.is_alive():
            process.terminate()
            await asyncio.to_thread(process.join, self._STOP_TIMEOUT_SEC)
            if process.is_alive():
                process.kill()
                await asyncio.to_thread(process.join, self._STOP_TIMEOUT_SEC)
        await self._reap_resources(session)

    async def _reap_resources(self, session: _Session) -> None:
        """公共收尾：关闭进程句柄、解除泵线程阻塞并回收、关闭两代队列。"""
        if session.cleaned:
            return
        session.cleaned = True
        process = session.process
        if process is not None and not process.is_alive():
            try:
                process.close()
            except Exception:
                pass
        # 先通知泵线程停止并关闭队列：阻塞在 get() 上的读端会随管道关闭抛错退出
        session.pump_stop.set()
        for q in (session.task_queue, session.result_queue):
            if q is None:
                continue
            try:
                await asyncio.to_thread(q.close)
            except Exception:
                pass
            try:
                await asyncio.to_thread(q.join_thread)
            except Exception:
                pass
        if session.pump_thread is not None and session.pump_thread.is_alive():
            await asyncio.to_thread(
                session.pump_thread.join, self._PUMP_JOIN_TIMEOUT_SEC
            )

    # ------------------------------------------------------------------
    # 泵线程：超时阻塞读结果队列，不占用事件循环，关闭时可被及时唤醒
    # ------------------------------------------------------------------

    def _pump_results(self, session: _Session) -> None:
        """把该代结果队列的消息逐条（带代际标记）转发到事件循环线程处理。"""
        try:
            while not session.pump_stop.is_set():
                try:
                    message = session.result_queue.get(
                        timeout=self._PUMP_GET_TIMEOUT_SEC
                    )
                except queue_module.Empty:
                    continue
                except (EOFError, OSError):
                    break
                loop = self._loop
                if loop is None:
                    break
                try:
                    loop.call_soon_threadsafe(
                        self._on_message, session.generation, message
                    )
                except RuntimeError:  # 事件循环已关闭
                    break
                if message[0] == "down":
                    break
        finally:
            loop = self._loop
            if loop is not None and not loop.is_closed():
                try:
                    loop.call_soon_threadsafe(
                        self._on_pump_exit, session.generation
                    )
                except RuntimeError:
                    pass
