"""控制面自定义异常（骨架桩）。"""


class FleetError(Exception):
    """所有控制面异常的基类。"""


class DuplicateSubmitError(FleetError):
    """同一提交标识重复提交（语义为幂等命中，不产生重复步骤）。"""


class UnknownTaskError(FleetError):
    """查询/操作了不存在的任务。"""


class InvalidSpecError(FleetError):
    """任务声明非法（重复步骤 id、依赖不存在、DAG 有环等）。"""


class DispatchRejectedError(FleetError):
    """派发被拒绝（携带可区分原因）。"""


class RecoverableExecError(FleetError):
    """可恢复的执行失败（超时/异常退出），允许重试。"""


class NonRecoverableExecError(FleetError):
    """不可恢复的执行失败，步骤直接进入 FAILED 终态。"""
