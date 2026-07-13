import time
from dataclasses import dataclass

from druncschema.controller_pb2 import RunInfo, Status

from drunc.utils.utils import get_logger


def get_status_message(controller):
    stateful = controller.stateful_node
    state_string = stateful.get_node_operational_state()
    # if state_string != stateful.get_node_operational_sub_state():
    #     state_string += f" ({stateful.get_node_operational_sub_state()})"

    msg = Status(
        state=state_string,
        sub_state=stateful.get_node_operational_sub_state(),
        in_error=stateful.node_is_in_error(),
        included=stateful.node_is_included(),
    )

    if state_string in ("initial", "configured"):
        return msg

    if controller.runinfo and controller.runinfo.get("run", None) is not None:
        run_time_since_start = 0
        run_time_at_start = controller.runinfo.get("run_time_at_start", 0)
        if run_time_at_start:
            run_time_since_start = int(time.time() - run_time_at_start)

        msg.run_info.CopyFrom(
            RunInfo(
                run_type=controller.runinfo.get("production_vs_test", ""),
                trigger_rate=controller.runinfo.get("trigger_rate", 0.0),
                run_number=controller.runinfo["run"],
                disable_data_storage=controller.runinfo.get(
                    "disable_data_storage", False
                ),
                run_time_at_start=int(controller.runinfo.get("run_time_at_start", 0)),
                run_time_since_start=run_time_since_start,
                run_config_file=controller.configuration.oks_path,
                run_config_name=controller.configuration.oks_key.session,
            )
        )

    return msg


def get_detector_name(configuration) -> str:
    detector_name = None
    log = get_logger("controller.core.get_detector_name")
    raw = getattr(configuration, "_raw", None)
    if raw is not None and hasattr(raw, "contains") and len(raw.contains) > 0:
        if len(raw.contains) > 0:
            log.debug(
                f"Application {raw.id} has multiple contains, using the first one"
            )
        detector_name = raw.contains[0].id.replace("-", "_").replace("_", " ")
    else:
        log.debug(
            f'Application {getattr(raw, "id", "?")} has no "contains" relation, hence no detector'
        )
    return detector_name


def get_segment_lookup_timeout(segment_conf, base_timeout=60):
    def recurse_segment(segment, recursion_count: int = 1) -> int:
        if segment.segments == []:
            return recursion_count

        max_recursion = 0
        for child_segment in segment.segments:
            child_recursion_count = recurse_segment(child_segment, recursion_count + 1)
            if child_recursion_count > max_recursion:
                max_recursion = child_recursion_count
        return max_recursion

    recursion_count = recurse_segment(segment_conf, 1)
    return base_timeout * recursion_count


@dataclass
class ControllerMonitoringMetrics:
    """Store the metrics that the OpMon Controller publishes"""

    run_type: str = ""
    trigger_rate: float = 0.0
    run_number: int = 0
    disable_data_storage: bool = False
    run_time_at_start: int = 0
    run_time_since_start: int = 0
