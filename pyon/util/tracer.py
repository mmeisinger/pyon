#!/usr/bin/env python

"""General utility to trace important calls or events in the system"""

__author__ = 'Michael Meisinger'

import inspect
from collections import defaultdict

from pyon.util.containers import get_ion_ts, get_datetime_str

from ooi.logging import log

DEFAULT_CONFIG = {"enabled": True,
                  "max_entries": 5000,
                  }

# Global trace log data
trace_data = dict(trace_log=[],             # Global log
                  format_cb={},             # Scope specific formatter function
                  scope_seq={},             # Sequence number per scope
                  config=DEFAULT_CONFIG,    # Store config dict
                  )
SCOPE_COLOR = {
    "MSG": 31,
    "GW": 32,
    "DB.resources": 34,
    "DB.events": 35,
    "DB.objects": 36,
    "DB.state": 36,
    "DB._": 36,
}
DEFAULT_COLOR = 39

class CallTracer(object):
    def __init__(self, scope, formatter=None):
        self.scope = scope
        trace_data["format_cb"][scope] = formatter
        if not scope in trace_data["scope_seq"]:
            trace_data["scope_seq"][scope] = 0

    def log_call(self, log_entry, include_stack=True):
        CallTracer.log_scope_call(self.scope, log_entry, include_stack=include_stack)

    @staticmethod
    def log_scope_call(scope, log_entry, include_stack=True):
        if not trace_data["config"].get("enabled", False):
            return

        log_entry["scope"] = scope
        if not "ts" in log_entry:
            log_entry["ts"] = get_ion_ts()
        trace_data["scope_seq"][scope] += 1
        log_entry["seq"] = trace_data["scope_seq"][scope]

        if include_stack:
            stack = inspect.stack()
            frame_num = 3
            context = []
            while len(stack) > frame_num and frame_num < 15:
                exec_line = "%s:%s:%s" % (stack[frame_num][1], stack[frame_num][2], stack[frame_num][3])
                context.insert(0, exec_line)
                if exec_line.endswith("_control_flow") or exec_line.endswith("load_ion") or exec_line.endswith("spawn_process")\
                    or exec_line.endswith(":main") or exec_line.endswith(":dispatch_request"):
                    break
                frame_num += 1
            log_entry["stack"] = context

        trace_data["trace_log"].append(log_entry)
        if len(trace_data["trace_log"]) > trace_data["config"].get("max_entries", DEFAULT_CONFIG["max_entries"]) + 100:
            trace_data["trace_log"] = trace_data["trace_log"][-trace_data["config"].get("max_entries", DEFAULT_CONFIG["max_entries"]):]

    @staticmethod
    def clear_scope(scope):
        trace_data["trace_log"] = [l for l in trace_data["trace_log"] if l["scope"] != scope]

    @staticmethod
    def clear_all():
        trace_data["trace_log"] = []

    @staticmethod
    def save_log(**kwargs):
        kwargs["tofile"] = True
        CallTracer.print_log(**kwargs)

    @staticmethod
    def print_log(scope=None, max_log=10000, reverse=False, color=True, truncate=2000, stack=True,
                  count=True, filter=None, tofile=False):
        cnt = 0
        counters = defaultdict(int)
        startts, endts = 0, 0
        f = None
        try:
            if tofile:
                import datetime
                dtstr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
                path = "interface/tracelog_%s.log" % dtstr
                f = open(path, "w")
            for log_entry in reversed(trace_data["trace_log"]) if reverse else trace_data["trace_log"]:
                logscope = log_entry["scope"]
                scope_cat = logscope.split(".", 1)[0]
                if not scope or logscope.startswith(scope):
                    if filter and not filter(log_entry):
                        continue
                    formatter = trace_data["format_cb"].get(logscope, None) or CallTracer._default_formatter
                    if tofile:
                        color = False
                    log_txt = formatter(log_entry, truncate=truncate, stack=stack, color=color)
                    logscope = log_entry["scope"]      # Read again to enable formatter to specialize scope
                    if scope and logscope.startswith(scope):
                        continue
                    if tofile:
                        f.write(log_txt)
                        f.write("\n")
                    else:
                        print log_txt

                    if not startts:
                        startts = log_entry["ts"]
                    endts = log_entry["ts"]
                    if count:
                        counters[logscope] += 1
                        if scope_cat != logscope:
                            counters[scope_cat] += 1
                    cnt += 1
                if cnt >= max_log:
                    break
            if count:
                print "\nCounts:", ", ".join(["%s=%s" % (k, counters[k]) for k in sorted(counters)])
                print "Elapsed time:", abs(int(endts) - int(startts)) / 1000.0, "s"
        finally:
            if f:
                f.close()

    @staticmethod
    def _default_formatter(log_entry, **kwargs):
        truncate = kwargs.get("truncate", 0)
        color = kwargs.get("color", False)
        logscope = log_entry["scope"]
        entry_color = SCOPE_COLOR.get(logscope, DEFAULT_COLOR)
        frags = []
        if color:
            frags.append("\033[%sm" % entry_color)
        frags.append("\n%s: #%s @%s (%s) -> %s" % (log_entry['scope'], log_entry['seq'], log_entry['ts'], get_datetime_str(log_entry['ts'], show_millis=True), log_entry.get("status", "OK")))
        if truncate:
            frags.append("\n" + log_entry['statement'][:truncate])
            if len(log_entry['statement']) > truncate:
                frags.append("...")
        else:
            frags.append("\n" + log_entry['statement'])
        if color:
            frags.append("\033[0m")
        if "stack" in log_entry and kwargs.get("stack", False):
            frags.append("\n ")
            frags.append("\n ".join(log_entry["stack"]))
        return "".join(frags)

    @staticmethod
    def configure(config):
        trace_data["config"] = config or {}
        enabled = bool(config.get("enabled", False))
        trace_data["enabled"] = enabled
        if not enabled:
            CallTracer.clear_all()
