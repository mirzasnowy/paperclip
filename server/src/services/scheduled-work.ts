import { readTaskDrain } from "./task-admission.js";
import { registerDeadlineWork } from "./deadline-work.js";
import type { createDeliveryWorkCoordinator } from "./delivery-work-coordinator.js";
import { DELIVERY_QUEUES } from "./delivery-work-notifications.js";

export function registerScheduledWork(
  coordinator: Pick<ReturnType<typeof createDeliveryWorkCoordinator>, "register">,
  options: {
    retryMs: number;
    tickAgentTimers: () => Promise<unknown>;
    tickIssueMonitors: (signal: AbortSignal) => Promise<unknown>;
    nextIssueMonitorAt: () => Promise<number | null>;
    tickRoutines: (signal: AbortSignal) => Promise<unknown>;
    nextRoutineRunAt: () => Promise<number | null>;
    canRun: () => boolean;
  },
) {
  const routines = registerDeadlineWork(coordinator, DELIVERY_QUEUES.routineSchedules, {
    run: options.tickRoutines,
    next: options.nextRoutineRunAt,
    canRun: options.canRun,
    retryMs: options.retryMs,
  });
  const monitors = registerDeadlineWork(coordinator, DELIVERY_QUEUES.issueMonitors, {
    run: options.tickIssueMonitors,
    next: options.nextIssueMonitorAt,
    canRun: () => options.canRun() && readTaskDrain(new Date()) === null,
    retryMs: options.retryMs,
    notificationCoalesceMs: options.retryMs,
  });


  const timers = coordinator.register(DELIVERY_QUEUES.agentTimers, {
    retryMs: options.retryMs,
    run: options.tickAgentTimers,
    hasPending: async () => false,
    nextRunAt: () => null,
  });
  return { routines, monitors, timers };
}
