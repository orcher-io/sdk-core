//! The local governor: how much work a driver may have in hand, and the work
//! it is still finishing when it stops.
//!
//! Today this is the capacity each work driver already had, given one home:
//!
//! - [`SlotBudget`]: a driver's slots. The workflow, task and actor drivers
//!   each have their own budget, sized from
//!   [`CompletionRetryConfig::max_in_flight`](crate::worker::CompletionRetryConfig::max_in_flight)
//!   or else the driver's `max_concurrent_executions`. A driver takes a slot
//!   before it accepts anything that will produce a report, and the
//!   [`SlotPermit`] is held until that report has been sent or given up on.
//!   With every slot held, the driver takes neither results nor new work, so
//!   a backlog of reports holds the language SDK and the pollers back.
//! - [`InFlight`]: the reports a driver is still sending, each from a task
//!   of its own, and the bounded wait for them when the driver stops.
//!
//! This is where the worker's resource model is meant to grow: one budget
//! shared across the three drivers, with today's per-driver limits as
//! ceilings within it; pressure signals (process CPU and memory, and a
//! signal from the language runtime) feeding admission; and polling that
//! asks only for the work there is capacity to run. None of that is built
//! yet, and nothing here changes how the drivers poll.

mod budget;
mod in_flight;

pub(crate) use budget::{SlotBudget, SlotPermit};
pub(crate) use in_flight::InFlight;
