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
//! - [`Credits`]: the work a driver may have claimed from the engine. Every
//!   work driver's pollers take a credit before each poll, so a driver
//!   claims only the work it has room to run, and the credit is held until
//!   that work has been reported.
//!
//! - [`SharedBudget`]: one budget the workflow, task and actor drivers may share
//!   ([`WorkBudget`](crate::WorkBudget)). Each credit then also takes its
//!   driver's weight in units of it, so the drivers' own limits become
//!   ceilings within one budget for the whole worker.
//!
//! This is where the worker's resource model is meant to grow: pressure
//! signals (process CPU and memory, and a signal from the language runtime)
//! feeding admission.

mod budget;
mod credits;
mod in_flight;
mod shared;

pub(crate) use budget::{SlotBudget, SlotPermit};
pub(crate) use credits::{Credit, Credits};
pub(crate) use in_flight::InFlight;
pub(crate) use shared::{fit, BudgetShare, Polls, SharedBudget};
