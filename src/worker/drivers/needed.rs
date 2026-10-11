//! Which work drivers a worker starts, from what it registered.
//!
//! Each running driver long-polls the engine. A worker that starts all of
//! them whatever it registered holds open polls for work it cannot run: a
//! task-only worker polling for workflows, a workflow-only worker polling for
//! tasks. A worker starts only the drivers its registrations need.

/// The task that creates a worker session. Every worker can run it, so it
/// does not make a worker one that runs tasks.
pub const SESSION_CREATE_TASK: &str = "__orcher_create_session";

/// The task that completes a worker session; see [`SESSION_CREATE_TASK`].
pub const SESSION_COMPLETE_TASK: &str = "__orcher_complete_session";

/// Task types the SDK registers on every worker rather than the user.
pub const BUILT_IN_TASK_TYPES: &[&str] = &[SESSION_CREATE_TASK, SESSION_COMPLETE_TASK];

/// The work drivers a worker needs.
///
/// ```
/// use orcher_sdk_core::worker::NeededDrivers;
///
/// // A worker that only runs tasks polls only for tasks.
/// let needed = NeededDrivers::for_registrations(&[] as &[&str], &["send_email"], &[] as &[&str]);
/// assert!(!needed.workflows);
/// assert!(needed.tasks);
/// assert!(!needed.actors);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct NeededDrivers {
    /// Start the workflow driver: the worker registered a workflow.
    pub workflows: bool,
    /// Start the task driver: the worker registered a task of its own. The
    /// built-in session tasks ([`BUILT_IN_TASK_TYPES`]) do not count; a
    /// worker that runs no tasks has no use for a session.
    pub tasks: bool,
    /// Start the actor driver: the worker registered an actor.
    pub actors: bool,
}

impl NeededDrivers {
    /// The drivers for a worker that registered these workflow, task and
    /// actor types.
    pub fn for_registrations<W, T, A>(
        workflow_types: &[W],
        task_types: &[T],
        actor_types: &[A],
    ) -> Self
    where
        W: AsRef<str>,
        T: AsRef<str>,
        A: AsRef<str>,
    {
        Self {
            workflows: !workflow_types.is_empty(),
            tasks: task_types
                .iter()
                .any(|task| !BUILT_IN_TASK_TYPES.contains(&task.as_ref())),
            actors: !actor_types.is_empty(),
        }
    }

    /// Whether the worker registered nothing it could run. Such a worker
    /// would poll for nothing; an SDK refuses to start it instead.
    pub fn is_empty(&self) -> bool {
        !(self.workflows || self.tasks || self.actors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NONE: &[&str] = &[];

    #[test]
    fn each_driver_starts_only_for_what_was_registered() {
        let only_workflows = NeededDrivers::for_registrations(&["checkout"], NONE, NONE);
        assert_eq!(
            (
                only_workflows.workflows,
                only_workflows.tasks,
                only_workflows.actors
            ),
            (true, false, false)
        );
        let only_tasks = NeededDrivers::for_registrations(NONE, &["charge"], NONE);
        assert_eq!(
            (only_tasks.workflows, only_tasks.tasks, only_tasks.actors),
            (false, true, false)
        );
        let only_actors = NeededDrivers::for_registrations(NONE, NONE, &["Cart"]);
        assert_eq!(
            (only_actors.workflows, only_actors.tasks, only_actors.actors),
            (false, false, true)
        );
        let all = NeededDrivers::for_registrations(&["checkout"], &["charge"], &["Cart"]);
        assert_eq!((all.workflows, all.tasks, all.actors), (true, true, true));
    }

    /// The session tasks are registered on every worker, so on their own
    /// they do not make it a worker that runs tasks.
    #[test]
    fn built_in_session_tasks_do_not_start_the_task_driver() {
        let needed = NeededDrivers::for_registrations(&["checkout"], BUILT_IN_TASK_TYPES, NONE);
        assert!(!needed.tasks);
        let with_own = NeededDrivers::for_registrations(
            &["checkout"],
            &[SESSION_CREATE_TASK, "charge", SESSION_COMPLETE_TASK],
            NONE,
        );
        assert!(with_own.tasks);
    }

    #[test]
    fn a_worker_that_registered_nothing_is_empty() {
        assert!(NeededDrivers::for_registrations(NONE, NONE, NONE).is_empty());
        assert!(NeededDrivers::for_registrations(NONE, BUILT_IN_TASK_TYPES, NONE).is_empty());
        assert!(!NeededDrivers::for_registrations(NONE, NONE, &["Cart"]).is_empty());
    }

    /// The names every SDK registers its session handlers under.
    #[test]
    fn the_session_task_names_are_the_ones_the_sdks_use() {
        assert_eq!(SESSION_CREATE_TASK, "__orcher_create_session");
        assert_eq!(SESSION_COMPLETE_TASK, "__orcher_complete_session");
    }
}
