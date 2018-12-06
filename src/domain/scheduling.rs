use chrono::DateTime;

pub trait TaskSchedule {
    type TimeZone: chrono::TimeZone<Offset = Self::TimeZoneOffset>;

    // TODO: Use a trait bound on Self::TimeZone::Offset instead of
    // an additional associated type?!
    type TimeZoneOffset: chrono::offset::Offset + std::fmt::Display;

    /// Determines the next point in time for (re-)scheduling this
    /// task.
    ///
    /// The returned point in time must not occur before `after`. If no
    /// next point in time exists or if this schedule is inactive then
    /// `None` should be returned.
    ///
    /// # Arguments
    ///
    /// * `after` - Some point in time
    fn schedule_next_after(
        &self,
        after: &DateTime<Self::TimeZone>,
    ) -> Option<DateTime<Self::TimeZone>>;
}

pub trait TaskScheduler {
    type TaskSchedule: TaskSchedule;

    fn now(&self) -> DateTime<<Self::TaskSchedule as TaskSchedule>::TimeZone>;

    /// Callback for dispatching and reschedule an expired task.
    fn dispatch_and_reschedule_expired_task(
        &self,
        now: &DateTime<<Self::TaskSchedule as TaskSchedule>::TimeZone>,
        task_schedule: Self::TaskSchedule,
    ) -> Option<Self::TaskSchedule>;
}
