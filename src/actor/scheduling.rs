use super::*;

use crate::domain::scheduling::*;

use chrono::{DateTime, SecondsFormat};

use futures::{Async, Future, Stream};

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::timer::{
    delay_queue::{Expired, Key as DelayQueueKey},
    DelayQueue,
};

enum DelayQueueItem<T> {
    TaskSchedule(T),
    KeepAlive,
}

// The valid duration is limited by this upper bound that is
// reserved for the keep alive token!
// TODO: The maximum acceptable value of 795 days that does
// not cause an internal panic has been discovered experimentally.
// No references about this limit can be found in the Tokio docs!?
const MAX_DELAY_TIMEOUT: Duration = Duration::from_secs(795 * 24 * 60 * 60);

struct ScheduledTaskQueue<T> {
    task_scheduler: Box<dyn TaskScheduler<TaskSchedule = T> + Send>,

    upcoming_tasks: DelayQueue<DelayQueueItem<T>>,

    keep_alive_key: DelayQueueKey,
}

fn format_datetime<Z: chrono::TimeZone>(dt: &DateTime<Z>) -> String
where
    <Z as chrono::TimeZone>::Offset: std::fmt::Display,
{
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl<T> ScheduledTaskQueue<T>
where
    T: TaskSchedule + std::fmt::Debug,
    <T::TimeZone as chrono::TimeZone>::Offset: std::fmt::Display,
{
    pub fn new(task_scheduler: Box<dyn TaskScheduler<TaskSchedule = T> + Send>) -> Self {
        let mut upcoming_tasks = DelayQueue::new();
        let keep_alive_key = upcoming_tasks.insert(DelayQueueItem::KeepAlive, MAX_DELAY_TIMEOUT);
        Self {
            task_scheduler,
            upcoming_tasks,
            keep_alive_key,
        }
    }

    fn keep_alive(&mut self) {
        self.upcoming_tasks
            .reset(&self.keep_alive_key, MAX_DELAY_TIMEOUT);
    }

    pub fn handle_expired(&mut self, expired: Expired<DelayQueueItem<T>>) {
        match expired.into_inner() {
            DelayQueueItem::TaskSchedule(task_schedule) => self.reschedule_expired(task_schedule),
            DelayQueueItem::KeepAlive => self.reschedule_all(Default::default()),
        }
    }

    fn reschedule_expired(&mut self, task_schedule: T) {
        let now = self.task_scheduler.now();
        debug!("{:?} expired at {}", task_schedule, now);
        let task_reschedule = self
            .task_scheduler
            .dispatch_and_reschedule_expired_task(&now, task_schedule);
        if let Some(task_schedule) = task_reschedule {
            self.schedule_next(&now, task_schedule);
            self.keep_alive();
        }
    }

    fn schedule_next(
        &mut self,
        now: &DateTime<T::TimeZone>,
        task_schedule: T,
    ) -> Option<DateTime<T::TimeZone>> {
        if let Some(next_after_now) = task_schedule.schedule_next_after(now) {
            debug_assert!(next_after_now > *now);
            debug!(
                "Rescheduling {:?} at {}",
                task_schedule,
                format_datetime(&next_after_now)
            );
            let timeout = (next_after_now.clone() - now.clone()).to_std().unwrap();
            if timeout < MAX_DELAY_TIMEOUT {
                self.upcoming_tasks
                    .insert(DelayQueueItem::TaskSchedule(task_schedule), timeout);
                Some(next_after_now)
            } else {
                error!(
                    "Cannot reschedule {:?} at {}: Maximum timeout duration exceeded: {:?} >= {:?}",
                    task_schedule,
                    format_datetime(&next_after_now),
                    timeout,
                    MAX_DELAY_TIMEOUT
                );
                None
            }
        } else {
            debug!("Finished {:?}", task_schedule);
            None
        }
    }

    pub fn reschedule_all(&mut self, task_schedules: Vec<T>) {
        // Clear the delay queue, i.e. discard all tasks
        debug!("Discarding all scheduled tasks");
        self.upcoming_tasks.clear();
        // Repopulate the delay queue with the given irrigation schedules
        debug_assert!(self.upcoming_tasks.is_empty());
        self.upcoming_tasks.reserve(task_schedules.len() + 1);
        let now = self.task_scheduler.now();
        task_schedules.into_iter().for_each(|task_schedule| {
            self.schedule_next(&now, task_schedule);
        });
        self.keep_alive_key = self
            .upcoming_tasks
            .insert(DelayQueueItem::KeepAlive, MAX_DELAY_TIMEOUT);
    }
}

impl<T> Stream for ScheduledTaskQueue<T> {
    type Item = <DelayQueue<DelayQueueItem<T>> as Stream>::Item;
    type Error = <DelayQueue<DelayQueueItem<T>> as Stream>::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.upcoming_tasks.poll()
    }
}

// This mutex will only be locked within the same executor,
// though maybe subsequently by different threads. There
// won't be any lock contention, i.e. no thread will block
// when locking this mutex! It is only required to satisfy
// the Send bound for the enclosing context.
struct ScheduledTasks<T>(Arc<Mutex<ScheduledTaskQueue<T>>>);

impl<T> ScheduledTasks<T> {
    pub fn new(inner: ScheduledTaskQueue<T>) -> Self {
        ScheduledTasks(Arc::new(Mutex::new(inner)))
    }

    pub fn lock_inner(&mut self) -> MutexGuard<ScheduledTaskQueue<T>> {
        // Even a try_lock() should never fail, but we prefer
        // the blocking variant to be safe!
        let lock_result = self.0.lock();
        debug_assert!(lock_result.is_ok());
        match lock_result {
            Ok(guard) => guard,
            Err(err) => {
                error!("Failed to lock mutex of scheduled tasks: {}", err);
                unreachable!();
            }
        }
    }
}

impl<T> Clone for ScheduledTasks<T> {
    fn clone(&self) -> Self {
        ScheduledTasks(self.0.clone())
    }
}

impl<T> Stream for ScheduledTasks<T> {
    type Item = <DelayQueue<DelayQueueItem<T>> as Stream>::Item;
    type Error = <DelayQueue<DelayQueueItem<T>> as Stream>::Error;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.lock_inner().poll()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TaskSchedulingSignal {}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskSchedulingCommand<T: TaskSchedule> {
    RescheduleAll(Vec<T>),
}

#[derive(Debug, Clone, Copy)]
pub enum TaskSchedulingQuery {}

pub type TaskSchedulingAction<T> =
    Action<TaskSchedulingSignal, TaskSchedulingCommand<T>, TaskSchedulingQuery>;

pub type TaskSchedulingActionSender<T> = ActionSender<TaskSchedulingAction<T>>;
type TaskSchedulingActionReceiver<T> = ActionReceiver<TaskSchedulingAction<T>>;

#[derive(Debug, Clone, Copy)]
pub enum TaskSchedulingNotification {}

type TaskSchedulingNotificationSender = NotificationSender<TaskSchedulingNotification>;
pub type TaskSchedulingNotificationReceiver = NotificationReceiver<TaskSchedulingNotification>;

pub struct TaskSchedulingActor<T: TaskSchedule> {
    // Currently unused
    _notification_tx: TaskSchedulingNotificationSender,

    scheduled_tasks: ScheduledTasks<T>,
}

impl<T> TaskSchedulingActor<T>
where
    T: TaskSchedule + Send + std::fmt::Debug,
{
    pub fn evoke(
        task_scheduler: Box<dyn TaskScheduler<TaskSchedule = T> + Send>,
    ) -> (
        impl Future<Item = (), Error = ()>,
        TaskSchedulingActionSender<T>,
        TaskSchedulingNotificationReceiver,
    ) {
        let (action_tx, action_rx) = new_request_channel();
        let (_notification_tx, notification_rx) = new_notification_channel();
        let _ping_action_tx = action_tx.clone();
        let event_loop = futures::lazy(move || {
            info!("Starting scheduler");
            // Lazy instantiation is essential to implicitly attach the
            // DelayQueue to the Timer of the corresponding Runtime in
            // DelayQueue::new()!!!
            Ok(ScheduledTasks::new(ScheduledTaskQueue::new(task_scheduler)))
        })
        .and_then(move |scheduled_tasks| {
            // Create a handler for expired tasks
            let mut expired_tasks = scheduled_tasks.clone();
            let expired_tasks_handler = scheduled_tasks
                .clone()
                .for_each(move |expired| {
                    expired_tasks.lock_inner().handle_expired(expired);
                    Ok(())
                })
                .map_err(|err| error!("Failed to handle expired tasks: {}", err));
            Ok((scheduled_tasks, expired_tasks_handler))
        })
        .and_then(move |(scheduled_tasks, expired_tasks_handler)| {
            // Create a handler for actions...
            let action_handler = Self {
                _notification_tx,
                scheduled_tasks,
            }
            .handle_actions(action_rx);
            // ...and combine the handlers.
            // Warning: The order for combining both futures seems to matter!!
            // Using select() on action_handler followed by expired_tasks_handler
            // as an argument works as expected. When reversing this order any
            // previously rescheduled tasks are retained and don't expire until
            // the next action is received.
            action_handler
                .select(expired_tasks_handler)
                .map(|_| ())
                .map_err(|_| ())
        });
        (event_loop, action_tx, notification_rx)
    }

    fn handle_actions(
        mut self,
        action_rx: TaskSchedulingActionReceiver<T>,
    ) -> impl Future<Item = (), Error = ()> {
        action_rx.for_each(move |action| {
            self.handle_action(action);
            Ok(())
        })
    }

    fn handle_action(&mut self, action: TaskSchedulingAction<T>) {
        match action {
            Action::Signal(signal) => match signal {},
            Action::Command(response_tx, command) => self.handle_command(response_tx, command),
            Action::Query(query) => match query {},
        }
    }

    fn handle_command(
        &mut self,
        response_tx: CommandResponseSender,
        command: TaskSchedulingCommand<T>,
    ) {
        let result = match command {
            TaskSchedulingCommand::RescheduleAll(task_schedules) => {
                self.scheduled_tasks
                    .lock_inner()
                    .reschedule_all(task_schedules);
                Ok(())
            }
        };
        reply(response_tx, result);
    }
}
