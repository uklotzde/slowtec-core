use super::*;

use futures;
use futures::sync::{mpsc, oneshot};
use futures::{Canceled, Future};

type MessageSender<T> = mpsc::UnboundedSender<T>;
type MessageReceiver<T> = mpsc::UnboundedReceiver<T>;

type RequestSender<T> = MessageSender<T>;
type RequestReceiver<T> = MessageReceiver<T>;

type Response<T> = Result<T, Error>;
type ResponseSender<T> = oneshot::Sender<Response<T>>;
type ResponseReceiver<T> = oneshot::Receiver<Response<T>>;

pub type CommandResponse = Response<()>;
pub type CommandResponseSender = ResponseSender<()>;
pub type CommandResponseReceiver = ResponseReceiver<()>;

pub type QueryResponse<R> = Response<R>;
pub type QueryResponseSender<R> = ResponseSender<R>;
pub type QueryResponseReceiver<R> = ResponseReceiver<R>;

// Action = Stimulus | Command | Query
pub enum Action<S, C, Q>
where
    S: Send,
    C: Send,
    Q: Send,
{
    // A stimulus is a one-way message that triggers a behavior, e.g. a state
    // change. Stimuli are executed without any response. Errors during execution
    // can only be logged and/or published by sending out notifications.
    // Notifications might also be published if the state of the actor has
    // changed during execution.
    Stimulus(S),
    // A command is a bi-directional message that triggers a reaction, e.g.
    // a state change. Commands are executed and answered by replying with
    // either an empty(!) response on success or an error upon any failure
    // or rejection. Notifications might be published if the state of the
    // actor has changed during execution.
    Command(CommandResponseSender, C),
    // A query is an idempotent action that must not change the state.
    // Queries are executed and answered by replying with a response. No
    // notifications are sent, since the state didn't change.
    Query(Q),
}

pub type ActionSender<A> = RequestSender<A>;
pub type ActionReceiver<A> = RequestReceiver<A>;

pub type NotificationSender<N> = MessageSender<N>;
pub type NotificationReceiver<N> = MessageReceiver<N>;

pub fn new_request_channel<T>() -> (RequestSender<T>, RequestReceiver<T>) {
    mpsc::unbounded()
}

pub fn new_notification_channel<N>() -> (NotificationSender<N>, NotificationReceiver<N>) {
    mpsc::unbounded()
}

pub fn reply<R>(response_tx: ResponseSender<R>, response: Response<R>) {
    trace!("Sending response");
    if let Err(_response) = response_tx.send(response) {
        error!("Failed to send response");
    }
}

pub fn notify<N>(notification_tx: &NotificationSender<N>, notification: N) {
    trace!("Sending notification");
    if let Err(err) = notification_tx.unbounded_send(notification) {
        error!("Failed to send notification: {}", err);
    }
}

pub fn forward_stimulus<S, C, Q>(action_tx: &ActionSender<Action<S, C, Q>>, stimulus: S)
where
    S: Send,
    C: Send,
    Q: Send,
{
    trace!("Forwarding stimulus");
    if let Err(err) = action_tx.unbounded_send(Action::Stimulus(stimulus)) {
        error!("Failed to submit stimulus: {}", err);
    }
}

fn await_response<T>(response_rx: ResponseReceiver<T>) -> impl Future<Item = T, Error = Error> {
    response_rx
        .map_err(|_: Canceled| {
            let msg = "No response received";
            error!("{}", msg);
            format_err!("{}", msg)
        })
        .and_then(move |response| response)
}

fn new_command_response_channel() -> (CommandResponseSender, CommandResponseReceiver) {
    oneshot::channel()
}

pub fn invoke_command<S, C, Q>(
    request_tx: &RequestSender<Action<S, C, Q>>,
    command: C,
) -> impl Future<Item = (), Error = Error>
where
    S: Send,
    C: Send,
    Q: Send,
{
    trace!("Invoking command");
    let (response_tx, response_rx) = new_command_response_channel();
    let action = Action::Command(response_tx, command);
    futures::future::result(
        request_tx
            .unbounded_send(action)
            .map_err(|err| format_err!("Failed to submit command: {}", err)),
    )
    .and_then(|()| await_response(response_rx))
}

pub fn new_query_response_channel<R>() -> (QueryResponseSender<R>, QueryResponseReceiver<R>) {
    oneshot::channel()
}

pub fn invoke_query<Q, R>(
    request_tx: &RequestSender<Q>,
    query: Q,
    response_rx: QueryResponseReceiver<R>,
) -> impl Future<Item = R, Error = Error> {
    trace!("Invoking query");
    futures::future::result(
        request_tx
            .unbounded_send(query)
            .map_err(|err| format_err!("Failed to submit query: {}", err)),
    )
    .and_then(|()| await_response(response_rx))
}

pub mod messaging;

pub mod scheduling;
