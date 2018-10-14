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

// Action = Observation | Command | Query
pub enum Action<O, C, Q>
where
    O: Send,
    C: Send,
    Q: Send,
{
    Observation(O),
    Command(CommandResponseSender, C),
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

pub fn forward_observation<O, C, Q>(action_tx: &ActionSender<Action<O, C, Q>>, observation: O)
where
    O: Send,
    C: Send,
    Q: Send,
{
    trace!("Forwarding observation");
    if let Err(err) = action_tx.unbounded_send(Action::Observation(observation)) {
        error!("Failed to forward observation: {}", err);
    }
}

fn await_response<T>(response_rx: ResponseReceiver<T>) -> impl Future<Item = T, Error = Error> {
    response_rx
        .map_err(|_: Canceled| {
            let msg = "No response received";
            error!("{}", msg);
            format_err!("{}", msg)
        }).and_then(move |response| response)
}

fn new_command_response_channel() -> (CommandResponseSender, CommandResponseReceiver) {
    oneshot::channel()
}

pub fn invoke_command<O, C, Q>(
    request_tx: &RequestSender<Action<O, C, Q>>,
    command: C,
) -> impl Future<Item = (), Error = Error>
where
    O: Send,
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
    ).and_then(|()| await_response(response_rx))
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
    ).and_then(|()| await_response(response_rx))
}

pub mod messaging;

pub mod scheduling;
