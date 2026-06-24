//package: mpsc
use std::sync::{Arc, Mutex};

use arcstr::literal;
use par_runtime::readback::Handle;
use par_runtime::registry::{DefinitionRef, ExternalDef, PackageRef};
use tokio::sync::mpsc;

macro_rules! mpsc_mailbox_external {
    ($name:literal, $f:path $(, $arg:expr)*) => {
        inventory::submit!(ExternalDef {
            path: DefinitionRef {
                package: PackageRef::MPSC,
                path: &[],
                module: "Mailbox",
                name: $name,
            },
            f: |handle| Box::pin($f(handle $(, $arg)*)),
        });
    };
}

mpsc_mailbox_external!("New", mailbox_new);

struct MailboxState {
    // Keep sender availability, queued accounting, and closed state under one
    // lock so close and trySend have a single linearization point.
    inner: Mutex<MailboxInner>,
    dead_letters: mpsc::UnboundedSender<DeadLetter>,
}

struct MailboxInner {
    sender: Option<mpsc::Sender<Handle>>,
    capacity: usize,
    queued: usize,
    closed: bool,
}

struct DeadLetter {
    reason: DeadLetterReason,
    message: Handle,
}

#[derive(Clone, Copy)]
enum DeadLetterReason {
    Full,
    Closed,
}

async fn mailbox_new(mut handle: Handle) {
    let capacity = usize::try_from(handle.receive().nat().await)
        .unwrap_or(usize::MAX)
        .max(1);
    eprintln!("[mailbox] new capacity={capacity}");
    let (tx, rx) = mpsc::channel::<Handle>(capacity);
    let (dead_tx, dead_rx) = mpsc::unbounded_channel::<DeadLetter>();
    let state = Arc::new(MailboxState {
        inner: Mutex::new(MailboxInner {
            sender: Some(tx),
            capacity,
            queued: 0,
            closed: false,
        }),
        dead_letters: dead_tx,
    });

    let sender_handle = handle.send();
    provide_sender(sender_handle, Arc::clone(&state));

    let receiver_pair = handle.send();
    provide_dead_letters(handle, dead_rx);
    // Receiver close is a sequencing signal in Par code (`let ! = receiver.close`).
    // Serving it directly avoids a spawn-scheduling race with the next trySend.
    provide_receiver(receiver_pair, rx, Arc::clone(&state)).await;
}

fn provide_sender(handle: Handle, state: Arc<MailboxState>) {
    handle.provide_box(move |mut handle| {
        let state = Arc::clone(&state);
        async move {
            match handle.case().await.as_str() {
                "trySend" => {
                    handle_try_send(handle, state);
                }
                "close" => {
                    close_mailbox(&state);
                    handle.break_();
                }
                _ => unreachable!(),
            }
        }
    });
}

fn handle_try_send(mut handle: Handle, state: Arc<MailboxState>) {
    let message = handle.receive();
    let result = handle.send();

    match try_send_message(&state, message) {
        SendAttempt::Sent => provide_send_result(result, Ok(())),
        SendAttempt::Rejected { reason, message } => {
            emit_dead_letter(&state, reason, message);
            provide_send_result(result, Err(reason));
        }
    }

    provide_sender(handle, state);
}

enum SendAttempt {
    Sent,
    Rejected {
        reason: DeadLetterReason,
        message: Handle,
    },
}

fn try_send_message(state: &MailboxState, message: Handle) -> SendAttempt {
    match reserve_slot(state) {
        ReserveSlot::Closed => {
            eprintln!("[mailbox] trySend rejected: closed");
            rejected_send(DeadLetterReason::Closed, message)
        }
        ReserveSlot::Full => {
            eprintln!("[mailbox] trySend rejected: full");
            rejected_send(DeadLetterReason::Full, message)
        }
        ReserveSlot::Reserved(sender) => match sender.try_send(message) {
            Ok(()) => {
                eprintln!("[mailbox] try_send ok");
                SendAttempt::Sent
            }
            Err(mpsc::error::TrySendError::Full(message)) => {
                eprintln!("[mailbox] tokio try_send full");
                release_slot(state);
                rejected_send(DeadLetterReason::Full, message)
            }
            Err(mpsc::error::TrySendError::Closed(message)) => {
                eprintln!("[mailbox] tokio try_send closed");
                release_slot(state);
                rejected_send(DeadLetterReason::Closed, message)
            }
        },
    }
}

fn rejected_send(reason: DeadLetterReason, message: Handle) -> SendAttempt {
    SendAttempt::Rejected { reason, message }
}

fn provide_send_result(mut result: Handle, send_result: Result<(), DeadLetterReason>) {
    match send_result {
        Ok(()) => {
            result.signal(literal!("ok"));
        }
        Err(reason) => {
            result.signal(literal!("err"));
            signal_dead_letter_reason(&mut result, reason);
        }
    }
    result.break_();
}

enum ReserveSlot {
    Reserved(mpsc::Sender<Handle>),
    Full,
    Closed,
}

fn reserve_slot(state: &MailboxState) -> ReserveSlot {
    let mut inner = state.inner.lock().expect("mailbox state lock failed");

    if inner.closed {
        return ReserveSlot::Closed;
    }

    if inner.queued >= inner.capacity {
        return ReserveSlot::Full;
    }

    let Some(sender) = inner.sender.clone() else {
        inner.closed = true;
        return ReserveSlot::Closed;
    };

    inner.queued += 1;
    ReserveSlot::Reserved(sender)
}

fn release_slot(state: &MailboxState) {
    let mut inner = state.inner.lock().expect("mailbox state lock failed");
    inner.queued = inner
        .queued
        .checked_sub(1)
        .expect("mailbox queued count underflowed");
}

fn close_mailbox(state: &MailboxState) {
    let mut inner = state.inner.lock().expect("mailbox state lock failed");
    inner.closed = true;
    inner.sender.take();
}

fn emit_dead_letter(state: &MailboxState, reason: DeadLetterReason, message: Handle) {
    if state
        .dead_letters
        .send(DeadLetter { reason, message })
        .is_err()
    {
        // If the probe was closed, there is no typed owner left for the rejected message.
    }
}

async fn provide_receiver(
    mut handle: Handle,
    mut receiver: mpsc::Receiver<Handle>,
    state: Arc<MailboxState>,
) {
    loop {
        match handle.case().await.as_str() {
            "receive" => {
                let mut result = handle.send();
                eprintln!("[mailbox] receive entered");
                let message = receiver.recv().await;
                match message {
                    Some(message) => {
                        release_slot(&state);
                        eprintln!("[mailbox] receive ok");
                        result.signal(literal!("ok"));
                        result.link(message);
                    }
                    None => {
                        eprintln!("[mailbox] receive closed");
                        result.signal(literal!("err"));
                        result.signal(literal!("closed"));
                        result.break_();
                    }
                }
            }
            "close" => {
                close_mailbox(&state);
                handle.break_();
                return;
            }
            _ => unreachable!(),
        }
    }
}

fn provide_dead_letters(handle: Handle, receiver: mpsc::UnboundedReceiver<DeadLetter>) {
    let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
    provide_dead_letters_loop(handle, receiver);
}

fn provide_dead_letters_loop(
    handle: Handle,
    receiver: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<DeadLetter>>>,
) {
    handle.concurrently(move |mut handle| async move {
        loop {
            match handle.case().await.as_str() {
                "receive" => {
                    let mut result = handle.send();
                    eprintln!("[mailbox] deadLetters receive entered");
                    let dead = receiver.lock().await.recv().await;
                    match dead {
                        Some(dead) => {
                            eprintln!("[mailbox] deadLetters receive ok");
                            result.signal(literal!("ok"));
                            let mut reason = result.send();
                            signal_dead_letter_reason(&mut reason, dead.reason);
                            reason.break_();
                            result.link(dead.message);
                        }
                        None => {
                            eprintln!("[mailbox] deadLetters receive closed");
                            result.signal(literal!("err"));
                            result.signal(literal!("closed"));
                            result.break_();
                        }
                    }
                }
                "close" => {
                    handle.break_();
                    return;
                }
                _ => unreachable!(),
            }
        }
    });
}

fn signal_dead_letter_reason(handle: &mut Handle, reason: DeadLetterReason) {
    match reason {
        DeadLetterReason::Full => handle.signal(literal!("full")),
        DeadLetterReason::Closed => handle.signal(literal!("closed")),
    }
}
