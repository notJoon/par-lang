//package: mpsc
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

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
    sender: Mutex<Option<mpsc::Sender<Handle>>>,
    capacity: usize,
    queued: AtomicUsize,
    closed: AtomicBool,
    dead_letters: mpsc::UnboundedSender<DeadLetter>,
}

struct DeadLetter {
    reason: DeadLetterReason,
    message: Handle,
}

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
        sender: Mutex::new(Some(tx)),
        capacity,
        queued: AtomicUsize::new(0),
        closed: AtomicBool::new(false),
        dead_letters: dead_tx,
    });

    let sender_handle = handle.send();
    provide_sender(sender_handle, Arc::clone(&state));

    let receiver_pair = handle.send();
    provide_receiver(receiver_pair, rx, Arc::clone(&state));

    provide_dead_letters(handle, dead_rx);
}

fn provide_sender(handle: Handle, state: Arc<MailboxState>) {
    handle.provide_box(move |mut handle| {
        let state = Arc::clone(&state);
        async move {
            match handle.case().await.as_str() {
                "trySend" => {
                    let message = handle.receive();
                    let mut result = handle.send();
                    eprintln!(
                        "[mailbox] trySend entered closed={} queued={} capacity={}",
                        state.closed.load(Ordering::SeqCst),
                        state.queued.load(Ordering::SeqCst),
                        state.capacity,
                    );
                    if state.closed.load(Ordering::SeqCst) {
                        eprintln!("[mailbox] trySend rejected: closed");
                        emit_dead_letter(&state, DeadLetterReason::Closed, message);
                        result.signal(literal!("err"));
                        result.signal(literal!("closed"));
                        result.break_();
                        provide_sender(handle, state);
                        return;
                    }
                    if !reserve_slot(&state) {
                        eprintln!("[mailbox] trySend rejected: full");
                        emit_dead_letter(&state, DeadLetterReason::Full, message);
                        result.signal(literal!("err"));
                        result.signal(literal!("full"));
                        result.break_();
                        provide_sender(handle, state);
                        return;
                    }
                    eprintln!(
                        "[mailbox] trySend reserved slot queued={}",
                        state.queued.load(Ordering::SeqCst),
                    );

                    let sender = state
                        .sender
                        .lock()
                        .expect("mailbox sender lock failed")
                        .clone();
                    match sender {
                        Some(sender) => match sender.try_send(message) {
                            Ok(()) => {
                                eprintln!("[mailbox] try_send ok");
                                result.signal(literal!("ok"));
                                result.break_();
                            }
                            Err(mpsc::error::TrySendError::Full(message)) => {
                                eprintln!("[mailbox] tokio try_send full");
                                release_slot(&state);
                                emit_dead_letter(&state, DeadLetterReason::Full, message);
                                result.signal(literal!("err"));
                                result.signal(literal!("full"));
                                result.break_();
                            }
                            Err(mpsc::error::TrySendError::Closed(message)) => {
                                eprintln!("[mailbox] tokio try_send closed");
                                release_slot(&state);
                                emit_dead_letter(&state, DeadLetterReason::Closed, message);
                                result.signal(literal!("err"));
                                result.signal(literal!("closed"));
                                result.break_();
                            }
                        },
                        None => {
                            eprintln!("[mailbox] trySend rejected: sender missing");
                            release_slot(&state);
                            emit_dead_letter(&state, DeadLetterReason::Closed, message);
                            result.signal(literal!("err"));
                            result.signal(literal!("closed"));
                            result.break_();
                        }
                    }
                    provide_sender(handle, state);
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

fn reserve_slot(state: &MailboxState) -> bool {
    state
        .queued
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |queued| {
            (queued < state.capacity).then_some(queued + 1)
        })
        .is_ok()
}

fn release_slot(state: &MailboxState) {
    let _ = state
        .queued
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |queued| {
            queued.checked_sub(1)
        });
}

fn close_mailbox(state: &MailboxState) {
    state.closed.store(true, Ordering::SeqCst);
    state
        .sender
        .lock()
        .expect("mailbox sender lock failed")
        .take();
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

fn provide_receiver(handle: Handle, receiver: mpsc::Receiver<Handle>, state: Arc<MailboxState>) {
    let receiver = Arc::new(tokio::sync::Mutex::new(receiver));
    provide_receiver_loop(handle, receiver, state);
}

fn provide_receiver_loop(
    handle: Handle,
    receiver: Arc<tokio::sync::Mutex<mpsc::Receiver<Handle>>>,
    state: Arc<MailboxState>,
) {
    handle.concurrently(move |mut handle| async move {
        loop {
            match handle.case().await.as_str() {
                "receive" => {
                    let mut result = handle.send();
                    eprintln!(
                        "[mailbox] receive entered queued={} capacity={}",
                        state.queued.load(Ordering::SeqCst),
                        state.capacity,
                    );
                    let message = receiver.lock().await.recv().await;
                    match message {
                        Some(message) => {
                            release_slot(&state);
                            eprintln!(
                                "[mailbox] receive ok queued={}",
                                state.queued.load(Ordering::SeqCst),
                            );
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
    });
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
                            match dead.reason {
                                DeadLetterReason::Full => reason.signal(literal!("full")),
                                DeadLetterReason::Closed => reason.signal(literal!("closed")),
                            }
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
