#![deny(warnings)]

//! This crate provides the ability to pass messages between multiple Geese instances,
//! which may exist in separate processes or even on separate computers. The crate functions
//! by maintaining a `GeesePool` of channels to other instances, through which messages
//! may be sent or received. Systems may send or receive messages by raising the appropriate
//! `geese_pool` events.
//! 
//! This crate is completely protocol agnostic; it does not provide an implementation for
//! networking Geese instances using any specific standard, like UDP or TCP. It is up to the
//! consumer to provide both a means of message serialization and network transport.
//! 
//! The following is a brief example of how an event may be sent between two separate event systems.
//! The example first creates two Geese contexts, and adds the `GeesePool` system to both.
//! Then, it creates a channel pair across which events may be forwarded, and places one channel
//! in each connection pool. Finally, the connection pool of `b` is notified to send an integer
//! to `a`. When `a` is subsequently updated, a message event containing the same integer is raised
//! in `a`'s event system.
//! 
//! ```
//! # use geese::*;
//! # use geese_pool::*;
//! #
//! struct Receiver(i32);
//! 
//! impl Receiver {
//!     fn respond(&mut self, message: &geese_pool::on::Message<i32>) {
//!         self.0 = **message;
//!     }
//! }
//! 
//! impl GeeseSystem for Receiver {
//!     fn new(_: GeeseContextHandle) -> Self {
//!         Self(0)
//!     }
//! 
//!     fn register(with: &mut GeeseSystemData<Self>) {
//!         with.event(Self::respond);
//!     }
//! }
//! 
//! # fn run() {
//! let mut a = GeeseContext::default();
//! a.raise_event(geese::notify::add_system::<GeesePool>());
//! a.raise_event(geese::notify::add_system::<Receiver>());
//!
//! let mut b = GeeseContext::default();
//! b.raise_event(geese::notify::add_system::<GeesePool>());
//! 
//! let (chan_a, chan_b) = LocalChannel::new_pair();
//! 
//! // Handles must be kept alive, as they are tied to the lifetime of a connection.
//! let _handle_b = a.system::<GeesePool>().add_peer(Box::new(chan_a));
//! let handle_a = b.system::<GeesePool>().add_peer(Box::new(chan_b));
//! 
//! b.raise_event(geese_pool::notify::message(1, handle_a.clone()));
//! b.flush_events();
//! 
//! a.raise_event(geese_pool::notify::Update);
//! a.flush_events();
//! 
//! assert_eq!(1, a.system::<Receiver>().0);
//! # }
//! ```
//! 
//! ## Optional features
//! 
//! **serde** - Provides a `PeerChannel` implementation that serializes and deserializes its data, as
//! this is required for most forms of interprocess communication.

/// Provides a `PeerChannel` implementation that serializes and deserializes its data.
#[cfg(feature = "serde")]
pub mod serde;

use fxhash::*;
use geese::*;
use std::any::*;
use std::borrow::*;
use std::cell::*;
use std::collections::*;
use std::hash::*;
use std::iter::*;
use std::ops::*;
use std::sync::*;
use std::sync::atomic::*;
use std::sync::mpsc::*;
use std::sync::mpsc::Receiver;
use takecell::*;

/// Represents a message that may be forwarded across Geese instances.
#[derive(Clone)]
pub struct Message(Arc<dyn InnerMessage>);

impl Message {
    /// Creates a new message containing the given structure.
    pub fn new<T: 'static + Send + Sync>(message: T) -> Self {
        Self(Arc::new(message))
    }

    /// Converts this message into an `Any` reference to the underlying data.
    pub fn as_inner(&self) -> &dyn Any {
        (*self.0).as_any()
    }

    /// Converts this message into a `Box` containing an `on::Message` event that
    /// holds the underlying data.
    pub fn as_message_event(&self, sender: ConnectionId) -> Box<dyn Any> {
        self.0.clone().into_message_event(sender)
    }

    /// Determines the name of the message's event type for debugging purposes.
    fn message_name(&self) -> &str {
        (*self.0).message_name()
    }
}

/// Provides the backing implementation for conversion between
/// messages and inner types.
trait InnerMessage: Any + Send + Sync {
    /// Converts this into an `Any` reference to the underlying data.
    fn as_any(&self) -> &dyn Any;

    /// Converts this into a `Box` containing an `on::Message` event that
    /// holds the underlying data.
    fn into_message_event(self: Arc<Self>, sender: ConnectionId) -> Box<dyn Any>;

    /// Determines the name of the message's event type for debugging purposes.
    fn message_name(&self) -> &str;
}

impl<T: 'static + Any + Send + Sync> InnerMessage for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_message_event(self: Arc<Self>, sender: ConnectionId) -> Box<dyn Any> {
        Box::new(on::Message::new(self, sender))
    }

    fn message_name(&self) -> &str {
        type_name::<T>()
    }
}

/// Represents a connection across which one can forward events.
pub trait PeerChannel {
    /// Reads an event from the channel. Returns none if there was no new event
    /// available.
    fn read(&mut self) -> std::io::Result<Option<Message>>;

    /// Writes an event to the channel.
    fn write(&mut self, message: &Message) -> std::io::Result<()>;

    /// Flushes this channel, ensuring that any buffered messages are sent to their destination.
    fn flush(&mut self) -> std::io::Result<()>;
}

/// Represents an in-process peer channel for sending events
/// from one Geese instance to another.
#[derive(Debug)]
pub struct LocalChannel {
    sender: Sender<Message>,
    receiver: Receiver<Message>
}

impl LocalChannel {
    /// Creates a new pair of channels that can send in-process messages to one another.
    pub fn new_pair() -> (Self, Self) {
        let (outgoing_b, incoming_a) = channel();
        let (outgoing_a, incoming_b) = channel();

        (Self { sender: outgoing_a, receiver: incoming_a }, Self { sender: outgoing_b, receiver: incoming_b })
    }
}

impl PeerChannel for LocalChannel {
    fn read(&mut self) -> std::io::Result<Option<Message>> {
        match self.receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(TryRecvError::Disconnected) => Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, TryRecvError::Disconnected)),
            Err(TryRecvError::Empty) => Ok(None)
        }
    }

    fn write(&mut self, message: &Message) -> std::io::Result<()> {
        self.sender.send(message.clone()).map_err(|x| std::io::Error::new(std::io::ErrorKind::ConnectionAborted, x))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Represents an owned handle to another Geese instance in the connection pool. Connection handles
/// may be utilized to identify connections, and control their lifetimes. When a connection handle
/// is dropped, the underlying connection is terminated. All handles must be dropped before
/// a `GeesePool` is disposed, or a panic will occur.
pub struct ConnectionHandle {
    id: ConnectionId,
    disposed_token: Arc<AtomicBool>
}

impl ConnectionHandle {
    /// Creates a new, unique connection handle.
    fn new() -> Self {
        Self {
            id: ConnectionId::new(),
            disposed_token: Arc::default()
        }
    }

    /// Obtains a shared reference to the token that determines
    /// whether this connection handle has been dropped.
    fn disposed_token(&self) -> Arc<AtomicBool> {
        self.disposed_token.clone()
    }
}

impl Borrow<ConnectionId> for ConnectionHandle {
    fn borrow(&self) -> &ConnectionId {
        &*self
    }
}

impl std::fmt::Debug for ConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.id.fmt(f)
    }
}

impl Hash for ConnectionHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl PartialEq for ConnectionHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ConnectionHandle {}

impl Deref for ConnectionHandle {
    type Target = ConnectionId;

    fn deref(&self) -> &Self::Target {
        &self.id
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        self.disposed_token.store(true, Ordering::Release);
    }
}

/// Represents a reference to another Geese instance in the connection pool. Connection identifiers
/// may be utilized to send messages or identify the senders of received messages.
#[derive(Clone)]
pub struct ConnectionId(Arc<()>);

impl ConnectionId {
    /// Creates a new, unique connection identifier.
    fn new() -> Self {
        Self(Arc::default())
    }
}

impl std::fmt::Debug for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{}", (&*self.0 as *const ()) as usize))
    }
}

impl Hash for ConnectionId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (&*self.0 as *const ()).hash(state);
    }
}

impl PartialEq for ConnectionId {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ConnectionId {}

/// Describes the state of an active peer connection.
struct ConnectionState {
    /// The channel over which data is sent for this connection.
    pub channel: Box<dyn PeerChannel>,
    /// Whether the channel has been dropped.
    pub disposed_token: Arc<AtomicBool>
}

/// Stores and manages peer connections to other Geese instances.
pub struct GeesePool {
    ctx: GeeseContextHandle,
    flush_required: bool,
    peer_connections: RefCell<FxHashMap<ConnectionId, ConnectionState>>,
}

impl GeesePool {
    /// Adds the provided peer to the connection pool, enabling it to
    /// send and receive events.
    pub fn add_peer(&self, channel: Box<dyn PeerChannel>) -> ConnectionHandle {
        let handle = ConnectionHandle::new();

        self.peer_connections().insert(handle.clone(), ConnectionState {
            channel,
            disposed_token: handle.disposed_token()
        });

        self.ctx.raise_event(on::PeerAdded { handle: handle.clone() });
        handle
    }

    /// Deals with the disconnection of a remote peer by removing it from the active connection
    /// set and raising a disconnection event.
    fn handle_peer_disconnection(&self, peer: &ConnectionId, error: std::io::Error) {
        self.peer_connections().remove(&peer).expect("The specified peer was not connected.");
        self.ctx.raise_event(on::PeerRemoved { handle: peer.clone(), reason: error });
    }

    /// Retrieves a mutable reference to the connections map of this struct.
    fn peer_connections(&self) -> RefMut<'_, FxHashMap<ConnectionId, ConnectionState>> {
        self.peer_connections.borrow_mut()
    }

    /// Obtains a mutable reference to the state of the specified connection. This function
    /// checks to see whether the connection has been dropped or does not exist, and
    /// if so, returns none.
    fn get_peer(&self, id: &ConnectionId) -> Option<RefMut<'_, dyn PeerChannel>> {
        let mut connections = self.peer_connections();
        let peer = connections.get_mut(&id);

        if let Some(conn) = peer {
            if conn.disposed_token.load(Ordering::Acquire) {
                drop(connections);
                self.handle_peer_disconnection(id, std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "Connection aborted by local peer."));
                None
            }
            else {
                Some(RefMut::map(connections, |x| &mut *x.get_mut(&id).expect("No peer was associated with the provided identifier.").channel))
            }
        }
        else {
            None
        }
    }

    /// Reads the next message from the given connection,
    /// returning whether another read should be attempted.
    fn read_peer(&mut self, id: &ConnectionId) -> bool {
        match self.get_peer(id).map(|mut x| x.read()).unwrap_or(Ok(None)) {
            Ok(Some(event)) => { self.ctx.raise_boxed_event(event.as_message_event(id.clone())); true },
            Ok(None) => false,
            Err(error) => {
                self.handle_peer_disconnection(id, error);
                false
            }
        }
    }

    /// Writes the given event to the specified recipient, and handles errors that occur
    /// by disconnecting the client. If the remote peer is no longer connected, the message
    /// is dropped.
    fn write_event(&mut self, event: &Message, recipient: &ConnectionId) {
        let result = self.get_peer(&recipient)
            .map(|mut conn| conn.write(event))
            .unwrap_or(Ok(()));

        if let Err(error) = result {
            self.handle_peer_disconnection(recipient, error);
        }
    }

    /// Flushes all peer channels, disconnecting any peers who have errors.
    fn flush_channels(&mut self) {
        self.flush_required = false;

        let mut disconnected = Vec::new();

        for (id, peer) in self.peer_connections().iter_mut() {
            if let Err(error) = peer.channel.flush() {
                disconnected.push((id.clone(), error));
            }
        }

        for (id, error) in disconnected {
            self.handle_peer_disconnection(&id, error);
        }
    }

    /// Reads new event messages from remote Geese systems.
    fn read_events(&mut self, _: &notify::Update) {
        let connections = self.peer_connections().keys().cloned().collect::<Vec<_>>();
        for conn in &connections {
            while self.read_peer(conn) {}
        }

        if self.flush_required {
            self.flush_channels();
        }

        self.flush_required = true;
    }

    /// Sends the message to all specified remote peers. If any remote peer is no longer connected,
    /// it is ignored.
    fn send_message(&mut self, message: &notify::Message) {
        for recipient in message.recipients.take().expect("The recipient iterator was already taken.") {
            self.write_event(&message.event, &recipient);
        }
    }

    fn flush_events(&mut self, _: &notify::Flush) {
        self.flush_channels();
    }
}

impl GeeseSystem for GeesePool {
    fn new(ctx: GeeseContextHandle) -> Self {
        let flush_required = true;
        let peer_connections = RefCell::new(HashMap::default());

        Self {
            ctx,
            flush_required,
            peer_connections
        }
    }

    fn register(with: &mut GeeseSystemData<Self>) {
        with.event(Self::flush_events);
        with.event(Self::read_events);
        with.event(Self::send_message);
    }
}

impl Drop for GeesePool {
    fn drop(&mut self) {
        for connection in self.peer_connections().values() {
            assert!(connection.disposed_token.load(Ordering::Acquire), "Geese pool was dropped while a connection handle remained alive.");
        }
    }
}

/// The set of events to which this module responds.
pub mod notify {
    use super::*;

    /// Causes the connection pool to notify recipients of an event.
    pub struct Message {
        pub(super) event: super::Message,
        pub(super) recipients: TakeOwnCell<Box<dyn Iterator<Item = ConnectionId>>>
    }

    /// Sends the given event to the specified recipient.
    pub fn message<T: 'static + Send + Sync>(event: T, recipient: ConnectionId) -> Message {
        Message { event: super::Message::new(event), recipients: TakeOwnCell::new(Box::new(once(recipient))) }
    }

    /// Broadcasts the given event to all specified recipients.
    pub fn broadcast<T: 'static + Send + Sync, Q: 'static + IntoIterator<Item = ConnectionId>>(event: T, recipients: Q) -> Message {
        Message { event: super::Message::new(event), recipients: TakeOwnCell::new(Box::new(recipients.into_iter())) }
    }

    /// Causes the connection pool to update all connections
    /// and raise any newly-received events appropriately.
    pub struct Update;

    /// Flushes all channels in the connection pool, clearing out buffered messages
    /// and sending them to their final destination. `GeesePool` will flush all channels
    /// at least once each update cycle, but this event serves as a hint as to when
    /// all events have been received for a cycle.
    pub struct Flush;
}

/// The set of events that this module raises.
pub mod on {
    use super::*;

    /// Raised when a new peer is added to the connection pool.
    #[derive(Clone, Debug)]
    pub struct PeerAdded {
        pub handle: ConnectionId
    }

    /// Raised when a peer is disconnected and removed from the
    /// connection pool.
    #[derive(Debug)]
    pub struct PeerRemoved {
        /// The handle of the disconnected peer.
        pub handle: ConnectionId,
        /// The reason that the peer disconnected.
        pub reason: std::io::Error
    }

    /// Raised when a message is received from another Geese instance in the connection pool.
    pub struct Message<T: 'static + Send + Sync> {
        event: Arc<T>,
        sender: ConnectionId
    }

    impl<T: 'static + Send + Sync> Message<T> {
        /// Creates a new message for the event and sender.
        pub(super) fn new(event: Arc<T>, sender: ConnectionId) -> Self {
            Self { event, sender }
        }

        /// Obtains the event associated with this message.
        pub fn event(&self) -> &T {
            &self.event
        }

        /// Obtains the sender associated with this message.
        pub fn sender(&self) -> ConnectionId {
            self.sender.clone()
        }
    }

    impl<T: 'static + Send + Sync> Deref for Message<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            self.event()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Receiver(i32);

    impl Receiver {
        fn respond(&mut self, message: &on::Message<i32>) {
            self.0 = **message;
        }
    }

    impl GeeseSystem for Receiver {
        fn new(_: GeeseContextHandle) -> Self {
            Self(0)
        }

        fn register(with: &mut GeeseSystemData<Self>) {
            with.event(Self::respond);
        }
    }

    #[test]
    fn test_local_message() {
        let mut a = GeeseContext::default();
        a.raise_event(geese::notify::add_system::<GeesePool>());
        a.raise_event(geese::notify::add_system::<Receiver>());

        let mut b = GeeseContext::default();
        b.raise_event(geese::notify::add_system::<GeesePool>());

        let (chan_a, chan_b) = LocalChannel::new_pair();

        // Handles must be kept alive, as they are tied to the lifetime of a connection.
        let _handle_b = a.system::<GeesePool>().add_peer(Box::new(chan_a));
        let handle_a = b.system::<GeesePool>().add_peer(Box::new(chan_b));

        b.raise_event(notify::message(1, handle_a.clone()));
        b.flush_events();

        a.raise_event(notify::Update);
        a.flush_events();

        assert_eq!(1, a.system::<Receiver>().0);
    }
}