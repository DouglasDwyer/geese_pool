#![deny(warnings)]

use geese::*;
use std::any::*;
use std::cell::*;
use std::collections::*;
use std::hash::*;
use std::ops::*;
use std::sync::*;
use std::sync::mpsc::*;
use std::sync::mpsc::Receiver;
use takecell::*;

pub struct Message(Box<dyn InnerMessage>);

impl Message {
    pub fn new<T: 'static + Send + Sync>(message: T) -> Self {
        Self(Box::new(message))
    }

    pub fn into_inner(self) -> Box<dyn Any> {
        self.0.into_inner()
    }

    pub fn into_message_event(self, sender: ConnectionHandle) -> Box<dyn Any> {
        self.0.into_message_event(sender)
    }
}

trait IntoClonedMessage: Send + Sync {
    fn cloned_message(&self) -> Message;
}

impl<T: 'static + Clone + Send + Sync> IntoClonedMessage for T {
    fn cloned_message(&self) -> Message {
        Message::new(self.clone())
    }
}

trait InnerMessage: Send + Sync {
    fn into_inner(self: Box<Self>) -> Box<dyn Any>;
    fn into_message_event(self: Box<Self>, sender: ConnectionHandle) -> Box<dyn Any>;
}

impl<T: 'static + Send + Sync> InnerMessage for T {
    fn into_inner(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn into_message_event(self: Box<Self>, sender: ConnectionHandle) -> Box<dyn Any> {
        Box::new(on::Message::new(*self, sender))
    }
}

/// Represents a connection across which one can forward events.
pub trait PeerChannel {
    /// Reads an event from the channel. Returns none if there was no new event
    /// available.
    fn read(&self) -> std::io::Result<Option<Message>>;

    /// Writes an event to the channel.
    fn write(&self, message: Message) -> std::io::Result<()>;
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
    fn read(&self) -> std::io::Result<Option<Message>> {
        match self.receiver.try_recv() {
            Ok(x) => Ok(Some(x)),
            Err(TryRecvError::Disconnected) => Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, TryRecvError::Disconnected)),
            Err(TryRecvError::Empty) => Ok(None)
        }
    }

    fn write(&self, message: Message) -> std::io::Result<()> {
        self.sender.send(message).map_err(|x| std::io::Error::new(std::io::ErrorKind::ConnectionAborted, x))
    }
}

/// Represents a handle to another Geese instance in the connection pool. Connection handles
/// may be utilized to send messages or identify the senders of received messages.
#[derive(Clone, Debug)]
pub struct ConnectionHandle(Arc<()>);

impl ConnectionHandle {
    /// Creates a new, unique connection handle.
    fn new() -> Self {
        Self(Arc::default())
    }
}

impl Hash for ConnectionHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (&*self.0 as *const ()).hash(state);
    }
}

impl PartialEq for ConnectionHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for ConnectionHandle {}

/// Stores and manages peer connections to other Geese instances.
pub struct ConnectionPool {
    ctx: GeeseContextHandle,
    peer_connections: RefCell<HashMap<ConnectionHandle, Box<dyn PeerChannel>>>
}

impl ConnectionPool {
    /// Adds the provided peer to the connection pool, enabling it to
    /// send and receive events.
    pub fn add_peer(&self, channel: Box<dyn PeerChannel>) -> ConnectionHandle {
        let handle = ConnectionHandle::new();

        self.peer_connections().insert(handle.clone(), channel);
        self.ctx.raise_event(on::PeerAdded { handle: handle.clone() });

        handle
    }

    /// Adds the provided peer in response to a peer addition event.
    fn add_peer_event(&mut self, event: &notify::AddPeer) {
        self.add_peer(event.channel.take().expect("The peer was already taken."));
    }

    /// Deals with the disconnection of a remote peer by removing it from the active connection
    /// set and raising a disconnection event.
    fn handle_peer_disconnection(&mut self, peer: ConnectionHandle, error: std::io::Error) {
        self.peer_connections().remove(&peer).expect("The specified peer was not connected.");
        self.ctx.raise_event(on::PeerRemoved { handle: peer, reason: error });
    }

    /// Broadcasts the message to all specified remote peers. If any remote peer is no longer connected,
    /// it is ignored.
    fn broadcast_message(&mut self, message: &notify::BroadcastMessage) {
        for recipient in message.recipients.take().expect("The recipient iterator was already taken.") {
            self.write_event(message.event.cloned_message(), recipient);
        }
    }

    /// Sends the message to the specified remote peer. If the remote peer is no longer connected,
    /// the message is dropped.
    fn send_message(&mut self, message: &notify::Message) {
        self.write_event(message.event.take().expect("Event was already taken."), message.recipient.clone());
    }

    /// Retrieves a mutable reference to the connections map of this struct.
    fn peer_connections(&self) -> RefMut<'_, HashMap<ConnectionHandle, Box<dyn PeerChannel>>> {
        self.peer_connections.borrow_mut()
    }

    /// Reads new event messages from remote Geese systems.
    fn read_events(&mut self, _: &notify::Update) {
        let connections = self.peer_connections().keys().cloned().collect::<Vec<_>>();
        for conn in connections {
            while self.read_peer(conn.clone()) {}
        }
    }

    /// Reads the next message from the given connection,
    /// returning whether another read should be attempted.
    fn read_peer(&mut self, handle: ConnectionHandle) -> bool {
        let result = self.peer_connections()[&handle].read();
        match result {
            Ok(Some(event)) => { self.ctx.raise_boxed_event(event.into_message_event(handle)); true },
            Ok(None) => false,
            Err(error) => {
                self.handle_peer_disconnection(handle, error);
                false
            }
        }
    }

    /// Writes the given event to the specified recipient, and handles errors that occur
    /// by disconnecting the client. If the remote peer is no longer connected, the message
    /// is dropped.
    fn write_event(&mut self, event: Message, recipient: ConnectionHandle) {
        let result = self.peer_connections().get(&recipient)
            .map(|conn| conn.write(event))
            .unwrap_or(Ok(()));

        if let Err(error) = result {
            self.handle_peer_disconnection(recipient, error);
        }
    }
}

impl GeeseSystem for ConnectionPool {
    fn new(ctx: GeeseContextHandle) -> Self {
        let peer_connections = RefCell::new(HashMap::default());

        Self {
            ctx,
            peer_connections
        }
    }

    fn register(with: &mut GeeseSystemData<Self>) {
        with.event(Self::add_peer_event);
        with.event(Self::broadcast_message);
        with.event(Self::read_events);
        with.event(Self::send_message);
    }
}

/// The set of events to which this module responds.
pub mod notify {
    use super::*;

    /// Broadcast the given event to all specified recipients in the connection pool. Shorthand for `BroadcastMessage::new`.
    pub fn broadcast<T: 'static + Clone + Send + Sync, Q: 'static + IntoIterator<Item = ConnectionHandle>>(event: T, recipients: Q) -> BroadcastMessage {
        BroadcastMessage::new(event, recipients)
    }

    /// Broadcast the given event to the specified recipient. Shorthand for `Message::new`.
    pub fn message<T: 'static + Send + Sync>(event: T, recipient: ConnectionHandle) -> Message {
        Message::new(event, recipient)
    }

    /// Adds a channel to the pool of active connections.
    pub struct AddPeer {
        pub(super) channel: TakeOwnCell<Box<dyn PeerChannel>>
    }

    impl AddPeer {
        /// Instructs the connection pool to add the specified channel.
        pub fn new(channel: Box<dyn PeerChannel>) -> Self {
            Self { channel: TakeOwnCell::new(channel) }
        }
    }

    /// Causes the connection pool to notify a single recipient of an event.
    pub struct Message {
        pub(super) event: TakeOwnCell<super::Message>,
        pub(super) recipient: ConnectionHandle
    }

    impl Message {
        /// Creates a new message for the given underlying event and recipient.
        pub fn new<T: 'static + Send + Sync>(event: T, recipient: ConnectionHandle) -> Self {
            Self { event: TakeOwnCell::new(super::Message::new(event)), recipient }
        }
    }

    /// Causes the connection pool to notify a set of recipients of an event.
    pub struct BroadcastMessage {
        pub(super) event: Box<dyn IntoClonedMessage>,
        pub(super) recipients: TakeOwnCell<Box<dyn Iterator<Item = ConnectionHandle>>>
    }

    impl BroadcastMessage {
        /// Creates a new broadcast message for the given underlying event and recipients.
        pub fn new<T: 'static + Clone + Send + Sync, Q: 'static + IntoIterator<Item = ConnectionHandle>>(event: T, recipients: Q) -> Self {
            Self { event: Box::new(event), recipients: TakeOwnCell::new(Box::new(recipients.into_iter())) }
        }
    }

    /// Causes the connection pool to update all connections
    /// and raise any newly-received events appropriately.
    pub struct Update;
}

/// The set of events that this module raises.
pub mod on {
    use super::*;

    /// Raised when a new peer is added to the connection pool.
    #[derive(Clone, Debug)]
    pub struct PeerAdded {
        pub handle: ConnectionHandle
    }

    /// Raised when a peer is disconnected and removed from the
    /// connection pool.
    #[derive(Debug)]
    pub struct PeerRemoved {
        /// The handle of the disconnected peer.
        pub handle: ConnectionHandle,
        /// The reason that the peer disconnected.
        pub reason: std::io::Error
    }

    /// Raised when a message is received from another Geese instance in the connection pool.
    pub struct Message<T: 'static + Send + Sync> {
        event: T,
        sender: ConnectionHandle
    }

    impl<T: 'static + Send + Sync> Message<T> {
        /// Creates a new message for the event and sender.
        pub(super) fn new(event: T, sender: ConnectionHandle) -> Self {
            Self { event, sender }
        }

        /// Obtains the event associated with this message.
        pub fn event(&self) -> &T {
            &self.event
        }

        /// Obtains the sender associated with this message.
        pub fn sender(&self) -> ConnectionHandle {
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