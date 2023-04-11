# geese_pool

[![Crates.io](https://img.shields.io/crates/v/geese_pool.svg)](https://crates.io/crates/geese_pool)
[![Docs.rs](https://docs.rs/geese_pool/badge.svg)](https://docs.rs/geese_pool)

This crate provides the ability to pass messages between multiple Geese instances,
which may exist in separate processes or even on separate computers. The crate functions
by maintaining a `ConnectionPool` of channels to other instances, through which messages
may be sent or received. Systems may send or receive messages by raising the appropriate
`geese_pool` events.

This crate is completely protocol agnostic; it does not provide an implementation for
networking Geese instances using any specific standard, like UDP or TCP. It is up to the
consumer to provide both a means of message serialization and network transport.

The following is a brief example of how an event may be sent between two separate event systems.
The example first creates two Geese contexts, and adds the `ConnectionPool` system to both.
Then, it creates a channel pair across which events may be forwarded, and places one channel
in each connection pool. Finally, the connection pool of `b` is notified to send an integer
to `a`. When `a` is subsequently updated, a message event containing the same integer is raised
in `a`'s event system.

```rust
struct Receiver(i32);

impl Receiver {
    fn respond(&mut self, message: &geese_pool::on::Message<i32>) {
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

let mut a = GeeseContext::default();
a.raise_event(geese::notify::AddSystem::new::<ConnectionPool>());
a.raise_event(geese::notify::AddSystem::new::<Receiver>());

let mut b = GeeseContext::default();
b.raise_event(geese::notify::AddSystem::new::<ConnectionPool>());

let (chan_a, chan_b) = LocalChannel::new_pair();
a.system::<ConnectionPool>().add_peer(Box::new(chan_a));
let handle_a = b.system::<ConnectionPool>().add_peer(Box::new(chan_b));

b.raise_event(geese_pool::notify::message(1, handle_a));
b.flush_events();

a.raise_event(geese_pool::notify::Update);
a.flush_events();

assert_eq!(1, a.system::<Receiver>().0);
```

## Optional features

**serde** - Provides a `PeerChannel` implementation that serializes and deserializes its data, as
this is required for most forms of interprocess communication.