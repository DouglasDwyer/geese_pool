use crate::*;
use fxhash::*;
use ::serde::*;
use std::marker::*;
use std::io::*;

/// Provides the ability to serialize and deserialize messages from a source.
pub trait MessageSerializer: 'static {
    /// Serializes an object of the provided type to the remote source.
    fn serialize<S: Serialize>(&mut self, value: S) -> Result<()>;
    /// Deserializes an object of the provided type from the remote source.
    fn deserialize<D: Deserialize<'static>>(&mut self) -> Result<Option<D>>;
    /// Flushes any buffered messages, ensuring that they reach the remote source.
    fn flush(&mut self) -> Result<()>;
}

/// Provides the ability to send and received serialized messages with another Geese instance.
pub struct SerializedPeerChannel<M: MessageSerializer> {
    next_message_type: Option<u16>,
    serializer: M,
    set: SerializationSet<M>,
}

impl<M: MessageSerializer> SerializedPeerChannel<M> {
    /// Creates a new peer channel with the specified serialization source and set.
    pub fn new(serializer: M, set: SerializationSet<M>) -> Self {
        Self {
            next_message_type: None,
            serializer,
            set
        }
    }
}

impl<M: MessageSerializer> PeerChannel for SerializedPeerChannel<M> {
    fn read(&mut self) -> Result<Option<Message>> {
        if self.next_message_type.is_none() {
            self.next_message_type = self.serializer.deserialize::<u16>()?;
        }

        if let Some(ty) = self.next_message_type {
            if let Some(ser) = self.set.get_deserializer(ty) {
                ser.deserialize(&mut self.serializer)
            }
            else {
                Err(Error::new(ErrorKind::InvalidData, "Unrecognized deserialization type."))
            }
        }
        else {
            Ok(None)
        }
    }

    fn write(&mut self, message: &Message) -> Result<()> {
        let inner = message.as_inner();
        if let Some((id, ser)) = self.set.get_serializer(inner.type_id()) {
            self.serializer.serialize(id)?;
            ser.serialize(inner, &mut self.serializer)
        }
        else {
            Err(Error::new(ErrorKind::InvalidData, "Cannot serialize message of the provided type."))
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.serializer.flush()
    }
}

/// Represents a collection of messages that a `SerializedPeerChannel` is capable of sending/receiving. Both
/// ends of the serialized channel must use identical serialization sets, with the same types added in the same order.
#[derive(Clone, Debug, Default)]
pub struct SerializationSet<M: MessageSerializer>(Arc<SerializationSetInner<M>>);

impl<M: MessageSerializer> SerializationSet<M> {
    /// Adds an additional type to this serialization set, panicking if the type was already added.
    pub fn with_type<T: 'static + Send + Sync + Serialize + Deserialize<'static>>(mut self) -> Self {
        let set = Arc::make_mut(&mut self.0);
        assert!(set.type_map.insert(TypeId::of::<T>(), set.serializers.len() as u16).is_none(), "Attempted to add duplicate type entry.");
        set.serializers.push(Arc::new(TypedMessageSerializer::<T, _>::default()));
        self
    }

    /// Obtains the identifier and serializer for the provided type.
    fn get_serializer(&self, id: TypeId) -> Option<(u16, &dyn MessageTypeSerializer<M>)> {
        self.0.type_map.get(&id).map(|&x| (x, &*self.0.serializers[x as usize]))
    }

    /// Gets the deserializer for the type with the provided identifier.
    fn get_deserializer(&self, index: u16) -> Option<&dyn MessageTypeSerializer<M>> {
        self.0.serializers.get(index as usize).map(|x| &**x)
    }
}

/// Stores inner data about a set of types that may be serialized.
#[derive(Debug, Default)]
struct SerializationSetInner<M: MessageSerializer> {
    /// A mapping from types to their network identifiers.
    pub type_map: FxHashMap<TypeId, u16>,
    /// A list of serializers, ordered by their network identifiers.
    pub serializers: Vec<Arc<dyn MessageTypeSerializer<M>>>
}

impl<M: MessageSerializer> Clone for SerializationSetInner<M> {
    fn clone(&self) -> Self {
        Self { type_map: self.type_map.clone(), serializers: self.serializers.clone() }
    }
}

/// Provides an object-safe interface for serializing and deserializing data of a particular type.
trait MessageTypeSerializer<M: MessageSerializer>: std::fmt::Debug {
    /// Serializes the provided value using the given message serializer,
    /// or panics if the value was of the incorrect type.
    fn serialize(&self, value: &dyn Any, serializer: &mut M) -> Result<()>;
    /// Deserializes the provided value with the given message serializer.
    fn deserialize(&self, deserializer: &mut M) -> Result<Option<Message>>;
}

/// Implements the ability to serialize and deserialize one particular kind of data.
#[derive(Clone)]
struct TypedMessageSerializer<T: 'static + Send + Sync + Serialize + Deserialize<'static>, M: MessageSerializer>(PhantomData<fn(T, M)>);

impl<T: 'static + Send + Sync + Serialize + Deserialize<'static>, M: MessageSerializer> MessageTypeSerializer<M> for TypedMessageSerializer<T, M> {
    fn serialize(&self, value: &dyn Any, serializer: &mut M) -> Result<()> {
        serializer.serialize(value.downcast_ref::<T>().expect("Value to be serialized was not of expected underlying type."))
    }

    fn deserialize(&self, deserializer: &mut M) -> Result<Option<Message>> {
        deserializer.deserialize::<T>().map(|x| x.map(|y| Message::new(y)))
    }
}

impl<T: 'static + Send + Sync + Serialize + Deserialize<'static>, M: MessageSerializer> std::fmt::Debug for TypedMessageSerializer<T, M> {
    fn fmt(&self, f: &mut __private::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(type_name::<Self>()).finish()
    }
}

impl<T: 'static + Send + Sync + Serialize + Deserialize<'static>, M: MessageSerializer> Default for TypedMessageSerializer<T, M> {
    fn default() -> Self {
        Self(PhantomData::default())
    }
}