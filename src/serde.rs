use crate::*;
use fxhash::*;
use ::serde::*;
use std::marker::*;

#[derive(Clone, Default)]
pub struct SerializationSet<S: 'static + Serializer, D: 'static + Deserializer<'static>> {
    serializers: Arc<FxHashMap<TypeId, Arc<dyn MessageTypeSerializer<S, D>>>>,
    marker: PhantomData<fn(S, D)>
}

impl<S: Serializer, D: Deserializer<'static>> SerializationSet<S, D> {
    pub fn with_type<T: 'static + Send + Sync + Serialize + Deserialize<'static>>(mut self) -> Self {
        Arc::make_mut(&mut self.serializers).insert(TypeId::of::<T>(), Arc::new(TypedMessageSerializer::<T, _, _>::default())).expect("Attempted to add duplicate type entry.");
        self
    }

    fn get_serializer(&self, id: TypeId) -> Option<&dyn MessageTypeSerializer<S, D>> {
        self.serializers.get(&id).map(|x| &**x)
    }
}

pub trait MessageSerializer {
    fn serialize<S: Serialize>(&mut self, message: S);
}

trait MessageTypeSerializer<S: Serializer, D: Deserializer<'static>>: std::fmt::Debug {
    fn serialize(&self, message: &dyn Any, serializer: S) -> Result<S::Ok, S::Error>;
    fn deserialize(&self, deserializer: D) -> Result<Message, D::Error>;
}

#[derive(Clone)]
struct TypedMessageSerializer<T: 'static + Send + Sync + Serialize + Deserialize<'static>, S: Serializer, D: Deserializer<'static>>(PhantomData<fn(T, S, D)>);

impl<T: 'static + Send + Sync + Serialize + Deserialize<'static>, S: Serializer, D: Deserializer<'static>> MessageTypeSerializer<S, D> for TypedMessageSerializer<T, S, D> {
    fn serialize(&self, message: &dyn Any, serializer: S) -> Result<S::Ok, S::Error> {
        message.downcast_ref::<T>().expect("Incorrect message type passed to serializer.").serialize(serializer)
    }

    fn deserialize(&self, deserializer: D) -> Result<Message, D::Error> {
        T::deserialize(deserializer).map(|x| Message::new(x))
    }
}

impl<T: 'static + Send + Sync + Serialize + Deserialize<'static>, S: Serializer, D: Deserializer<'static>> std::fmt::Debug for TypedMessageSerializer<T, S, D> {
    fn fmt(&self, f: &mut __private::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple(type_name::<Self>()).finish()
    }
}

impl<T: 'static + Send + Sync + Serialize + Deserialize<'static>, S: Serializer, D: Deserializer<'static>> Default for TypedMessageSerializer<T, S, D> {
    fn default() -> Self {
        Self(PhantomData::default())
    }
}

pub struct SerializedPeerChannel<S: 'static + Serializer, D: 'static + Deserializer<'static>> {
    serializer: S,
    deserializer: D,
    set: SerializationSet<S, D>
}

impl<S: Serializer, D: Deserializer<'static>> PeerChannel for SerializedPeerChannel<S, D> {
    fn read(&self) -> std::io::Result<Option<Message>> {
        todo!()
    }

    fn write(&self, message: Message) -> std::io::Result<()> {
        let obj = message.into_inner();
        if let Some(ser) = self.set.get_serializer(obj.type_id()) {
            ser.serialize(&obj, )
        }
        else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("Unrecognized message submitted for serialization.")))
        }
    }
}
