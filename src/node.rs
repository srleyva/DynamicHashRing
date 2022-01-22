use std::{
    fmt::{Debug, Display},
    hash::Hash,
    net::SocketAddr,
};

use foca::Identity;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub trait ID: Sized + Hash + Display + Clone + Eq + Debug {
    fn prefix(&self) -> &str;
    fn id(&self) -> &str;
    fn renew(&self) -> Option<Self>;
}

// Node Identity is used the identify of node to load balance amongst a hash ring
#[derive(Debug, Eq, Deserialize, Serialize, Clone)]
pub struct NodeIdentity<Id: ID> {
    pub(crate) id: Id,
    pub(crate) socket_addr: SocketAddr,
}

impl<Id: ID> NodeIdentity<Id> {
    pub fn new(id: Id, socket_addr: SocketAddr) -> Self {
        Self { id, socket_addr }
    }
    pub fn id(&self) -> &Id {
        &self.id
    }
}

impl<Id: ID + Display> Display for NodeIdentity<Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.id.id(), self.socket_addr)
    }
}

impl<Id: ID> Hash for NodeIdentity<Id> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.id().hash(state)
    }
}

impl<Id: ID> PartialEq for NodeIdentity<Id> {
    fn eq(&self, other: &Self) -> bool {
        self.id.id() == other.id.id()
    }
}

impl<Id: ID + Clone + Eq + Debug + Default> Identity for NodeIdentity<Id> {
    fn has_same_prefix(&self, other: &Self) -> bool {
        other.id().prefix() == self.id().prefix()
    }

    fn renew(&self) -> Option<Self> {
        self.id
            .renew()
            .map(|id| NodeIdentity::new(id, self.socket_addr))
    }
}

#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeUUID {
    prefix: String,
    uuid: Uuid,
    id: String,
}

impl Display for NodeUUID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.id())
    }
}

impl Default for NodeUUID {
    fn default() -> Self {
        let uuid = Uuid::new_v4();
        let prefix = "my-cluster".to_owned();
        let id = format!("{}-{}", prefix, uuid);
        Self { prefix, uuid, id }
    }
}

impl ID for NodeUUID {
    fn prefix(&self) -> &str {
        &self.prefix
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn renew(&self) -> Option<Self> {
        None
    }
}
