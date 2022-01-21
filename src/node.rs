use std::{
    fmt::{Debug, Display},
    hash::Hash,
    net::SocketAddr,
};

use foca::Identity;
use serde::{Deserialize, Serialize};

pub trait ID: Sized + Hash + Display + Clone + Eq + Debug {
    fn prefix(&self) -> &str;
    fn id(&self) -> &str;
    fn renew(&self) -> Option<Self>;
}

// Node Identity is used the identify of node to load balance amongst a hash ring
#[derive(Debug, PartialEq, Eq, Deserialize, Serialize, Clone)]
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

impl<Id: ID + Clone + Eq + Debug + Default> Identity for NodeIdentity<Id> {
    fn has_same_prefix(&self, other: &Self) -> bool {
        other.id().prefix() == self.id().prefix()
    }

    fn renew(&self) -> Option<Self> {
        return match self.id.renew() {
            Some(id) => Some(NodeIdentity::new(id, self.socket_addr)),
            None => None,
        };
    }
}
