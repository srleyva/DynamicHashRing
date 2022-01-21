use std::{fmt::Display, slice::SliceIndex};
use tokio::sync::OnceCell;

use dynamic_hash_ring::{
    node::{NodeIdentity, ID},
    HashRing,
};
use foca::Config;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[macro_use]
extern crate log;

lazy_static::lazy_static! {
    static ref SERVER: OnceCell<HashRing<NodeUUID>> = OnceCell::new();
}

#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NodeUUID {
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

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect::<Vec<String>>();
    let addr = args.get(1).unwrap();
    let announce_to = match args.get(2) {
        None => None,
        Some(addr) => Some(addr.parse().unwrap()),
    };
    env_logger::init();
    SERVER
        .get_or_init(|| async {
            HashRing::new(
                NodeIdentity::new(NodeUUID::default(), addr.parse().expect("bad node port")),
                announce_to,
                Config::simple(),
            )
            .await
        })
        .await
        .start()
        .await;
}
