use tokio::sync::OnceCell;

use dynamic_hash_ring::{
    node::{NodeIdentity, NodeUUID},
    HashRing,
};
use foca::Config;

lazy_static::lazy_static! {
    static ref SERVER: OnceCell<HashRing<NodeUUID>> = OnceCell::new();
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect::<Vec<String>>();
    let addr = args.get(1).unwrap();
    let announce_to = args.get(2).map(|addr| addr.parse().unwrap());
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
