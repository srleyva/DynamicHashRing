pub mod node;
pub mod node_membership;

use bytes::{BufMut, Bytes, BytesMut};
use foca::{Config, Foca, Notification, PostcardCodec, Runtime, Timer};
use node::{NodeIdentity, ID};
use node_membership::NodeMembership;
use rand::prelude::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt::{Debug, Display},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc::Sender, Notify, RwLock},
};

#[macro_use]
extern crate log;

#[derive(Default)]
struct HashRingRuntime<Id: ID> {
    pub to_send: Vec<(NodeIdentity<Id>, Bytes)>,
    pub to_schedule: Vec<(Duration, Timer<NodeIdentity<Id>>)>,
    pub notifications: Vec<Notification<NodeIdentity<Id>>>,
    buf: BytesMut,
}

impl<Id: ID> HashRingRuntime<Id> {
    fn new() -> Self {
        Self {
            to_send: vec![],
            to_schedule: vec![],
            notifications: vec![],
            buf: BytesMut::new(),
        }
    }
}

impl<Id: ID + Default> Runtime<NodeIdentity<Id>> for HashRingRuntime<Id> {
    fn notify(&mut self, notification: Notification<NodeIdentity<Id>>) {
        self.notifications.push(notification);
    }

    fn send_to(&mut self, to: NodeIdentity<Id>, data: &[u8]) {
        let mut packet = self.buf.split();
        packet.put_slice(data);
        self.to_send.push((to, packet.freeze()));
    }

    fn submit_after(&mut self, event: Timer<NodeIdentity<Id>>, after: Duration) {
        self.to_schedule.push((after, event));
    }
}

pub struct HashRing<Id: ID> {
    node_membership: NodeMembership<Id>,
    current_node: NodeIdentity<Id>,
    socket: Arc<UdpSocket>, // TODO: Should genericize this to work with any transport
    announce_to: Option<SocketAddr>,
    channels: RwLock<Channels<Id>>,
    config: Config,
    network_ready: Arc<Notify>,
}

impl<Id: ID> HashRing<Id> {
    pub async fn new(
        current_node: NodeIdentity<Id>,
        announce_to: Option<SocketAddr>,
        config: Config,
    ) -> Self {
        let mut node_membership = NodeMembership::new(3);
        node_membership.add_node(current_node.clone()).await;
        let gossip_addr = current_node.socket_addr;
        Self {
            node_membership,
            current_node,
            socket: Arc::new(
                UdpSocket::bind(gossip_addr)
                    .await
                    .expect("could not bind gossip addr"),
            ),
            channels: RwLock::new(Channels {
                send_data: None,
                foca: None,
            }),
            announce_to,
            network_ready: Arc::new(Notify::new()),
            config,
        }
    }
}

impl<Id: ID> HashRing<Id> {
    pub async fn start(&'static self)
    where
        Id: ID
            + Clone
            + Display
            + Default
            + Eq
            + Serialize
            + DeserializeOwned
            + 'static
            + Debug
            + Send
            + Sync,
    {
        tokio::spawn(self.serve());
        tokio::spawn(self.start_gossip())
            .await
            .expect("could not start the gossip");
    }

    async fn start_gossip(&self)
    where
        Id: ID
            + Clone
            + Display
            + Default
            + Eq
            + Serialize
            + DeserializeOwned
            + 'static
            + Debug
            + Send
            + Sync,
    {
        let (tx_foca, mut rx_foca) = tokio::sync::mpsc::channel::<Input<Id>>(100);
        {
            let mut channels = self.channels.write().await;
            channels.foca = Some(tx_foca.clone());
        }

        let buf_len = self.config.max_packet_size.get();

        let mut recv_buf = vec![0u8; buf_len];
        let mut foca = Foca::new(
            self.current_node.clone(),
            self.config.clone(),
            StdRng::from_entropy(),
            PostcardCodec,
        );

        let mut runtime = HashRingRuntime::new();
        let notify = self.network_ready.clone();
        let network_waiter = tokio::spawn(async move {
            notify.notified().await;
        });
        network_waiter.await.unwrap();

        let tx_send_data = {
            let channels = self.channels.read().await;
            channels
                .send_data
                .as_ref()
                .expect("network not initialzied")
                .clone()
        };
        let mut node_membership = self.node_membership.clone();

        tokio::spawn(async move {
            while let Some(input) = rx_foca.recv().await {
                let result = match input {
                    Input::Event(timer) => foca.handle_timer(timer, &mut runtime),
                    Input::Data(data) => foca.handle_data(&data, &mut runtime),
                    Input::Announce(dst) => foca.announce(dst, &mut runtime),
                };

                if let Err(error) = result {
                    error!("Ignored Error: {}", error);
                }

                while let Some((dst, data)) = runtime.to_send.pop() {
                    match tx_send_data.send((dst.socket_addr, data)).await {
                        Ok(_) => (), // debug!("Sent data"),
                        Err(err) => error!("err sending data: {}", err),
                    }
                }

                while let Some((delay, event)) = runtime.to_schedule.pop() {
                    let own_input_handle = tx_foca.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        match own_input_handle.send(Input::Event(event)).await {
                            Ok(_) => (), // debug!("Sent data"),
                            Err(err) => error!("err sending data: {}", err),
                        }
                    });
                }

                let mut active_list_has_changed = false;
                while let Some(notification) = runtime.notifications.pop() {
                    match notification {
                        Notification::MemberUp(id) => {
                            info!("Member Up: {}", id.id);
                            active_list_has_changed |= node_membership.add_node(id).await;
                        }
                        Notification::MemberDown(id) => {
                            info!("Member Down: {}", id.id);
                            active_list_has_changed |= node_membership.remove_node(id).await
                        }

                        other => {
                            debug!("unhandled other: {:?}", other)
                        }
                    }
                }

                if active_list_has_changed {
                    node_membership.print_nodes().await;
                }
            }
        });

        if let Some(dst) = self.announce_to {
            let annouce_node_id = NodeIdentity::new(Id::default(), dst);
            let channels = self.channels.read().await;
            match channels
                .foca
                .as_ref()
                .expect("network not setup")
                .send(Input::Announce(annouce_node_id))
                .await
            {
                Ok(()) => info!("Sending announce to node: {}", dst),
                Err(err) => error!("err joining node {}", err),
            };
        }

        let mut databuf = BytesMut::new();
        info!("Starting Gossip On: {}", self.current_node);
        loop {
            let (len, _from_addr) = self
                .socket
                .recv_from(&mut recv_buf)
                .await
                .expect("network down");
            // Accordinly, we would undo everything that's done prior to
            // sending: decompress, decrypt, remove the envelope
            databuf.put_slice(&recv_buf[..len]);
            let tx_foca = {
                let channels = self.channels.read().await;
                channels.foca.clone()
            };
            // And simply forward it to foca
            let _ignored_send_error = tx_foca
                .as_ref()
                .expect("foca not set up")
                .send(Input::Data(databuf.split().freeze()))
                .await;
        }
    }

    async fn serve(&self) {
        let (tx_send_data, mut rx_send_data) =
            tokio::sync::mpsc::channel::<(SocketAddr, Bytes)>(100);
        {
            let mut channels = self.channels.write().await;
            channels.send_data = Some(tx_send_data);
        }

        let write_socket = self.socket.clone();
        tokio::spawn(async move {
            while let Some((dst, data)) = rx_send_data.recv().await {
                debug!("Sending to dest: {}", &dst);
                match write_socket.send_to(&data, &dst).await {
                    Ok(_sent) => (),
                    Err(err) => error!("err writing to gossip: {}", err),
                };
            }
        });

        self.network_ready.notify_one();
    }
}

struct Channels<Id: ID> {
    send_data: Option<Sender<(SocketAddr, Bytes)>>,
    foca: Option<Sender<Input<Id>>>,
}

enum Input<Id: ID> {
    Event(Timer<NodeIdentity<Id>>),
    Data(Bytes),
    Announce(NodeIdentity<Id>),
}
