use crate::node::{NodeIdentity, ID};
use hash_ring::HashRing;
use std::{fmt::Display, sync::Arc};
use tokio::sync::RwLock;

pub struct NodeMembership<Id: ID> {
    inner: Arc<RwLock<NodeMembershipInner<Id>>>,
}

impl<Id: ID> Clone for NodeMembership<Id> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

unsafe impl<Id: ID> Send for NodeMembership<Id> {}
unsafe impl<Id: ID> Sync for NodeMembership<Id> {}

impl<Id: ID + Clone> NodeMembership<Id> {
    pub fn new(replicas: isize) -> Self {
        let inner = NodeMembershipInner {
            nodes: vec![],
            membership: HashRing::new(vec![], replicas),
        };
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub async fn print_nodes(&mut self) {
        let inner = self.inner.read().await;
        let nodes = inner.nodes.iter().fold(String::new(), |current, node| {
            format!("{} [{}]", current, node)
        });

        info!("Nodes: {}", nodes);
    }

    pub async fn add_node(&mut self, node: NodeIdentity<Id>) -> bool
    where
        Id: PartialEq + Eq,
    {
        let mut membership = self.inner.write().await;
        if membership.nodes.contains(&node) {
            false
        } else {
            membership.membership.add_node(&node);
            membership.nodes.push(node);
            true
        }
    }

    pub async fn remove_node(&self, node: NodeIdentity<Id>) -> bool
    where
        Id: PartialEq + Eq,
    {
        let mut membership = self.inner.write().await;
        return if !membership.nodes.contains(&node) {
            false
        } else {
            membership.membership.remove_node(&node);
            let index = membership
                .nodes
                .iter()
                .position(|x| *x.id().id() == *node.id().id())
                .unwrap();
            membership.nodes.remove(index);
            true
        };
    }

    pub async fn get_node<I>(&self, item: I) -> Option<NodeIdentity<Id>>
    where
        I: Into<String>,
    {
        let membership = self.inner.read().await;
        membership.membership.get_node(item.into()).cloned()
    }
}

struct NodeMembershipInner<Id: ID> {
    pub nodes: Vec<NodeIdentity<Id>>,
    pub membership: HashRing<NodeIdentity<Id>>,
}

impl<Id: Display + ID> Display for NodeMembershipInner<Id> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let nodes = self.nodes.iter().fold(String::new(), |current, node| {
            format!("{} {}", current, node)
        });
        write!(f, "{}", nodes)
    }
}
