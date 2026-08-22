//! Static cluster membership configuration (`cluster.toml`) for the TCP
//! transport (Raft Phase A3, PR 2 of issue #5197).
//!
//! A [`ClusterConfig`] maps node ids to `host:port` consensus addresses. It
//! is loaded once at startup ([`ClusterConfig::load`]) and handed to
//! [`OpenraftBackend::join_tcp_cluster`]; membership is **static** — adding
//! or removing nodes is a separate later issue (openraft supports it via
//! `change_membership`, so there is no design debt in starting static).
//!
//! ```toml
//! # cluster.toml — one [[node]] table per cluster member.
//! [[node]]
//! id = 1
//! addr = "10.0.0.1"        # no port ⇒ the default consensus port, 5433
//!
//! [[node]]
//! id = 2
//! addr = "10.0.0.2:5433"
//!
//! [[node]]
//! id = 3
//! addr = "raft-3.internal:6000"
//! ```
//!
//! ## Addresses are the local node's *view*
//!
//! The addresses in this config are used for two things on the node that
//! loaded it: binding its own listener (its own entry) and dialing peers
//! (every other entry). They are deliberately **not** written into the
//! replicated Raft membership (`BasicNode` stays empty): replicated
//! membership must be identical on every node, while dial addresses are
//! legitimately per-node (NAT, split-horizon DNS, and the per-edge proxies
//! the partition tests use). Two nodes may therefore reach the same peer
//! through different addresses without corrupting shared state.
//!
//! The consensus port (default [`DEFAULT_CONSENSUS_PORT`], 5433) is distinct
//! from the SQL port on purpose. Note the port-defaulting rule keys on the
//! absence of `:` in the address, so IPv6 literals must always carry an
//! explicit port (`[::1]:5433`).

use std::{collections::BTreeMap, path::Path};

use openraft::BasicNode;
use serde::Deserialize;

use crate::backend::{ConsensusError, Result};

/// Default TCP port for consensus traffic, distinct from the SQL port.
pub const DEFAULT_CONSENSUS_PORT: u16 = 5433;

/// Static membership of a consensus cluster: node id → `host:port`.
///
/// See the [module docs](self) for the `cluster.toml` format and the
/// per-node-view addressing rationale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    nodes: BTreeMap<u64, String>,
}

/// serde mirror of the `cluster.toml` file.
#[derive(Deserialize)]
struct ConfigFile {
    #[serde(default)]
    node: Vec<NodeEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NodeEntry {
    id: u64,
    addr: String,
}

impl ClusterConfig {
    /// Build a config from `(node_id, addr)` pairs. An `addr` without a
    /// port gets [`DEFAULT_CONSENSUS_PORT`] appended.
    ///
    /// Fails on an empty member list, duplicate ids, or empty addresses.
    pub fn new(nodes: impl IntoIterator<Item = (u64, String)>) -> Result<Self> {
        let mut map = BTreeMap::new();
        for (id, addr) in nodes {
            if addr.trim().is_empty() {
                return Err(ConsensusError::Config(format!("node {id} has an empty address")));
            }
            let addr =
                if addr.contains(':') { addr } else { format!("{addr}:{DEFAULT_CONSENSUS_PORT}") };
            if map.insert(id, addr).is_some() {
                return Err(ConsensusError::Config(format!("duplicate node id {id}")));
            }
        }
        if map.is_empty() {
            return Err(ConsensusError::Config("cluster config has no nodes".to_string()));
        }
        Ok(Self { nodes: map })
    }

    /// Parse a config from `cluster.toml` text.
    pub fn from_toml_str(text: &str) -> Result<Self> {
        let file: ConfigFile = toml::from_str(text)
            .map_err(|e| ConsensusError::Config(format!("invalid cluster.toml: {e}")))?;
        Self::new(file.node.into_iter().map(|n| (n.id, n.addr)))
    }

    /// Load a config from a `cluster.toml` file on disk.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| {
            ConsensusError::Config(format!("failed to read {}: {e}", path.display()))
        })?;
        Self::from_toml_str(&text)
    }

    /// The `host:port` address of `node_id`, if it is a member.
    pub fn addr(&self, node_id: u64) -> Option<&str> {
        self.nodes.get(&node_id).map(String::as_str)
    }

    /// Ids of all cluster members, ascending.
    pub fn node_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.nodes.keys().copied()
    }

    /// Number of cluster members.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Always `false` (construction rejects empty configs); provided for
    /// `len`/`is_empty` convention completeness.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The replicated openraft membership derived from this config: ids
    /// only, with empty [`BasicNode`]s — dial addresses stay local (see the
    /// module docs).
    pub(crate) fn membership(&self) -> BTreeMap<u64, BasicNode> {
        self.nodes.keys().map(|id| (*id, BasicNode::default())).collect()
    }

    /// Dial-address map handed to the TCP network factory.
    pub(crate) fn dial_addrs(&self) -> BTreeMap<u64, String> {
        self.nodes.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact example from the module docs parses, with the default port
    /// applied to the entry that omits one.
    #[test]
    fn parses_the_documented_example() {
        let config = ClusterConfig::from_toml_str(
            r#"
            [[node]]
            id = 1
            addr = "10.0.0.1"

            [[node]]
            id = 2
            addr = "10.0.0.2:5433"

            [[node]]
            id = 3
            addr = "raft-3.internal:6000"
            "#,
        )
        .unwrap();

        assert_eq!(config.len(), 3);
        assert_eq!(config.addr(1), Some("10.0.0.1:5433"));
        assert_eq!(config.addr(2), Some("10.0.0.2:5433"));
        assert_eq!(config.addr(3), Some("raft-3.internal:6000"));
        assert_eq!(config.node_ids().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(config.addr(4), None);
    }

    #[test]
    fn rejects_duplicate_node_ids() {
        let err = ClusterConfig::from_toml_str(
            r#"
            [[node]]
            id = 1
            addr = "a:1"

            [[node]]
            id = 1
            addr = "b:2"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, ConsensusError::Config(ref m) if m.contains("duplicate")), "{err}");
    }

    #[test]
    fn rejects_empty_configs_and_empty_addresses() {
        assert!(matches!(ClusterConfig::from_toml_str("").unwrap_err(), ConsensusError::Config(_)));
        assert!(matches!(
            ClusterConfig::new([(1, String::new())]).unwrap_err(),
            ConsensusError::Config(_)
        ));
    }

    #[test]
    fn rejects_unknown_fields_and_malformed_toml() {
        let err = ClusterConfig::from_toml_str(
            r#"
            [[node]]
            id = 1
            addr = "a:1"
            sql_port = 5432
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, ConsensusError::Config(_)), "{err}");

        assert!(matches!(
            ClusterConfig::from_toml_str("not toml at all [").unwrap_err(),
            ConsensusError::Config(_)
        ));
    }

    #[test]
    fn loads_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.toml");
        std::fs::write(&path, "[[node]]\nid = 1\naddr = \"127.0.0.1\"\n").unwrap();

        let config = ClusterConfig::load(&path).unwrap();
        assert_eq!(config.addr(1), Some("127.0.0.1:5433"));

        let err = ClusterConfig::load(dir.path().join("missing.toml")).unwrap_err();
        assert!(matches!(err, ConsensusError::Config(_)), "{err}");
    }
}
