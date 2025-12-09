//! Wire protocol integration for subscription management.
//!
//! This module provides methods for bridging wire protocol UUIDs to internal
//! subscription IDs, and for updating subscription state from wire protocol.

use super::SubscriptionManager;
use crate::subscription::{SelectiveColumnConfig, SubscriptionId};

impl SubscriptionManager {
    /// Find a subscription by its wire protocol ID
    ///
    /// # Arguments
    ///
    /// * `wire_id` - The wire protocol subscription ID (UUID bytes)
    ///
    /// # Returns
    ///
    /// The internal subscription ID if found
    pub fn find_subscription_by_wire_id(&self, wire_id: &[u8; 16]) -> Option<SubscriptionId> {
        self.wire_id_index.get(wire_id).map(|r| *r)
    }

    /// Get the subscription count for a specific connection
    ///
    /// # Arguments
    ///
    /// * `connection_id` - The connection ID to check
    ///
    /// # Returns
    ///
    /// Number of subscriptions for this connection
    pub fn connection_subscription_count(&self, connection_id: &str) -> usize {
        self.connection_subscription_counts
            .get(connection_id)
            .map(|c| c.load(std::sync::atomic::Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Get affected subscription details for wire protocol
    ///
    /// This method finds all subscriptions that depend on the given table and returns
    /// their details needed for wire protocol notifications.
    ///
    /// # Arguments
    ///
    /// * `table` - The table name to find affected subscriptions for
    ///
    /// # Returns
    ///
    /// Vector of (wire_subscription_id, query, last_result_hash, last_result) for
    /// each affected subscription that has a wire ID.
    #[allow(clippy::type_complexity)]
    pub fn get_affected_subscriptions_for_wire_protocol(
        &self,
        table: &str,
    ) -> Vec<([u8; 16], String, u64, Option<Vec<crate::Row>>)> {
        let table_lower = table.to_lowercase();
        let subscription_ids: Vec<SubscriptionId> = self
            .table_index
            .get(&table_lower)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();

        subscription_ids
            .into_iter()
            .filter_map(|id| {
                self.subscriptions.get(&id).and_then(|sub| {
                    sub.wire_subscription_id.map(|wire_id| {
                        (wire_id, sub.query.clone(), sub.last_result_hash, sub.last_result.clone())
                    })
                })
            })
            .collect()
    }

    /// Get affected subscription details for wire protocol filtered by connection
    ///
    /// This method finds subscriptions for a specific connection that depend on the
    /// given table and returns their details needed for wire protocol notifications.
    ///
    /// # Arguments
    ///
    /// * `table` - The table name to find affected subscriptions for
    /// * `connection_id` - The connection ID to filter by
    ///
    /// # Returns
    ///
    /// Vector of (wire_subscription_id, query, last_result_hash, last_result, filter) for
    /// each affected subscription that belongs to the specified connection.
    #[allow(clippy::type_complexity)]
    pub fn get_affected_subscriptions_for_connection(
        &self,
        table: &str,
        connection_id: &str,
    ) -> Vec<([u8; 16], String, u64, Option<Vec<crate::Row>>, Option<String>)> {
        let table_lower = table.to_lowercase();
        let subscription_ids: Vec<SubscriptionId> = self
            .table_index
            .get(&table_lower)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();

        subscription_ids
            .into_iter()
            .filter_map(|id| {
                self.subscriptions.get(&id).and_then(|sub| {
                    // Only include subscriptions that belong to this connection
                    if sub.connection_id.as_deref() == Some(connection_id) {
                        sub.wire_subscription_id.map(|wire_id| {
                            (
                                wire_id,
                                sub.query.clone(),
                                sub.last_result_hash,
                                sub.last_result.clone(),
                                sub.filter.clone(),
                            )
                        })
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    /// Update the stored result for a subscription by wire ID
    ///
    /// This is used by wire protocol to store results for delta computation.
    ///
    /// # Arguments
    ///
    /// * `wire_id` - The wire protocol subscription ID (UUID bytes)
    /// * `result_hash` - Hash of the new result set
    /// * `result` - The new result set
    pub fn update_result_by_wire_id(
        &self,
        wire_id: &[u8; 16],
        result_hash: u64,
        result: Vec<crate::Row>,
    ) {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                sub.last_result_hash = result_hash;
                sub.last_result = Some(result);
            }
        }
    }

    /// Update the primary key columns for a subscription by wire ID
    ///
    /// This is called after PK detection to set the actual PK column indices
    /// for selective column updates.
    pub fn update_pk_columns_by_wire_id(&self, wire_id: &[u8; 16], pk_columns: Vec<usize>) {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                sub.pk_columns = pk_columns;
            }
        }
    }

    /// Update PK columns with eligibility tracking
    ///
    /// Sets the PK columns and marks whether the subscription is eligible
    /// for selective column updates (based on confident PK detection).
    ///
    /// Returns `true` if the subscription is newly marked as selective-eligible.
    pub fn update_pk_columns_with_eligibility_by_wire_id(
        &self,
        wire_id: &[u8; 16],
        pk_columns: Vec<usize>,
        confident: bool,
    ) -> bool {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                return sub.set_pk_columns_with_eligibility(pk_columns, confident);
            }
        }
        false
    }

    /// Update PK columns with eligibility tracking by internal subscription ID
    ///
    /// Sets the PK columns and marks whether the subscription is eligible
    /// for selective column updates (based on confident PK detection).
    /// This variant is used for HTTP SSE subscriptions that don't have wire IDs.
    ///
    /// Returns `true` if the subscription is newly marked as selective-eligible.
    pub fn update_pk_columns_with_eligibility(
        &self,
        id: SubscriptionId,
        pk_columns: Vec<usize>,
        confident: bool,
    ) -> bool {
        if let Some(mut sub) = self.subscriptions.get_mut(&id) {
            return sub.set_pk_columns_with_eligibility(pk_columns, confident);
        }
        false
    }

    /// Update selective updates configuration for a subscription
    ///
    /// Allows per-subscription overrides of the server-level selective updates config.
    ///
    /// # Arguments
    ///
    /// * `id` - The subscription ID
    /// * `config` - The new selective updates configuration (Some to set, None to clear)
    pub fn update_selective_updates(&self, id: SubscriptionId, config: SelectiveColumnConfig) {
        if let Some(mut sub) = self.subscriptions.get_mut(&id) {
            sub.set_selective_updates_override(config);
        }
    }

    /// Get the primary key columns for a subscription by wire ID
    ///
    /// Returns the PK column indices, or default [0] if not found.
    pub fn get_pk_columns_by_wire_id(&self, wire_id: &[u8; 16]) -> Vec<usize> {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(sub) = self.subscriptions.get(&id) {
                return sub.pk_columns.clone();
            }
        }
        vec![0] // default
    }

    /// Set per-subscription selective updates configuration override by wire ID
    ///
    /// Allows clients to override server-level selective update thresholds
    /// on a per-subscription basis via the wire protocol.
    pub fn set_selective_updates_override_by_wire_id(
        &self,
        wire_id: &[u8; 16],
        config: SelectiveColumnConfig,
    ) {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                sub.set_selective_updates_override(config);
            }
        }
    }

    /// Get the effective selective column config for a subscription by wire ID
    ///
    /// Returns the per-subscription override config if set, otherwise creates
    /// a config using the server-level settings with subscription-specific pk_columns.
    pub fn get_effective_selective_config_by_wire_id(
        &self,
        wire_id: &[u8; 16],
        server_config: &SelectiveColumnConfig,
    ) -> SelectiveColumnConfig {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(sub) = self.subscriptions.get(&id) {
                return sub.get_effective_selective_config(server_config);
            }
        }
        // Subscription not found, return server config with default pk_columns
        server_config.with_pk_columns(vec![0])
    }
}
