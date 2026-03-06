//! Raft state machine implementation for bisque-meta.
//!
//! `MetaStateMachine` implements openraft's `RaftStateMachine` trait,
//! dispatching applied log entries to the `MetaEngine` which holds the
//! in-memory registries of tenants, catalogs, API keys, and routing.

use std::io::{self, Cursor};
use std::sync::Arc;

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StoredMembership};
use tracing::{info, warn};

use crate::MetaTypeConfig;
use crate::engine::MetaEngine;
use crate::types::{EngineType, MetaCommand, MetaResponse, MetaSnapshotData};

/// Raft state machine that drives the MetaEngine control plane.
pub struct MetaStateMachine {
    engine: Arc<MetaEngine>,
    last_applied: Option<LogId<MetaTypeConfig>>,
    last_membership: StoredMembership<MetaTypeConfig>,
}

impl MetaStateMachine {
    pub fn new(engine: Arc<MetaEngine>) -> Self {
        Self {
            engine,
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }

    pub fn engine(&self) -> &Arc<MetaEngine> {
        &self.engine
    }

    pub fn apply_command(&self, cmd: MetaCommand) -> MetaResponse {
        match cmd {
            MetaCommand::CreateAccount { name } => {
                match self.engine.create_account(name) {
                    Ok(account_id) => MetaResponse::AccountCreated { account_id },
                    Err(e) => MetaResponse::Error(e.to_string()),
                }
            }

            MetaCommand::DeleteAccount { account_id } => {
                match self.engine.delete_account(account_id) {
                    Ok(()) => MetaResponse::Ok,
                    Err(e) => MetaResponse::Error(e.to_string()),
                }
            }

            MetaCommand::CreateUser {
                username,
                password_hash,
            } => match self.engine.create_user(username, password_hash) {
                Ok(user_id) => MetaResponse::UserCreated { user_id },
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::DisableUser { user_id } => {
                match self.engine.disable_user(user_id) {
                    Ok(()) => MetaResponse::Ok,
                    Err(e) => MetaResponse::Error(e.to_string()),
                }
            }

            MetaCommand::UpdatePassword {
                user_id,
                password_hash,
            } => match self.engine.update_password(user_id, password_hash) {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::AddAccountMembership {
                user_id,
                account_id,
                role,
            } => match self.engine.add_membership(user_id, account_id, role) {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::RemoveAccountMembership {
                user_id,
                account_id,
            } => match self.engine.remove_membership(user_id, account_id) {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::SetAccountRole {
                user_id,
                account_id,
                role,
            } => match self.engine.set_account_role(user_id, account_id, role) {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::GrantTenantAccess {
                user_id,
                tenant_id,
                catalog_access,
            } => match self
                .engine
                .grant_tenant_access(user_id, tenant_id, catalog_access)
            {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::RevokeTenantAccess {
                user_id,
                tenant_id,
            } => match self.engine.revoke_tenant_access(user_id, tenant_id) {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::CreateTenant {
                account_id,
                name,
                limits,
            } => {
                match self.engine.create_tenant(account_id, name.clone(), limits.clone()) {
                    Ok(tenant_id) => {
                        // Auto-create _otel catalog if configured
                        if self.engine.config().auto_create_otel_catalog {
                            if let Err(e) = self.engine.create_catalog(
                                tenant_id,
                                "_otel".to_string(),
                                EngineType::Lance,
                                "{}".into(),
                            ) {
                                warn!(
                                    tenant_id,
                                    error = %e,
                                    "failed to auto-create _otel catalog"
                                );
                            }
                        }
                        MetaResponse::TenantCreated { tenant_id }
                    }
                    Err(e) => MetaResponse::Error(e.to_string()),
                }
            }

            MetaCommand::UpdateTenantLimits { tenant_id, limits } => {
                match self.engine.update_tenant_limits(tenant_id, limits) {
                    Ok(()) => MetaResponse::Ok,
                    Err(e) => MetaResponse::Error(e.to_string()),
                }
            }

            MetaCommand::DeleteTenant { tenant_id } => {
                match self.engine.delete_tenant(tenant_id) {
                    Ok(()) => MetaResponse::Ok,
                    Err(e) => MetaResponse::Error(e.to_string()),
                }
            }

            MetaCommand::CreateCatalog {
                tenant_id,
                name,
                engine,
                config,
            } => match self.engine.create_catalog(tenant_id, name, engine, config) {
                Ok((catalog_id, raft_group_id)) => MetaResponse::CatalogCreated {
                    catalog_id,
                    raft_group_id,
                },
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::UpdateCatalogStatus { catalog_id, status } => {
                match self.engine.update_catalog_status(catalog_id, status) {
                    Ok(()) => MetaResponse::Ok,
                    Err(e) => MetaResponse::Error(e.to_string()),
                }
            }

            MetaCommand::UpdateCatalogPlacement {
                catalog_id,
                placement,
            } => match self.engine.update_catalog_placement(catalog_id, placement) {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::DeleteCatalog {
                tenant_id,
                catalog_id,
            } => match self.engine.delete_catalog(tenant_id, catalog_id) {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::UpdateLeader {
                raft_group_id,
                leader_node_id,
            } => match self.engine.update_leader(raft_group_id, leader_node_id) {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },

            MetaCommand::CreateApiKey { tenant_id, scopes } => {
                match self.engine.create_api_key(tenant_id, scopes) {
                    Ok((key_id, raw_key)) => MetaResponse::ApiKeyCreated { key_id, raw_key },
                    Err(e) => MetaResponse::Error(e.to_string()),
                }
            }

            MetaCommand::RevokeApiKey { key_id } => {
                match self.engine.revoke_api_key(key_id) {
                    Ok(()) => MetaResponse::Ok,
                    Err(e) => MetaResponse::Error(e.to_string()),
                }
            }

            MetaCommand::ReportUsage {
                catalog_id,
                disk_bytes,
                deep_storage_bytes,
            } => match self.engine.report_usage(catalog_id, disk_bytes, deep_storage_bytes) {
                Ok(()) => MetaResponse::Ok,
                Err(e) => MetaResponse::Error(e.to_string()),
            },
        }
    }
}

impl RaftStateMachine<MetaTypeConfig> for MetaStateMachine {
    type SnapshotBuilder = MetaSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<MetaTypeConfig>>,
            StoredMembership<MetaTypeConfig>,
        ),
        io::Error,
    > {
        // No MDBX for v1 — purely in-memory with Raft snapshots for recovery.
        Ok((self.last_applied.clone(), self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<
                Item = Result<openraft::storage::EntryResponder<MetaTypeConfig>, io::Error>,
            > + Unpin
            + OptionalSend,
    {
        while let Some(entry_result) = entries.next().await {
            let (entry, responder) = entry_result?;
            self.last_applied = Some(entry.log_id.clone());

            let response = match entry.payload {
                EntryPayload::Blank => MetaResponse::Ok,
                EntryPayload::Normal(cmd) => self.apply_command(cmd),
                EntryPayload::Membership(m) => {
                    self.last_membership =
                        StoredMembership::new(Some(entry.log_id.clone()), m);
                    MetaResponse::Ok
                }
            };

            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetaSnapshotBuilder {
            data: self.engine.snapshot_data(),
            last_applied: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<MetaTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let (data, _): (MetaSnapshotData, _) =
            bincode::serde::decode_from_slice(snapshot.get_ref(), bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        info!(
            ?meta.last_log_id,
            tenants = data.tenants.len(),
            catalogs = data.catalogs.len(),
            "installing meta snapshot"
        );

        self.engine.restore_from_snapshot(data);
        self.last_applied = meta.last_log_id.clone();
        self.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetaTypeConfig>>, io::Error> {
        if self.last_applied.is_none() {
            return Ok(None);
        }
        let mut builder = self.get_snapshot_builder().await;
        match builder.build_snapshot().await {
            Ok(snap) => Ok(Some(snap)),
            Err(e) => {
                warn!("failed to build meta snapshot: {}", e);
                Ok(None)
            }
        }
    }
}

/// Builds snapshots from the current MetaEngine state.
pub struct MetaSnapshotBuilder {
    data: MetaSnapshotData,
    last_applied: Option<LogId<MetaTypeConfig>>,
    last_membership: StoredMembership<MetaTypeConfig>,
}

impl RaftSnapshotBuilder<MetaTypeConfig> for MetaSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetaTypeConfig>, io::Error> {
        let bytes = bincode::serde::encode_to_vec(&self.data, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let last_applied = self.last_applied.clone().unwrap_or(LogId {
            leader_id: openraft::impls::leader_id_adv::LeaderId {
                term: 0,
                node_id: 0,
            },
            index: 0,
        });

        let meta = SnapshotMeta {
            last_log_id: Some(last_applied),
            last_membership: self.last_membership.clone(),
            snapshot_id: format!(
                "meta-snap-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            ),
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MetaConfig;
    use crate::types::{CatalogStatus, Scope, TenantLimits};

    fn test_engine() -> Arc<MetaEngine> {
        let engine = Arc::new(MetaEngine::new(MetaConfig::new(b"test-secret".to_vec())));
        engine.create_account("test-org".into()).unwrap();
        engine
    }

    const TEST_ACCOUNT: u64 = 1;

    #[test]
    fn test_apply_create_tenant() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        let resp = sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });

        match resp {
            MetaResponse::TenantCreated { tenant_id } => {
                assert_eq!(tenant_id, 1);
                let t = engine.get_tenant(1).unwrap();
                assert_eq!(t.name, "acme");
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }

    #[test]
    fn test_apply_create_tenant_auto_otel() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });

        // _otel catalog should be auto-created
        let catalogs = engine.list_catalogs_for_tenant(1);
        assert_eq!(catalogs.len(), 1);
        assert_eq!(catalogs[0].name, "_otel");
        assert_eq!(catalogs[0].engine, EngineType::Lance);
    }

    #[test]
    fn test_apply_sequence() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        // Create tenant
        let MetaResponse::TenantCreated { tenant_id } = sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        }) else {
            panic!("expected TenantCreated");
        };

        // Create catalog
        let MetaResponse::CatalogCreated {
            catalog_id,
            raft_group_id,
        } = sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id,
            name: "analytics".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        })
        else {
            panic!("expected CatalogCreated");
        };

        // Update status
        assert!(matches!(
            sm.apply_command(MetaCommand::UpdateCatalogStatus {
                catalog_id,
                status: CatalogStatus::Active,
            }),
            MetaResponse::Ok
        ));

        // Update leader
        assert!(matches!(
            sm.apply_command(MetaCommand::UpdateLeader {
                raft_group_id,
                leader_node_id: Some(1),
            }),
            MetaResponse::Ok
        ));

        // Create API key
        let MetaResponse::ApiKeyCreated { key_id, raw_key } =
            sm.apply_command(MetaCommand::CreateApiKey {
                tenant_id,
                scopes: vec![],
            })
        else {
            panic!("expected ApiKeyCreated");
        };
        assert!(!raw_key.is_empty());

        // Revoke API key
        assert!(matches!(
            sm.apply_command(MetaCommand::RevokeApiKey { key_id }),
            MetaResponse::Ok
        ));

        // Report usage
        assert!(matches!(
            sm.apply_command(MetaCommand::ReportUsage {
                catalog_id,
                disk_bytes: 1024,
                deep_storage_bytes: 4096,
            }),
            MetaResponse::Ok
        ));

        // Verify final state
        let cat = engine.get_catalog(catalog_id).unwrap();
        assert_eq!(cat.status, CatalogStatus::Active);

        let re = engine.get_routing(raft_group_id).unwrap();
        assert_eq!(re.leader_node_id, Some(1));

        let key = engine.get_api_key(key_id).unwrap();
        assert!(key.revoked);

        let usage = engine.get_usage(catalog_id).unwrap();
        assert_eq!(usage.disk_bytes, 1024);
    }

    #[test]
    fn test_snapshot_build_and_restore() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        // Create some state
        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });
        sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id: 1,
            name: "analytics".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        });

        // Snapshot
        let snap = engine.snapshot_data();
        let bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();

        // Restore into fresh engine
        let engine2 = test_engine();
        let (data, _): (MetaSnapshotData, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        engine2.restore_from_snapshot(data);

        assert_eq!(engine2.list_tenants().len(), 1);
        // _otel + analytics = 2 catalogs
        assert_eq!(engine2.list_catalogs_for_tenant(1).len(), 2);
    }

    // ── Error dispatch tests ─────────────────────────────────────────

    #[test]
    fn test_apply_create_tenant_duplicate_returns_error() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });

        let resp = sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });
        assert!(matches!(resp, MetaResponse::Error(_)));
    }

    #[test]
    fn test_apply_update_tenant_limits() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });

        let new_limits = TenantLimits {
            max_catalogs: 128,
            ..TenantLimits::default()
        };
        let resp = sm.apply_command(MetaCommand::UpdateTenantLimits {
            tenant_id: 1,
            limits: new_limits.clone(),
        });
        assert!(matches!(resp, MetaResponse::Ok));
        assert_eq!(engine.get_tenant(1).unwrap().limits, new_limits);
    }

    #[test]
    fn test_apply_update_tenant_limits_not_found() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        let resp = sm.apply_command(MetaCommand::UpdateTenantLimits {
            tenant_id: 999,
            limits: TenantLimits::default(),
        });
        assert!(matches!(resp, MetaResponse::Error(_)));
    }

    #[test]
    fn test_apply_delete_tenant() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });

        // Must mark all catalogs as deleted first (_otel was auto-created)
        let catalogs = engine.list_catalogs_for_tenant(1);
        for c in &catalogs {
            sm.apply_command(MetaCommand::UpdateCatalogStatus {
                catalog_id: c.catalog_id,
                status: CatalogStatus::Deleted,
            });
        }

        let resp = sm.apply_command(MetaCommand::DeleteTenant { tenant_id: 1 });
        assert!(matches!(resp, MetaResponse::Ok));
        assert!(engine.get_tenant(1).is_none());
    }

    #[test]
    fn test_apply_delete_tenant_with_active_catalogs() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });

        // Tenant has auto-created _otel, can't delete
        let resp = sm.apply_command(MetaCommand::DeleteTenant { tenant_id: 1 });
        assert!(matches!(resp, MetaResponse::Error(_)));
    }

    #[test]
    fn test_apply_update_catalog_status() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });
        sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id: 1,
            name: "analytics".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        });

        let cat = engine.find_catalog_by_name(1, "analytics").unwrap();

        let resp = sm.apply_command(MetaCommand::UpdateCatalogStatus {
            catalog_id: cat.catalog_id,
            status: CatalogStatus::Active,
        });
        assert!(matches!(resp, MetaResponse::Ok));
        assert_eq!(
            engine.get_catalog(cat.catalog_id).unwrap().status,
            CatalogStatus::Active
        );
    }

    #[test]
    fn test_apply_update_catalog_placement() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });
        sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id: 1,
            name: "analytics".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        });

        let cat = engine.find_catalog_by_name(1, "analytics").unwrap();

        let resp = sm.apply_command(MetaCommand::UpdateCatalogPlacement {
            catalog_id: cat.catalog_id,
            placement: vec![10, 20, 30],
        });
        assert!(matches!(resp, MetaResponse::Ok));

        let updated = engine.get_catalog(cat.catalog_id).unwrap();
        assert_eq!(updated.placement, vec![10, 20, 30]);

        let routing = engine.get_routing(cat.raft_group_id).unwrap();
        assert_eq!(routing.node_addresses.len(), 3);
    }

    #[test]
    fn test_apply_delete_catalog() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });
        sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id: 1,
            name: "analytics".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        });

        let cat = engine.find_catalog_by_name(1, "analytics").unwrap();

        let resp = sm.apply_command(MetaCommand::DeleteCatalog {
            tenant_id: 1,
            catalog_id: cat.catalog_id,
        });
        assert!(matches!(resp, MetaResponse::Ok));
        assert_eq!(
            engine.get_catalog(cat.catalog_id).unwrap().status,
            CatalogStatus::Deleting
        );
    }

    #[test]
    fn test_apply_update_leader() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });
        sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id: 1,
            name: "analytics".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        });

        let cat = engine.find_catalog_by_name(1, "analytics").unwrap();

        let resp = sm.apply_command(MetaCommand::UpdateLeader {
            raft_group_id: cat.raft_group_id,
            leader_node_id: Some(42),
        });
        assert!(matches!(resp, MetaResponse::Ok));

        let routing = engine.get_routing(cat.raft_group_id).unwrap();
        assert_eq!(routing.leader_node_id, Some(42));
    }

    #[test]
    fn test_apply_create_api_key() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });

        let resp = sm.apply_command(MetaCommand::CreateApiKey {
            tenant_id: 1,
            scopes: vec![Scope::TenantAdmin, Scope::Catalog("analytics".into())],
        });
        match resp {
            MetaResponse::ApiKeyCreated { key_id, raw_key } => {
                assert_eq!(key_id, 1);
                assert!(!raw_key.is_empty());
                let key = engine.get_api_key(key_id).unwrap();
                assert_eq!(key.tenant_id, 1);
                assert_eq!(key.scopes.len(), 2);
                assert!(!key.revoked);
            }
            other => panic!("expected ApiKeyCreated, got {other:?}"),
        }
    }

    #[test]
    fn test_apply_revoke_api_key() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });

        let MetaResponse::ApiKeyCreated { key_id, .. } =
            sm.apply_command(MetaCommand::CreateApiKey {
                tenant_id: 1,
                scopes: vec![],
            })
        else {
            panic!("expected ApiKeyCreated");
        };

        let resp = sm.apply_command(MetaCommand::RevokeApiKey { key_id });
        assert!(matches!(resp, MetaResponse::Ok));
        assert!(engine.get_api_key(key_id).unwrap().revoked);
    }

    #[test]
    fn test_apply_revoke_api_key_not_found() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        let resp = sm.apply_command(MetaCommand::RevokeApiKey { key_id: 999 });
        assert!(matches!(resp, MetaResponse::Error(_)));
    }

    #[test]
    fn test_apply_report_usage() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });
        sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id: 1,
            name: "analytics".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        });

        let cat = engine.find_catalog_by_name(1, "analytics").unwrap();

        let resp = sm.apply_command(MetaCommand::ReportUsage {
            catalog_id: cat.catalog_id,
            disk_bytes: 2048,
            deep_storage_bytes: 8192,
        });
        assert!(matches!(resp, MetaResponse::Ok));

        let usage = engine.get_usage(cat.catalog_id).unwrap();
        assert_eq!(usage.disk_bytes, 2048);
        assert_eq!(usage.deep_storage_bytes, 8192);
    }

    #[test]
    fn test_apply_report_usage_catalog_not_found() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        let resp = sm.apply_command(MetaCommand::ReportUsage {
            catalog_id: 999,
            disk_bytes: 100,
            deep_storage_bytes: 200,
        });
        assert!(matches!(resp, MetaResponse::Error(_)));
    }

    #[test]
    fn test_apply_create_catalog_not_found_tenant() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        let resp = sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id: 999,
            name: "x".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        });
        assert!(matches!(resp, MetaResponse::Error(_)));
    }

    // ── Auto _otel disabled ──────────────────────────────────────────

    #[test]
    fn test_apply_create_tenant_no_auto_otel() {
        let config = MetaConfig::new(b"test-secret".to_vec()).with_auto_create_otel_catalog(false);
        let engine = Arc::new(MetaEngine::new(config));
        engine.create_account("test-org".into()).unwrap();
        let sm = MetaStateMachine::new(engine.clone());

        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        });

        // No _otel catalog should exist
        let catalogs = engine.list_catalogs_for_tenant(1);
        assert_eq!(catalogs.len(), 0);
    }

    // ── Multi-tenant through state machine ───────────────────────────

    #[test]
    fn test_apply_multi_tenant_full_lifecycle() {
        let engine = test_engine();
        let sm = MetaStateMachine::new(engine.clone());

        // Create two tenants
        let MetaResponse::TenantCreated { tenant_id: t1 } =
            sm.apply_command(MetaCommand::CreateTenant {
                account_id: TEST_ACCOUNT,
                name: "acme".into(),
                limits: TenantLimits::default(),
            })
        else {
            panic!("expected TenantCreated");
        };

        let MetaResponse::TenantCreated { tenant_id: t2 } =
            sm.apply_command(MetaCommand::CreateTenant {
                account_id: TEST_ACCOUNT,
                name: "beta".into(),
                limits: TenantLimits::default(),
            })
        else {
            panic!("expected TenantCreated");
        };

        assert_ne!(t1, t2);

        // Create catalogs for each with the same name
        let MetaResponse::CatalogCreated {
            catalog_id: c1, ..
        } = sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id: t1,
            name: "analytics".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        })
        else {
            panic!("expected CatalogCreated");
        };

        let MetaResponse::CatalogCreated {
            catalog_id: c2, ..
        } = sm.apply_command(MetaCommand::CreateCatalog {
            tenant_id: t2,
            name: "analytics".into(),
            engine: EngineType::Mq,
            config: "{}".into(),
        })
        else {
            panic!("expected CatalogCreated");
        };

        assert_ne!(c1, c2);

        // Activate both
        sm.apply_command(MetaCommand::UpdateCatalogStatus {
            catalog_id: c1,
            status: CatalogStatus::Active,
        });
        sm.apply_command(MetaCommand::UpdateCatalogStatus {
            catalog_id: c2,
            status: CatalogStatus::Active,
        });

        // Report usage for both
        sm.apply_command(MetaCommand::ReportUsage {
            catalog_id: c1,
            disk_bytes: 1000,
            deep_storage_bytes: 2000,
        });
        sm.apply_command(MetaCommand::ReportUsage {
            catalog_id: c2,
            disk_bytes: 3000,
            deep_storage_bytes: 4000,
        });

        // Verify isolation
        let (d1, s1) = engine.get_tenant_usage(t1);
        let (d2, s2) = engine.get_tenant_usage(t2);
        assert_eq!(d1, 1000);
        assert_eq!(s1, 2000);
        assert_eq!(d2, 3000);
        assert_eq!(s2, 4000);

        // Create API keys for each
        let MetaResponse::ApiKeyCreated {
            key_id: k1,
            raw_key: raw1,
        } = sm.apply_command(MetaCommand::CreateApiKey {
            tenant_id: t1,
            scopes: vec![Scope::TenantAdmin],
        })
        else {
            panic!("expected ApiKeyCreated");
        };

        let MetaResponse::ApiKeyCreated {
            key_id: k2,
            raw_key: raw2,
        } = sm.apply_command(MetaCommand::CreateApiKey {
            tenant_id: t2,
            scopes: vec![Scope::CatalogRead("analytics".into())],
        })
        else {
            panic!("expected ApiKeyCreated");
        };

        assert_ne!(k1, k2);
        assert_ne!(raw1, raw2);

        // Snapshot and restore
        let snap = engine.snapshot_data();
        let bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();

        let config2 = MetaConfig::new(b"test-secret".to_vec());
        let engine2 = Arc::new(MetaEngine::new(config2));
        let (data, _): (MetaSnapshotData, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        engine2.restore_from_snapshot(data);

        // Verify restored state
        assert_eq!(engine2.list_tenants().len(), 2);
        assert!(engine2.get_tenant(t1).is_some());
        assert!(engine2.get_tenant(t2).is_some());

        // _otel + analytics per tenant
        assert_eq!(engine2.list_catalogs_for_tenant(t1).len(), 2);
        assert_eq!(engine2.list_catalogs_for_tenant(t2).len(), 2);

        assert!(engine2.get_api_key(k1).is_some());
        assert!(engine2.get_api_key(k2).is_some());
    }
}
