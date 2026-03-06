//! MetaRaftNode — public API wrapper around `Raft<MetaTypeConfig>`.
//!
//! Mirrors the `LanceRaftNode` pattern from bisque-lance: typed write methods
//! that construct `MetaCommand`s and propose them through Raft consensus,
//! plus direct read methods from the in-memory `MetaEngine`.

use std::sync::Arc;

use openraft::Raft;
use openraft::async_runtime::WatchReceiver;
use openraft::error::{ClientWriteError, RaftError};
use openraft::impls::BasicNode;

use crate::MetaTypeConfig;
use crate::engine::MetaEngine;
use crate::types::*;

/// Wraps `Raft<MetaTypeConfig>` with a typed control plane API.
pub struct MetaRaftNode {
    raft: Raft<MetaTypeConfig>,
    engine: Arc<MetaEngine>,
    node_id: u64,
    group_id: u64,
}

impl MetaRaftNode {
    pub fn new(raft: Raft<MetaTypeConfig>, engine: Arc<MetaEngine>, node_id: u64) -> Self {
        Self {
            raft,
            engine,
            node_id,
            group_id: 0,
        }
    }

    pub fn with_group_id(mut self, group_id: u64) -> Self {
        self.group_id = group_id;
        self
    }

    // ── Core propose ───────────────────────────────────────────────────

    pub async fn propose(&self, cmd: MetaCommand) -> Result<MetaResponse, WriteError> {
        let result = self.raft.client_write(cmd).await;
        match result {
            Ok(resp) => {
                let response = resp.response().clone();
                if let MetaResponse::Error(e) = &response {
                    return Err(WriteError::Application(e.clone()));
                }
                Ok(response)
            }
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                Err(WriteError::NotLeader {
                    leader_id: fwd.leader_id,
                    leader_node: fwd.leader_node,
                })
            }
            Err(RaftError::APIError(e)) => Err(WriteError::Raft(e.to_string())),
            Err(RaftError::Fatal(e)) => Err(WriteError::Fatal(e.to_string())),
        }
    }

    // ── Tenant API ─────────────────────────────────────────────────────

    pub async fn create_account(&self, name: String) -> Result<u64, WriteError> {
        let resp = self
            .propose(MetaCommand::CreateAccount { name })
            .await?;
        match resp {
            MetaResponse::AccountCreated { account_id } => Ok(account_id),
            other => Err(WriteError::Application(format!("unexpected: {other}"))),
        }
    }

    pub async fn delete_account(&self, account_id: u64) -> Result<(), WriteError> {
        self.propose(MetaCommand::DeleteAccount { account_id })
            .await?;
        Ok(())
    }

    pub async fn create_user(
        &self,
        username: String,
        password_hash: Vec<u8>,
    ) -> Result<u64, WriteError> {
        let resp = self
            .propose(MetaCommand::CreateUser {
                username,
                password_hash,
            })
            .await?;
        match resp {
            MetaResponse::UserCreated { user_id } => Ok(user_id),
            other => Err(WriteError::Application(format!("unexpected: {other}"))),
        }
    }

    pub async fn disable_user(&self, user_id: u64) -> Result<(), WriteError> {
        self.propose(MetaCommand::DisableUser { user_id }).await?;
        Ok(())
    }

    pub async fn update_password(
        &self,
        user_id: u64,
        password_hash: Vec<u8>,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::UpdatePassword {
            user_id,
            password_hash,
        })
        .await?;
        Ok(())
    }

    pub async fn add_membership(
        &self,
        user_id: u64,
        account_id: u64,
        role: AccountRole,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::AddAccountMembership {
            user_id,
            account_id,
            role,
        })
        .await?;
        Ok(())
    }

    pub async fn remove_membership(
        &self,
        user_id: u64,
        account_id: u64,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::RemoveAccountMembership {
            user_id,
            account_id,
        })
        .await?;
        Ok(())
    }

    pub async fn set_account_role(
        &self,
        user_id: u64,
        account_id: u64,
        role: AccountRole,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::SetAccountRole {
            user_id,
            account_id,
            role,
        })
        .await?;
        Ok(())
    }

    pub async fn grant_tenant_access(
        &self,
        user_id: u64,
        tenant_id: u64,
        catalog_access: CatalogAccess,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::GrantTenantAccess {
            user_id,
            tenant_id,
            catalog_access,
        })
        .await?;
        Ok(())
    }

    pub async fn revoke_tenant_access(
        &self,
        user_id: u64,
        tenant_id: u64,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::RevokeTenantAccess {
            user_id,
            tenant_id,
        })
        .await?;
        Ok(())
    }

    pub async fn create_tenant(
        &self,
        account_id: u64,
        name: String,
        limits: TenantLimits,
    ) -> Result<u64, WriteError> {
        let resp = self
            .propose(MetaCommand::CreateTenant {
                account_id,
                name,
                limits,
            })
            .await?;
        match resp {
            MetaResponse::TenantCreated { tenant_id } => Ok(tenant_id),
            other => Err(WriteError::Application(format!("unexpected: {other}"))),
        }
    }

    pub async fn update_tenant_limits(
        &self,
        tenant_id: u64,
        limits: TenantLimits,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::UpdateTenantLimits { tenant_id, limits })
            .await?;
        Ok(())
    }

    pub async fn delete_tenant(&self, tenant_id: u64) -> Result<(), WriteError> {
        self.propose(MetaCommand::DeleteTenant { tenant_id })
            .await?;
        Ok(())
    }

    // ── Catalog API ────────────────────────────────────────────────────

    pub async fn create_catalog(
        &self,
        tenant_id: u64,
        name: String,
        engine: EngineType,
        config: String,
    ) -> Result<(u64, u64), WriteError> {
        let resp = self
            .propose(MetaCommand::CreateCatalog {
                tenant_id,
                name,
                engine,
                config,
            })
            .await?;
        match resp {
            MetaResponse::CatalogCreated {
                catalog_id,
                raft_group_id,
            } => Ok((catalog_id, raft_group_id)),
            other => Err(WriteError::Application(format!("unexpected: {other}"))),
        }
    }

    pub async fn update_catalog_status(
        &self,
        catalog_id: u64,
        status: CatalogStatus,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::UpdateCatalogStatus { catalog_id, status })
            .await?;
        Ok(())
    }

    pub async fn update_catalog_placement(
        &self,
        catalog_id: u64,
        placement: Vec<u64>,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::UpdateCatalogPlacement {
            catalog_id,
            placement,
        })
        .await?;
        Ok(())
    }

    pub async fn delete_catalog(
        &self,
        tenant_id: u64,
        catalog_id: u64,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::DeleteCatalog {
            tenant_id,
            catalog_id,
        })
        .await?;
        Ok(())
    }

    // ── Routing API (write path) ───────────────────────────────────────

    pub async fn update_leader(
        &self,
        raft_group_id: u64,
        leader_node_id: Option<u64>,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::UpdateLeader {
            raft_group_id,
            leader_node_id,
        })
        .await?;
        Ok(())
    }

    // ── Security API ───────────────────────────────────────────────────

    pub async fn create_api_key(
        &self,
        tenant_id: u64,
        scopes: Vec<Scope>,
    ) -> Result<(u64, String), WriteError> {
        let resp = self
            .propose(MetaCommand::CreateApiKey { tenant_id, scopes })
            .await?;
        match resp {
            MetaResponse::ApiKeyCreated { key_id, raw_key } => Ok((key_id, raw_key)),
            other => Err(WriteError::Application(format!("unexpected: {other}"))),
        }
    }

    pub async fn revoke_api_key(&self, key_id: u64) -> Result<(), WriteError> {
        self.propose(MetaCommand::RevokeApiKey { key_id }).await?;
        Ok(())
    }

    // ── Usage API ──────────────────────────────────────────────────────

    pub async fn report_usage(
        &self,
        catalog_id: u64,
        disk_bytes: u64,
        deep_storage_bytes: u64,
    ) -> Result<(), WriteError> {
        self.propose(MetaCommand::ReportUsage {
            catalog_id,
            disk_bytes,
            deep_storage_bytes,
        })
        .await?;
        Ok(())
    }

    // ── Read-only queries (directly from engine, no Raft) ──────────────

    pub fn get_account(&self, account_id: u64) -> Option<Account> {
        self.engine.get_account(account_id)
    }

    pub fn list_accounts(&self) -> Vec<Account> {
        self.engine.list_accounts()
    }

    pub fn get_user(&self, user_id: u64) -> Option<User> {
        self.engine.get_user(user_id)
    }

    pub fn find_user_by_username(&self, username: &str) -> Option<User> {
        self.engine.find_user_by_username(username)
    }

    pub fn get_user_memberships(&self, user_id: u64) -> Vec<AccountMembership> {
        self.engine.get_user_memberships(user_id)
    }

    pub fn get_account_members(&self, account_id: u64) -> Vec<(User, AccountRole)> {
        self.engine.get_account_members(account_id)
    }

    pub fn get_user_tenant_grants(&self, user_id: u64) -> Vec<TenantGrant> {
        self.engine.get_user_tenant_grants(user_id)
    }

    pub fn get_user_accessible_tenants(&self, user_id: u64) -> Vec<Tenant> {
        self.engine.get_user_accessible_tenants(user_id)
    }

    pub fn get_tenant(&self, tenant_id: u64) -> Option<Tenant> {
        self.engine.get_tenant(tenant_id)
    }

    pub fn list_tenants(&self) -> Vec<Tenant> {
        self.engine.list_tenants()
    }

    pub fn get_catalog(&self, catalog_id: u64) -> Option<CatalogEntry> {
        self.engine.get_catalog(catalog_id)
    }

    pub fn list_catalogs_for_tenant(&self, tenant_id: u64) -> Vec<CatalogEntry> {
        self.engine.list_catalogs_for_tenant(tenant_id)
    }

    pub fn find_catalog_by_name(&self, tenant_id: u64, name: &str) -> Option<CatalogEntry> {
        self.engine.find_catalog_by_name(tenant_id, name)
    }

    pub fn get_routing_table(&self) -> Vec<RoutingEntry> {
        self.engine.get_routing_table()
    }

    pub fn get_routing(&self, raft_group_id: u64) -> Option<RoutingEntry> {
        self.engine.get_routing(raft_group_id)
    }

    // ── Node info ──────────────────────────────────────────────────────

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn group_id(&self) -> u64 {
        self.group_id
    }

    pub fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics.current_leader == Some(self.node_id)
    }

    pub fn current_leader(&self) -> Option<u64> {
        self.raft.metrics().borrow_watched().current_leader
    }

    pub fn raft(&self) -> &Raft<MetaTypeConfig> {
        &self.raft
    }

    pub fn engine(&self) -> &Arc<MetaEngine> {
        &self.engine
    }

    // ── Lifecycle ──────────────────────────────────────────────────────

    pub async fn shutdown(&self) {
        if let Err(e) = self.raft.shutdown().await {
            tracing::warn!(error = %e, "error shutting down meta raft node");
        }
    }
}

/// Errors from MetaRaftNode write operations.
#[derive(Debug, Clone, thiserror::Error)]
pub enum WriteError {
    #[error("not leader (leader: {leader_id:?})")]
    NotLeader {
        leader_id: Option<u64>,
        leader_node: Option<BasicNode>,
    },

    #[error("raft error: {0}")]
    Raft(String),

    #[error("fatal raft error: {0}")]
    Fatal(String),

    #[error("application error: {0}")]
    Application(String),
}
