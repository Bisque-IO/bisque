//! Integration tests for bisque-meta.
//!
//! Tests exercise the full stack: MetaStateMachine dispatching MetaCommands
//! to the MetaEngine, including snapshot/restore cycles, token issuance, and
//! multi-tenant isolation.

use std::sync::Arc;

use bisque_meta::{
    CatalogStatus, EngineType, MetaCommand, MetaConfig, MetaEngine, MetaResponse, MetaSnapshotData,
    MetaStateMachine, Scope, TenantLimits, TokenManager,
};

const TEST_ACCOUNT: u64 = 1;

fn new_engine() -> Arc<MetaEngine> {
    let engine = Arc::new(MetaEngine::new(MetaConfig::new(
        b"integration-test-secret-key".to_vec(),
    )));
    engine.create_account("test-org".into()).unwrap();
    engine
}

fn new_sm(engine: &Arc<MetaEngine>) -> MetaStateMachine {
    MetaStateMachine::new(engine.clone())
}

// ── Full tenant lifecycle ────────────────────────────────────────────

#[test]
fn test_full_tenant_lifecycle() {
    let engine = new_engine();
    let sm = new_sm(&engine);

    // 1. Create tenant
    let MetaResponse::TenantCreated { tenant_id } = sm.apply_command(MetaCommand::CreateTenant {
        account_id: TEST_ACCOUNT,
        name: "acme-corp".into(),
        limits: TenantLimits::default(),
    }) else {
        panic!("expected TenantCreated");
    };

    // 2. No auto-created catalogs (OTel is per-node, not per-tenant)
    let catalogs = engine.list_catalogs_for_tenant(tenant_id);
    assert_eq!(catalogs.len(), 0);

    // 3. Create catalogs
    let MetaResponse::CatalogCreated {
        catalog_id: analytics_id,
        raft_group_id: analytics_group,
    } = sm.apply_command(MetaCommand::CreateCatalog {
        tenant_id,
        name: "analytics".into(),
        engine: EngineType::Lance,
        config: r#"{"compression":"zstd"}"#.into(),
    })
    else {
        panic!("expected CatalogCreated");
    };

    let MetaResponse::CatalogCreated {
        catalog_id: events_id,
        ..
    } = sm.apply_command(MetaCommand::CreateCatalog {
        tenant_id,
        name: "events".into(),
        engine: EngineType::Mq,
        config: "{}".into(),
    })
    else {
        panic!("expected CatalogCreated");
    };

    let MetaResponse::CatalogCreated {
        catalog_id: app_db_id,
        ..
    } = sm.apply_command(MetaCommand::CreateCatalog {
        tenant_id,
        name: "app_db".into(),
        engine: EngineType::LibSql,
        config: "{}".into(),
    })
    else {
        panic!("expected CatalogCreated");
    };

    // 3 user catalogs
    assert_eq!(engine.list_catalogs_for_tenant(tenant_id).len(), 3);

    // 4. Activate catalogs
    for cid in [analytics_id, events_id, app_db_id] {
        sm.apply_command(MetaCommand::UpdateCatalogStatus {
            catalog_id: cid,
            status: CatalogStatus::Active,
        });
    }

    // 5. Set placement
    sm.apply_command(MetaCommand::UpdateCatalogPlacement {
        catalog_id: analytics_id,
        placement: vec![1, 2, 3],
    });

    let cat = engine.get_catalog(analytics_id).unwrap();
    assert_eq!(cat.placement, vec![1, 2, 3]);
    assert_eq!(cat.status, CatalogStatus::Active);

    // 6. Set leader
    sm.apply_command(MetaCommand::UpdateLeader {
        raft_group_id: analytics_group,
        leader_node_id: Some(2),
    });

    let routing = engine.get_routing(analytics_group).unwrap();
    assert_eq!(routing.leader_node_id, Some(2));
    assert_eq!(routing.node_addresses.len(), 3);

    // 7. Create API key
    let MetaResponse::ApiKeyCreated { key_id, raw_key } =
        sm.apply_command(MetaCommand::CreateApiKey {
            tenant_id,
            scopes: vec![Scope::TenantAdmin, Scope::Catalog("analytics".into())],
        })
    else {
        panic!("expected ApiKeyCreated");
    };
    assert!(!raw_key.is_empty());

    // Key is referenced by tenant
    let tenant = engine.get_tenant(tenant_id).unwrap();
    assert!(tenant.api_keys.contains(&key_id));

    // 8. Report usage
    sm.apply_command(MetaCommand::ReportUsage {
        catalog_id: analytics_id,
        disk_bytes: 10_000_000,
        deep_storage_bytes: 50_000_000,
    });
    sm.apply_command(MetaCommand::ReportUsage {
        catalog_id: events_id,
        disk_bytes: 5_000_000,
        deep_storage_bytes: 20_000_000,
    });

    let (total_disk, total_deep) = engine.get_tenant_usage(tenant_id);
    assert_eq!(total_disk, 15_000_000);
    assert_eq!(total_deep, 70_000_000);

    // 9. Revoke API key
    sm.apply_command(MetaCommand::RevokeApiKey { key_id });
    assert!(engine.get_api_key(key_id).unwrap().revoked);

    // 10. Delete catalogs (mark as deleting then deleted)
    for cid in [analytics_id, events_id, app_db_id] {
        sm.apply_command(MetaCommand::DeleteCatalog {
            tenant_id,
            catalog_id: cid,
        });
    }

    // Mark remaining catalogs as Deleted
    let all_catalogs = engine.list_catalogs_for_tenant(tenant_id);
    // list_catalogs_for_tenant excludes Deleted but includes Deleting
    for c in &all_catalogs {
        sm.apply_command(MetaCommand::UpdateCatalogStatus {
            catalog_id: c.catalog_id,
            status: CatalogStatus::Deleted,
        });
    }

    // 11. Delete tenant
    let resp = sm.apply_command(MetaCommand::DeleteTenant { tenant_id });
    assert!(matches!(resp, MetaResponse::Ok));
    assert!(engine.get_tenant(tenant_id).is_none());
}

// ── Multi-tenant isolation ───────────────────────────────────────────

#[test]
fn test_multi_tenant_isolation() {
    let engine = new_engine();
    let sm = new_sm(&engine);

    // Create 3 tenants
    let mut tenant_ids = Vec::new();
    for name in ["alpha", "beta", "gamma"] {
        let MetaResponse::TenantCreated { tenant_id } =
            sm.apply_command(MetaCommand::CreateTenant {
                account_id: TEST_ACCOUNT,
                name: name.into(),
                limits: TenantLimits::default(),
            })
        else {
            panic!("expected TenantCreated");
        };
        tenant_ids.push(tenant_id);
    }

    // Each tenant creates a catalog with the same name
    let mut catalog_ids = Vec::new();
    for &tid in &tenant_ids {
        let MetaResponse::CatalogCreated { catalog_id, .. } =
            sm.apply_command(MetaCommand::CreateCatalog {
                tenant_id: tid,
                name: "logs".into(),
                engine: EngineType::Lance,
                config: "{}".into(),
            })
        else {
            panic!("expected CatalogCreated");
        };
        catalog_ids.push(catalog_id);
    }

    // All catalog_ids should be unique
    let unique: std::collections::HashSet<u64> = catalog_ids.iter().copied().collect();
    assert_eq!(unique.len(), 3);

    // Each tenant sees only their own catalog (logs)
    for &tid in &tenant_ids {
        let cats = engine.list_catalogs_for_tenant(tid);
        assert_eq!(cats.len(), 1);
    }

    // Usage is isolated
    for (i, &cid) in catalog_ids.iter().enumerate() {
        sm.apply_command(MetaCommand::ReportUsage {
            catalog_id: cid,
            disk_bytes: (i as u64 + 1) * 1000,
            deep_storage_bytes: (i as u64 + 1) * 2000,
        });
    }

    for (i, &tid) in tenant_ids.iter().enumerate() {
        let (disk, deep) = engine.get_tenant_usage(tid);
        assert_eq!(disk, (i as u64 + 1) * 1000);
        assert_eq!(deep, (i as u64 + 1) * 2000);
    }

    // API keys for different tenants produce different raw keys
    let mut raw_keys = Vec::new();
    for &tid in &tenant_ids {
        let MetaResponse::ApiKeyCreated { raw_key, .. } =
            sm.apply_command(MetaCommand::CreateApiKey {
                tenant_id: tid,
                scopes: vec![Scope::TenantAdmin],
            })
        else {
            panic!("expected ApiKeyCreated");
        };
        raw_keys.push(raw_key);
    }

    let unique_keys: std::collections::HashSet<&String> = raw_keys.iter().collect();
    assert_eq!(unique_keys.len(), 3);
}

// ── Snapshot and restore preserves everything ────────────────────────

#[test]
fn test_snapshot_restore_preserves_full_state() {
    let engine = new_engine();
    let sm = new_sm(&engine);

    // Build up state
    sm.apply_command(MetaCommand::CreateTenant {
        account_id: TEST_ACCOUNT,
        name: "acme".into(),
        limits: TenantLimits {
            max_catalogs: 10,
            ..TenantLimits::default()
        },
    });

    sm.apply_command(MetaCommand::CreateCatalog {
        tenant_id: 1,
        name: "analytics".into(),
        engine: EngineType::Lance,
        config: r#"{"key":"val"}"#.into(),
    });

    let analytics = engine.find_catalog_by_name(1, "analytics").unwrap();

    sm.apply_command(MetaCommand::UpdateCatalogStatus {
        catalog_id: analytics.catalog_id,
        status: CatalogStatus::Active,
    });

    sm.apply_command(MetaCommand::UpdateCatalogPlacement {
        catalog_id: analytics.catalog_id,
        placement: vec![1, 2, 3],
    });

    sm.apply_command(MetaCommand::UpdateLeader {
        raft_group_id: analytics.raft_group_id,
        leader_node_id: Some(2),
    });

    sm.apply_command(MetaCommand::CreateApiKey {
        tenant_id: 1,
        scopes: vec![Scope::TenantAdmin],
    });

    sm.apply_command(MetaCommand::ReportUsage {
        catalog_id: analytics.catalog_id,
        disk_bytes: 999,
        deep_storage_bytes: 1999,
    });

    // Snapshot
    let snap = engine.snapshot_data();
    let bytes = bincode::serde::encode_to_vec(&snap, bincode::config::standard()).unwrap();

    // Restore into fresh engine
    let engine2 = Arc::new(MetaEngine::new(MetaConfig::new(
        b"integration-test-secret-key".to_vec(),
    )));
    let (data, _): (MetaSnapshotData, _) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
    engine2.restore_from_snapshot(data);

    // Verify everything
    let tenant = engine2.get_tenant(1).unwrap();
    assert_eq!(tenant.name, "acme");
    assert_eq!(tenant.limits.max_catalogs, 10);
    assert!(!tenant.api_keys.is_empty());

    let cat = engine2.get_catalog(analytics.catalog_id).unwrap();
    assert_eq!(cat.status, CatalogStatus::Active);
    assert_eq!(cat.placement, vec![1, 2, 3]);
    assert_eq!(cat.config, r#"{"key":"val"}"#);

    let routing = engine2.get_routing(analytics.raft_group_id).unwrap();
    assert_eq!(routing.leader_node_id, Some(2));
    assert_eq!(routing.node_addresses.len(), 3);

    let usage = engine2.get_usage(analytics.catalog_id).unwrap();
    assert_eq!(usage.disk_bytes, 999);
    assert_eq!(usage.deep_storage_bytes, 1999);

    // New IDs should continue from restored counters
    let t2 = engine2
        .create_tenant(TEST_ACCOUNT, "beta".into(), TenantLimits::default())
        .unwrap();
    assert!(t2 > 1);
}

// ── Token integration with engine ────────────────────────────────────

#[test]
fn test_token_integration_with_engine() {
    let secret = b"shared-secret-for-tokens".to_vec();
    let engine = Arc::new(MetaEngine::new(MetaConfig::new(secret.clone())));
    engine.create_account("test-org".into()).unwrap();
    let sm = new_sm(&engine);
    let token_mgr = TokenManager::new(secret);

    // Create tenant + API key
    sm.apply_command(MetaCommand::CreateTenant {
        account_id: TEST_ACCOUNT,
        name: "acme".into(),
        limits: TenantLimits::default(),
    });

    let MetaResponse::ApiKeyCreated { key_id, .. } = sm.apply_command(MetaCommand::CreateApiKey {
        tenant_id: 1,
        scopes: vec![Scope::Catalog("analytics".into())],
    }) else {
        panic!("expected ApiKeyCreated");
    };

    // Issue token for this key
    let claims = bisque_meta::token::TokenClaims {
        user_id: None,
        account_id: None,
        tenant_id: 1,
        key_id,
        scopes: vec![Scope::Catalog("analytics".into())],
        issued_at: chrono::Utc::now().timestamp(),
        expires_at: chrono::Utc::now().timestamp() + 3600,
    };
    let token = token_mgr.issue(&claims);

    // Verify token
    let verified = token_mgr.verify(&token).unwrap();
    assert_eq!(verified.tenant_id, 1);
    assert_eq!(verified.key_id, key_id);
    assert_eq!(verified.scopes, vec![Scope::Catalog("analytics".into())]);

    // The key_id from the token maps back to the engine's API key
    let api_key = engine.get_api_key(verified.key_id).unwrap();
    assert_eq!(api_key.tenant_id, verified.tenant_id);
    assert!(!api_key.revoked);

    // After revoking, token still verifies cryptographically but key is revoked
    sm.apply_command(MetaCommand::RevokeApiKey { key_id });
    let still_valid_token = token_mgr.verify(&token).unwrap();
    assert_eq!(still_valid_token.key_id, key_id);
    let revoked_key = engine.get_api_key(key_id).unwrap();
    assert!(revoked_key.revoked);
}

// ── Catalog limit enforcement ────────────────────────────────────────

#[test]
fn test_catalog_limit_enforcement_through_state_machine() {
    let engine = new_engine();
    let sm = new_sm(&engine);

    // Create tenant with very low catalog limit
    sm.apply_command(MetaCommand::CreateTenant {
        account_id: TEST_ACCOUNT,
        name: "limited".into(),
        limits: TenantLimits {
            max_catalogs: 1,
            ..TenantLimits::default()
        },
    });

    // First catalog should succeed
    let resp = sm.apply_command(MetaCommand::CreateCatalog {
        tenant_id: 1,
        name: "first".into(),
        engine: EngineType::Lance,
        config: "{}".into(),
    });
    assert!(matches!(resp, MetaResponse::CatalogCreated { .. }));

    // Second should fail (limit is 1: first = 1)
    let resp = sm.apply_command(MetaCommand::CreateCatalog {
        tenant_id: 1,
        name: "second".into(),
        engine: EngineType::Lance,
        config: "{}".into(),
    });
    assert!(matches!(resp, MetaResponse::Error(_)));
}

// ── Routing table ────────────────────────────────────────────────────

#[test]
fn test_routing_table_across_tenants() {
    let engine = new_engine();
    let sm = new_sm(&engine);

    // Two tenants, each with catalogs
    for name in ["t1", "t2"] {
        sm.apply_command(MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: name.into(),
            limits: TenantLimits::default(),
        });
    }

    sm.apply_command(MetaCommand::CreateCatalog {
        tenant_id: 1,
        name: "a".into(),
        engine: EngineType::Lance,
        config: "{}".into(),
    });
    sm.apply_command(MetaCommand::CreateCatalog {
        tenant_id: 2,
        name: "b".into(),
        engine: EngineType::Mq,
        config: "{}".into(),
    });

    // Routing table should have entries for all catalogs:
    // t1: a = 1
    // t2: b = 1
    // Total: 2 routing entries
    let table = engine.get_routing_table();
    assert_eq!(table.len(), 2);

    // Each routing entry should have a unique raft_group_id
    let group_ids: std::collections::HashSet<u64> = table.iter().map(|r| r.raft_group_id).collect();
    assert_eq!(group_ids.len(), 2);
}

// ── Deterministic replay ─────────────────────────────────────────────

#[test]
fn test_deterministic_replay() {
    // Apply the same sequence of commands to two independent engines
    // and verify they produce identical state.
    let commands = vec![
        MetaCommand::CreateTenant {
            account_id: TEST_ACCOUNT,
            name: "acme".into(),
            limits: TenantLimits::default(),
        },
        MetaCommand::CreateCatalog {
            tenant_id: 1,
            name: "analytics".into(),
            engine: EngineType::Lance,
            config: "{}".into(),
        },
        MetaCommand::UpdateCatalogStatus {
            catalog_id: 1, // analytics
            status: CatalogStatus::Active,
        },
        MetaCommand::CreateApiKey {
            tenant_id: 1,
            scopes: vec![Scope::TenantAdmin],
        },
        MetaCommand::ReportUsage {
            catalog_id: 1,
            disk_bytes: 1234,
            deep_storage_bytes: 5678,
        },
    ];

    let engine1 = Arc::new(MetaEngine::new(MetaConfig::new(
        b"integration-test-secret-key".to_vec(),
    )));
    engine1.create_account("test-org".into()).unwrap();
    let sm1 = MetaStateMachine::new(engine1.clone());

    let engine2 = Arc::new(MetaEngine::new(MetaConfig::new(
        b"integration-test-secret-key".to_vec(),
    )));
    engine2.create_account("test-org".into()).unwrap();
    let sm2 = MetaStateMachine::new(engine2.clone());

    // Apply same commands to both
    for cmd in &commands {
        let r1 = sm1.apply_command(cmd.clone());
        let r2 = sm2.apply_command(cmd.clone());
        assert_eq!(format!("{r1}"), format!("{r2}"));
    }

    // Compare snapshots
    let snap1 = engine1.snapshot_data();
    let snap2 = engine2.snapshot_data();

    assert_eq!(snap1.tenants.len(), snap2.tenants.len());
    assert_eq!(snap1.catalogs.len(), snap2.catalogs.len());
    assert_eq!(snap1.api_keys.len(), snap2.api_keys.len());
    assert_eq!(snap1.routing.len(), snap2.routing.len());
    assert_eq!(snap1.usage.len(), snap2.usage.len());
    assert_eq!(snap1.next_tenant_id, snap2.next_tenant_id);
    assert_eq!(snap1.next_catalog_id, snap2.next_catalog_id);
    assert_eq!(snap1.next_key_id, snap2.next_key_id);
    assert_eq!(snap1.next_raft_group_id, snap2.next_raft_group_id);

    // API keys should be identical (deterministic derivation)
    for (kid, key1) in &snap1.api_keys {
        let key2 = snap2.api_keys.get(kid).unwrap();
        assert_eq!(key1.key_hash, key2.key_hash);
        assert_eq!(key1.tenant_id, key2.tenant_id);
    }
}
