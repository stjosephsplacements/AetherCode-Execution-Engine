//! The Postgres-backed identity store: tenant by org slug, user lookup and auto-provision.

mod common;

use ae_core::Config;
use ae_core::model::DEFAULT_TENANT_ID;
use ae_gateway::auth::{AuthSettings, Authenticator, IdentityStore, JwksOptions};
use common::*;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

#[tokio::test]
async fn db_store_resolves_tenants_and_provisions_users() {
    let c = Postgres::default()
        .with_user("aethercode")
        .with_password("aethercode")
        .with_db_name("aethercode_test")
        .with_tag("18")
        .start()
        .await
        .expect("start postgres:18 (is docker running?)");
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://aethercode:aethercode@127.0.0.1:{port}/aethercode_test");
    let cfg = Config::from_map(&HashMap::from([(
        "AC_DATABASE_URL".to_owned(),
        url.clone(),
    )]))
    .unwrap();
    let db = ae_db::connect(&cfg).await.unwrap();
    db.migrate().await.unwrap();

    let active = Uuid::new_v4();
    sqlx::query("INSERT INTO tenants (id, name, slug) VALUES ($1, 'Org', 'org-active')")
        .bind(active)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO tenants (name, slug, active) VALUES ('Old', 'org-inactive', false)")
        .execute(db.pool())
        .await
        .unwrap();

    let store: Arc<dyn IdentityStore> = Arc::new(db.clone());
    assert_eq!(store.tenant_by_org("org-active").await.unwrap(), active);
    assert!(store.tenant_by_org("org-inactive").await.is_err());
    assert!(store.tenant_by_org("missing").await.is_err());

    let key = rsa_key("test-key");
    let jwks = JwksServer::start(vec![key.jwk.clone()]).await;
    let auth = Authenticator::new(
        AuthSettings {
            issuer: ISSUER.to_owned(),
            audience: AUDIENCE.to_owned(),
            jwks_url: jwks.url.clone(),
        },
        JwksOptions::default(),
        store,
    )
    .await;

    let mut claims = valid_claims(ISSUER, AUDIENCE, "zitadel-user");
    claims.insert("urn:zitadel:iam:org:id".to_owned(), json!("org-active"));
    claims.insert("email".to_owned(), json!("u@x.test"));
    let id = auth.validate(&sign(&key, &claims)).await.unwrap();
    assert_eq!(id.tenant_id, active);
    let (tenant, email, name): (Uuid, String, String) =
        sqlx::query_as("SELECT tenant_id, email, display_name FROM users WHERE id = $1")
            .bind(id.user_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        (tenant, email.as_str(), name.as_str()),
        (active, "u@x.test", "Test User")
    );

    // Org-less token provisions in the default tenant; inactive org is refused.
    let id = auth
        .validate(&sign(&key, &valid_claims(ISSUER, AUDIENCE, "plain")))
        .await
        .unwrap();
    assert_eq!(id.tenant_id, Uuid::parse_str(DEFAULT_TENANT_ID).unwrap());
    let mut claims = valid_claims(ISSUER, AUDIENCE, "plain");
    claims.insert("urn:zitadel:iam:org:id".to_owned(), json!("org-inactive"));
    assert!(auth.validate(&sign(&key, &claims)).await.is_err());

    // A pre-existing user is found, not duplicated.
    let again = auth
        .validate(&sign(&key, &valid_claims(ISSUER, AUDIENCE, "plain")))
        .await
        .unwrap();
    assert_eq!(again.user_id, id.user_id);
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM users")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(n, 2);
}
