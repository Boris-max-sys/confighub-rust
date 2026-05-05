// ============================================================
// PARTIE 4 — L'API HTTP REST
// ============================================================
// Implémente une API HTTP avec axum pour la gestion de la
// configuration :
//   GET    /config/{namespace}/{key}    → valeur + version
//   PUT    /config/{namespace}/{key}    → body JSON {"value": "..."}
//   DELETE /config/{namespace}/{key}    → supprime la clé
//   GET    /config/{namespace}          → export du namespace complet
//   GET    /health                      → état du serveur
//   GET    /version                     → version courante du store

use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, put, delete},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

use crate::config_store::{SharedConfigStore, ChangeEvent};
use crate::persistence::ConfigLoader;

pub const HTTP_SERVER_PORT: u16 = 3001;

// --------------------------------------------------------
// État partagé de l'API
// --------------------------------------------------------
#[derive(Clone)]
pub struct AppState {
    pub loader: Arc<ConfigLoader>,
}

// --------------------------------------------------------
// Types de réponse JSON
// --------------------------------------------------------
#[derive(Serialize)]
pub struct ConfigValueResponse {
    pub namespace: String,
    pub key: String,
    pub value: String,
    pub version: u64,
}

#[derive(Serialize)]
pub struct NamespaceResponse {
    pub namespace: String,
    pub values: std::collections::HashMap<String, String>,
    pub version: u64,
}

#[derive(Deserialize)]
pub struct SetValueRequest {
    pub value: String,
}

#[derive(Serialize)]
pub struct SetValueResponse {
    pub namespace: String,
    pub key: String,
    pub value: String,
    pub version: u64,
    pub message: String,
}

#[derive(Serialize)]
pub struct DeleteResponse {
    pub namespace: String,
    pub key: String,
    pub deleted: bool,
    pub message: String,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: u64,
    pub namespaces: Vec<String>,
}

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// --------------------------------------------------------
// Handlers
// --------------------------------------------------------

/// GET /config/{namespace}/{key}
async fn get_config(
    State(state): State<AppState>,
    Path((namespace, key)): Path<(String, String)>,
) -> Result<Json<ConfigValueResponse>, (StatusCode, Json<ErrorResponse>)> {
    let store = state.loader.store.read().await;

    match store.get(&namespace, &key) {
        Some(val) => Ok(Json(ConfigValueResponse {
            namespace,
            key,
            value: val.value.clone(),
            version: val.version,
        })),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Clé '{}.{}' introuvable", namespace, key),
            }),
        )),
    }
}

/// PUT /config/{namespace}/{key}
/// Body JSON : {"value": "nouvelle_valeur"}
async fn set_config(
    State(state): State<AppState>,
    Path((namespace, key)): Path<(String, String)>,
    Json(body): Json<SetValueRequest>,
) -> Result<Json<SetValueResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Validation basique
    if namespace.is_empty() || key.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Namespace et clé ne peuvent pas être vides".to_string(),
            }),
        ));
    }

    let version = state.loader.set(&namespace, &key, body.value.clone()).await;

    info!("SET {}.{} = {} (v{})", namespace, key, body.value, version);

    Ok(Json(SetValueResponse {
        namespace,
        key,
        value: body.value,
        version,
        message: "Valeur mise à jour".to_string(),
    }))
}

/// DELETE /config/{namespace}/{key}
async fn delete_config(
    State(state): State<AppState>,
    Path((namespace, key)): Path<(String, String)>,
) -> Json<DeleteResponse> {
    let deleted_value = state.loader.delete(&namespace, &key).await;
    let deleted = deleted_value.is_some();

    if deleted {
        info!("DELETE {}.{}", namespace, key);
    }

    Json(DeleteResponse {
        namespace,
        key,
        deleted,
        message: if deleted {
            "Clé supprimée".to_string()
        } else {
            "Clé introuvable".to_string()
        },
    })
}

/// GET /config/{namespace}  — export du namespace complet
async fn get_namespace(
    State(state): State<AppState>,
    Path(namespace): Path<String>,
) -> Result<Json<NamespaceResponse>, (StatusCode, Json<ErrorResponse>)> {
    let store = state.loader.store.read().await;

    if !store.namespace_exists(&namespace) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Namespace '{}' introuvable", namespace),
            }),
        ));
    }

    let values = store.get_namespace(&namespace);
    let version = store.current_version();

    Ok(Json(NamespaceResponse {
        namespace,
        values,
        version,
    }))
}

/// GET /health
async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let store = state.loader.store.read().await;
    Json(HealthResponse {
        status: "ok".to_string(),
        version: store.current_version(),
        namespaces: store.namespaces(),
    })
}

/// GET /version
async fn get_version(State(state): State<AppState>) -> Json<serde_json::Value> {
    let store = state.loader.store.read().await;
    Json(serde_json::json!({ "version": store.current_version() }))
}

// --------------------------------------------------------
// Construction du routeur
// --------------------------------------------------------
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/version", get(get_version))
        .route("/config/:namespace", get(get_namespace))
        .route("/config/:namespace/:key", get(get_config))
        .route("/config/:namespace/:key", put(set_config))
        .route("/config/:namespace/:key", delete(delete_config))
        .with_state(state)
}

// --------------------------------------------------------
// Démarrage du serveur HTTP
// --------------------------------------------------------
pub async fn run_http_server(
    store: SharedConfigStore,
    change_tx: broadcast::Sender<ChangeEvent>,
    port: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::path::PathBuf;

    let loader = Arc::new(ConfigLoader::new(
        store,
        PathBuf::from("config_data"),
        change_tx,
    ));

    let state = AppState { loader };
    let router = build_router(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("API HTTP démarrée sur http://{}", addr);

    axum::serve(listener, router).await?;
    Ok(())
}

// ============================================================
// TESTS (Partie 4)
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_store::new_shared_store;
    use axum::http::StatusCode;
    use axum_test::TestServer;

    async fn test_app() -> TestServer {
        let (store, tx) = new_shared_store(16);
        let loader = Arc::new(ConfigLoader::new(
            store,
            std::path::PathBuf::from("/tmp/confighub-test"),
            tx,
        ));
        let state = AppState { loader };
        let router = build_router(state);
        TestServer::new(router).unwrap()
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let server = test_app().await;
        let response = server.get("/health").await;
        response.assert_status(StatusCode::OK);
        let body: serde_json::Value = response.json();
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let server = test_app().await;

        // PUT
        let put_response = server
            .put("/config/db/host")
            .json(&serde_json::json!({ "value": "localhost" }))
            .await;
        put_response.assert_status(StatusCode::OK);

        // GET
        let get_response = server.get("/config/db/host").await;
        get_response.assert_status(StatusCode::OK);
        let body: serde_json::Value = get_response.json();
        assert_eq!(body["value"], "localhost");
    }

    #[tokio::test]
    async fn test_get_missing_key() {
        let server = test_app().await;
        let response = server.get("/config/db/missing").await;
        response.assert_status(StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_key() {
        let server = test_app().await;

        // Crée d'abord
        server
            .put("/config/db/host")
            .json(&serde_json::json!({ "value": "localhost" }))
            .await;

        // Supprime
        let del_response = server.delete("/config/db/host").await;
        del_response.assert_status(StatusCode::OK);
        let body: serde_json::Value = del_response.json();
        assert!(body["deleted"].as_bool().unwrap());

        // Vérifie suppression
        let get_response = server.get("/config/db/host").await;
        get_response.assert_status(StatusCode::NOT_FOUND);
    }
}
