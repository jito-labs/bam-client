use {
    crate::config::ClusterInfo,
    axum::{extract::State, http::StatusCode, response::Json, routing::get, Router},
    log::info,
    std::sync::Arc,
    tokio::sync::Notify,
};

pub struct AppState {
    pub cluster_info: ClusterInfo,
    pub shutdown_notify: Arc<Notify>,
}

pub async fn get_cluster_info(State(state): State<Arc<AppState>>) -> Json<ClusterInfo> {
    Json(state.cluster_info.clone())
}

pub async fn exit_handler(State(state): State<Arc<AppState>>) -> StatusCode {
    info!("Received exit request via HTTP");
    state.shutdown_notify.notify_one();
    StatusCode::OK
}

pub fn create_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/cluster-info", get(get_cluster_info))
        .route("/exit", get(exit_handler))
        .with_state(state)
}
