use {
    axum::{
        extract::State,
        http::StatusCode,
        response::Json,
        routing::get,
        Router,
    },
    log::info,
    std::sync::Arc,
    crate::config::ClusterInfo,
};

pub struct AppState {
    pub cluster_info: ClusterInfo,
}

pub async fn get_cluster_info(State(state): State<Arc<AppState>>) -> Json<ClusterInfo> {
    Json(state.cluster_info.clone())
}

pub async fn exit_handler(State(_state): State<Arc<AppState>>) -> StatusCode {
    info!("Received exit request via HTTP");
    std::process::exit(0);
}

pub fn create_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/cluster-info", get(get_cluster_info))
        .route("/exit", get(exit_handler))
        .with_state(state)
} 