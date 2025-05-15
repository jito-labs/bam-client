use std::sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex, RwLock};

use solana_poh::poh_recorder::PohRecorder;

use crate::{jss_connection::JssConnection, jss_dependencies::JssDependencies};

pub struct JssManager {
    thread: std::thread::JoinHandle<()>,
}

impl JssManager {
    pub fn new(
        exit: Arc<AtomicBool>,
        jss_url: Arc<Mutex<Option<String>>>,
        dependencies: JssDependencies,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) -> Self {
        Self {
            thread: std::thread::spawn(move || {
                Self::run(
                    exit,
                    jss_url,
                    dependencies,
                    poh_recorder,
                )
            }),
        }
    }

    fn run(
        exit: Arc<AtomicBool>,
        jss_url: Arc<Mutex<Option<String>>>,
        dependencies: JssDependencies,
        poh_recorder: Arc<RwLock<PohRecorder>>,
    ) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        let mut current_connection = None;
        while !exit.load(Ordering::Relaxed) {
            // Update if jss is enabled and sleep for a while before checking again
            dependencies.jss_enabled.store(
                current_connection.is_some(),
                Ordering::Relaxed,
            );

            // If no connection then try to create a new one
            if current_connection.is_none() {
                let url = jss_url.lock().unwrap().clone();
                if let Some(url) = url {
                    let result = runtime.block_on(JssConnection::try_init(
                        url,
                        poh_recorder.clone(),
                        dependencies.cluster_info.clone(),
                        dependencies.builder_config.clone(),
                        dependencies.bundle_sender.clone(),
                        dependencies.outbound_receiver.clone()));
                    match result {
                        Ok(connection) => {
                            current_connection = Some(connection);
                            info!("JSS connection established");
                        }
                        Err(e) => {
                            error!("Failed to connect to JSS: {}", e);
                            std::thread::sleep(std::time::Duration::from_secs(5));
                        }
                    }
                }
            }

            let Some(connection) = current_connection.as_mut() else {
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            };

            // Check if connection is healthy; if no then disconnect
            if !connection.is_healthy() {
                current_connection = None;
                info!("JSS connection lost");
                continue;
            }

            // Check if url changed; if yes then disconnect
            let url = jss_url.lock().unwrap().clone();
            if Some(connection.url().to_string()) != url {
                current_connection = None;
                info!("JSS URL changed");
                continue;
            }

            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }
}