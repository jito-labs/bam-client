use std::sync::{atomic::AtomicBool, Arc, Mutex};

use crate::jss_dependencies::JssDependencies;

pub struct JssManager {
    thread: std::thread::JoinHandle<()>,
}

impl JssManager {
    pub fn new(
        exit: Arc<AtomicBool>,
        jss_url: Arc<Mutex<Option<String>>>,
        dependencies: JssDependencies
    ) -> Self {
        Self {
            thread: std::thread::spawn(move || {
                Self::run(
                    exit,
                    jss_url,
                    dependencies,
                )
            }),
        }
    }

    fn run(
        exit: Arc<AtomicBool>,
        jss_url: Arc<Mutex<Option<String>>>,
        dependencies: JssDependencies,
    ) {
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread.join()
    }
}