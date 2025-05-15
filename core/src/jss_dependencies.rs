use std::sync::{atomic::AtomicBool, Arc, Mutex};

use jito_protos::proto::{jss_api::BuilderConfigResp, jss_types::Bundle};


#[derive(Debug, Clone)]
pub struct JssDependencies {
    pub jss_enabled: Arc<AtomicBool>,

    pub bundle_sender: crossbeam_channel::Sender<Bundle>,
    pub bundle_receiver: crossbeam_channel::Receiver<Bundle>,

    pub outbound_sender: crossbeam_channel::Sender<Bundle>,
    pub outbound_receiver: crossbeam_channel::Receiver<Bundle>,

    pub builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
}