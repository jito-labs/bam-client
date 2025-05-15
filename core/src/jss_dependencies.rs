use std::sync::{Arc, Mutex};

use jito_protos::proto::{jss_api::BuilderConfigResp, jss_types::Bundle};


#[derive(Debug, Clone)]
pub struct JssDependencies {
    pub bundle_sender: crossbeam_channel::Sender<Bundle>,
    pub bundle_receiver: crossbeam_channel::Receiver<Bundle>,

    // TODO: outbound sender/receiver

    pub builder_config: Arc<Mutex<Option<BuilderConfigResp>>>,
}