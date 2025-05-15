use std::sync::{atomic::AtomicBool, Arc, Mutex};

use jito_protos::proto::{jss_api::StartSchedulerMessage, jss_types::Bundle};
use solana_gossip::cluster_info::ClusterInfo;

use crate::proxy::block_engine_stage::BlockBuilderFeeInfo;

#[derive(Clone)]
pub struct JssDependencies {
    pub jss_enabled: Arc<AtomicBool>,

    pub bundle_sender: crossbeam_channel::Sender<Bundle>,
    pub bundle_receiver: crossbeam_channel::Receiver<Bundle>,

    pub outbound_sender: crossbeam_channel::Sender<StartSchedulerMessage>,
    pub outbound_receiver: crossbeam_channel::Receiver<StartSchedulerMessage>,

    pub cluster_info: Arc<ClusterInfo>,
    pub block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
}
