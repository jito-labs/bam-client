/// Dependencies that are needed for the JSS (Jito Scheduler Service) to function.
/// All-in-one for convenience.
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use {
    crate::proxy::block_engine_stage::BlockBuilderFeeInfo,
    jito_protos::proto::{jss_api::StartSchedulerMessage, jss_types::Bundle},
    solana_gossip::cluster_info::ClusterInfo,
};

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
