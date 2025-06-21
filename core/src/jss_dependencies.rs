/// Dependencies that are needed for the JSS (Jito Scheduler Service) to function.
/// All-in-one for convenience.
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use {
    crate::proxy::block_engine_stage::BlockBuilderFeeInfo,
    jito_protos::proto::{
        jss_api::{
            start_scheduler_message::VersionedMsg, start_scheduler_message_v0::Msg,
            StartSchedulerMessage, StartSchedulerMessageV0,
        },
        jss_types::{Bundle, BundleResult},
    },
    solana_gossip::cluster_info::ClusterInfo,
};

#[derive(Clone)]
pub struct JssDependencies {
    pub jss_enabled: Arc<AtomicBool>,

    pub bundle_sender: crossbeam_channel::Sender<Bundle>,
    pub bundle_receiver: crossbeam_channel::Receiver<Bundle>,

    pub outbound_sender: crossbeam_channel::Sender<StartSchedulerMessageV0>,
    pub outbound_receiver: crossbeam_channel::Receiver<StartSchedulerMessageV0>,

    pub cluster_info: Arc<ClusterInfo>,
    pub block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
}

pub fn v0_to_versioned_proto(v0: StartSchedulerMessageV0) -> StartSchedulerMessage {
    StartSchedulerMessage {
        versioned_msg: Some(VersionedMsg::V0(v0)),
    }
}
