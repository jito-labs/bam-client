/// Dependencies that are needed for the BAM (Jito Scheduler Service) to function.
/// All-in-one for convenience.
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use {
    crate::proxy::block_engine_stage::BlockBuilderFeeInfo,
    jito_protos::proto::{
        bam_api::{
            start_scheduler_message::VersionedMsg,
            StartSchedulerMessage, StartSchedulerMessageV0,
        },
        bam_types::AtomicTxnBatch,
    },
    solana_gossip::cluster_info::ClusterInfo,
};

#[derive(Clone)]
pub struct BamDependencies {
    pub bam_enabled: Arc<AtomicBool>,

    pub batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
    pub batch_receiver: crossbeam_channel::Receiver<AtomicTxnBatch>,

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
