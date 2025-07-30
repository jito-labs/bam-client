/// Dependencies that are needed for the BAM (Jito Scheduler Service) to function.
/// All-in-one for convenience.
use std::sync::{atomic::AtomicBool, Arc, Mutex};
use jito_protos::proto::bam_types;

use {
    crate::proxy::block_engine_stage::BlockBuilderFeeInfo,
    jito_protos::proto::{
        bam_api::{
            start_scheduler_message::VersionedMsg, StartSchedulerMessage, StartSchedulerMessageV0,
        },
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_pubkey::Pubkey,
};

pub enum BamOutboundMessage {
    AtomicTxnBatchResult(bam_types::AtomicTxnBatchResult),
    Heartbeat(bam_types::ValidatorHeartBeat),
    LeaderState(bam_types::LeaderState),
}

#[derive(Clone)]
pub struct BamDependencies {
    pub bam_enabled: Arc<AtomicBool>,

    pub batch_sender: crossbeam_channel::Sender<bam_types::AtomicTxnBatch>,
    pub batch_receiver: crossbeam_channel::Receiver<bam_types::AtomicTxnBatch>,

    pub outbound_sender: crossbeam_channel::Sender<BamOutboundMessage>,
    pub outbound_receiver: crossbeam_channel::Receiver<BamOutboundMessage>,

    pub cluster_info: Arc<ClusterInfo>,
    pub block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
    pub bam_node_pubkey: Arc<Mutex<Pubkey>>,
}

pub fn v0_to_versioned_proto(v0: StartSchedulerMessageV0) -> StartSchedulerMessage {
    StartSchedulerMessage {
        versioned_msg: Some(VersionedMsg::V0(v0)),
    }
}
