/// Dependencies that are needed for the BAM (Jito Scheduler Service) to function.
/// All-in-one for convenience.
use jito_protos::proto::{
    bam_api::{scheduler_message::VersionedMsg, SchedulerMessage, SchedulerMessageV0},
    bam_types,
};

pub enum BamOutboundMessage {
    AtomicTxnBatchResult(bam_types::AtomicTxnBatchResult),
    Heartbeat(bam_types::ValidatorHeartBeat),
    LeaderState(bam_types::LeaderState),
}

pub fn v0_to_versioned_proto(v0: SchedulerMessageV0) -> SchedulerMessage {
    SchedulerMessage {
        versioned_msg: Some(VersionedMsg::V0(v0)),
    }
}
