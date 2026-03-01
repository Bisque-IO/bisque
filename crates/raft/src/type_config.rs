use openraft::AppData;
use openraft::AppDataResponse;
use openraft::RaftTypeConfig;
use openraft::TokioRuntime;
use openraft::impls::BasicNode;
use openraft::impls::Entry;
use openraft::impls::OneshotResponder;
use openraft::impls::Vote;
use openraft::impls::leader_id_adv::LeaderId;
use std::io::Cursor;

pub struct BisqueRaftTypeConfig<D, R> {
    _d: std::marker::PhantomData<D>,
    _r: std::marker::PhantomData<R>,
}

impl<D, R> Clone for BisqueRaftTypeConfig<D, R> {
    fn clone(&self) -> Self {
        Self {
            _d: std::marker::PhantomData,
            _r: std::marker::PhantomData,
        }
    }
}

impl<D, R> Copy for BisqueRaftTypeConfig<D, R> {}

impl<D, R> Default for BisqueRaftTypeConfig<D, R> {
    fn default() -> Self {
        Self {
            _d: std::marker::PhantomData,
            _r: std::marker::PhantomData,
        }
    }
}

impl<D, R> PartialEq for BisqueRaftTypeConfig<D, R> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl<D, R> Eq for BisqueRaftTypeConfig<D, R> {}

impl<D, R> PartialOrd for BisqueRaftTypeConfig<D, R> {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        Some(std::cmp::Ordering::Equal)
    }
}

impl<D, R> Ord for BisqueRaftTypeConfig<D, R> {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

impl<D, R> std::fmt::Debug for BisqueRaftTypeConfig<D, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BisqueRaftTypeConfig").finish()
    }
}

impl<D, R> RaftTypeConfig for BisqueRaftTypeConfig<D, R>
where
    D: AppData,
    R: AppDataResponse,
{
    type D = D;
    type R = R;
    type NodeId = u64;
    type Node = BasicNode;
    type Term = u64;
    type LeaderId = LeaderId<Self>;
    type Vote = Vote<Self>;
    type Entry = Entry<Self>;
    type SnapshotData = Cursor<Vec<u8>>;
    type Responder<T: openraft::OptionalSend + 'static> = OneshotResponder<Self, T>;
    type AsyncRuntime = TokioRuntime;
    type ErrorSource = openraft::AnyError;
}
