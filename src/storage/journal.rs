use crate::error::{CopyTraderError, Result};
use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{sync::mpsc, task::JoinHandle};

type Write = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
// A 1,000-copy burst emits several journal writes per trade before SQLite
// catches up. Keep a bounded buffer large enough for that burst.
const CAPACITY: usize = 16_384;

pub(super) struct Journal {
    sender: mpsc::Sender<Write>,
    state: Mutex<State>,
}
struct State {
    attempts: HashSet<String>,
    unsupported: HashSet<String>,
    submitted: HashSet<String>,
}
pub(crate) struct JournalWriter(JoinHandle<Result<()>>);
impl Journal {
    pub(super) fn start(
        attempts: Vec<String>,
        unsupported: Vec<String>,
        submitted: Vec<String>,
    ) -> (Arc<Self>, JournalWriter) {
        let (sender, mut receiver) = mpsc::channel::<Write>(CAPACITY);
        let task = tokio::spawn(async move {
            while let Some(write) = receiver.recv().await {
                if let Err(error) = write.await {
                    tracing::error!(%error, "journal persistence failed; stopping writer");
                    return Err(error);
                }
            }
            Ok(())
        });
        (
            Arc::new(Self {
                sender,
                state: Mutex::new(State {
                    attempts: attempts.into_iter().collect(),
                    unsupported: unsupported.into_iter().collect(),
                    submitted: submitted.into_iter().collect(),
                }),
            }),
            JournalWriter(task),
        )
    }
    pub(super) fn enqueue(&self, write: Write) -> Result<()> {
        self.sender
            .try_send(write)
            .map_err(|_| CopyTraderError::Storage("journal queue full or unavailable".into()))
    }
    pub(super) fn claim(&self, signature: &str) -> Result<bool> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CopyTraderError::Storage("journal state poisoned".into()))?;
        if state.unsupported.contains(signature) {
            return Ok(false);
        }
        Ok(state.attempts.insert(signature.to_owned()))
    }
    pub(super) fn submitted(&self, signature: &str) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| CopyTraderError::Storage("journal state poisoned".into()))?
            .submitted
            .insert(signature.to_owned());
        Ok(())
    }
    pub(super) fn unsupported(&self, signature: &str) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| CopyTraderError::Storage("journal state poisoned".into()))?;
        if !state.submitted.contains(signature) {
            state.unsupported.insert(signature.to_owned());
        }
        Ok(())
    }
    pub(super) fn is_unsupported(&self, signature: &str) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| CopyTraderError::Storage("journal state poisoned".into()))?
            .unsupported
            .contains(signature))
    }
}
impl JournalWriter {
    pub(crate) async fn wait(&mut self) -> Result<()> {
        (&mut self.0)
            .await
            .map_err(|error| CopyTraderError::Storage(format!("journal task failed: {error}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn claims_are_shared_and_submitted_sources_remain_supported() {
        let (journal, mut writer) =
            Journal::start(vec!["old".into()], vec!["unsupported".into()], vec![]);
        let other = journal.clone();
        assert!(!journal.claim("old").expect("old"));
        assert!(!journal.claim("unsupported").expect("unsupported"));
        assert!(journal.claim("new").expect("new"));
        assert!(!other.claim("new").expect("duplicate"));
        journal.submitted("new").expect("submitted");
        journal.unsupported("new").expect("classification");
        assert!(!journal.is_unsupported("new").expect("submitted protected"));
        drop(other);
        drop(journal);
        writer.wait().await.expect("drain");
    }

    #[tokio::test]
    async fn persistence_failure_closes_writer_and_is_reported() {
        let (journal, mut writer) = Journal::start(vec![], vec![], vec![]);
        journal
            .enqueue(Box::pin(async {
                Err(CopyTraderError::Storage("fixture failure".into()))
            }))
            .expect("enqueue");
        assert!(writer.wait().await.is_err());
        assert!(journal.enqueue(Box::pin(async { Ok(()) })).is_err());
    }
}
