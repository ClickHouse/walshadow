//! First fatal error across pipeline stages

use std::sync::Arc;

use tokio::sync::watch;

/// One-shot fatal-error signal shared across pipeline stages. Pump polls
/// [`Fatal::message`] to exit with the root cause; the DDL barrier `select`s
/// on [`Fatal::wait`] so a CH outage mid-barrier surfaces instead of hanging.
#[derive(Clone)]
pub struct Fatal(Arc<watch::Sender<Option<String>>>);

impl Default for Fatal {
    fn default() -> Self {
        Self(Arc::new(watch::channel(None).0))
    }
}

impl Fatal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the first message (root cause) and wake every waiter; later
    /// calls keep the first.
    pub fn set(&self, msg: String) {
        self.0.send_if_modified(|slot| {
            if slot.is_some() {
                return false;
            }
            tracing::error!(target: "walshadow::pipeline", error = %msg, "pipeline fatal");
            *slot = Some(msg);
            true
        });
    }

    pub fn is_set(&self) -> bool {
        self.0.borrow().is_some()
    }

    pub fn message(&self) -> Option<String> {
        self.0.borrow().clone()
    }

    /// Root cause, once set (now or later). Holding the sender keeps the
    /// channel open, so this never resolves unset.
    pub async fn wait(&self) -> String {
        let mut rx = self.0.subscribe();
        let set = rx.wait_for(Option::is_some).await;
        set.ok().and_then(|m| m.clone()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fatal_keeps_first_message() {
        let fatal = Fatal::new();
        fatal.set("root".into());
        fatal.set("echo".into());
        assert_eq!(fatal.message().as_deref(), Some("root"));
    }
}
