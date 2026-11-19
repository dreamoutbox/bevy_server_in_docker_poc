use std::{future::Future, sync::OnceLock};

use bevy::{prelude::Component, tasks::futures::check_ready};
use tokio::{
    runtime::{Builder, Runtime},
    task::{JoinError, JoinHandle},
};

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Returns the shared multi-threaded Tokio runtime used by Tokio-dependent async libraries.
fn tokio_runtime() -> &'static Runtime {
    TOKIO_RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("bevy-tokio-io")
            .build()
            .expect("failed to build shared Tokio runtime")
    })
}

/// Bevy component wrapping a [`tokio::task::JoinHandle`] so Tokio-dependent async I/O (such as
/// `reqwest`) runs inside a real Tokio reactor while Bevy polls it non-blockingly each frame.
#[derive(Component)]
pub struct TokioTask<T: Send + 'static>(JoinHandle<T>);

impl<T: Send + 'static> TokioTask<T> {
    /// Spawns `future` onto the shared Tokio runtime and wraps its handle in a Bevy component.
    ///
    /// # Arguments
    /// * `future` - Tokio-compatible async computation returning `T`.
    ///
    /// # Example
    /// ```no_run
    /// use bevy_server_in_docker_poc::tokio_task::TokioTask;
    ///
    /// let _task = TokioTask::spawn(async { 1 + 1 });
    /// ```
    pub fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
    {
        Self(tokio_runtime().spawn(future))
    }

    /// Polls the Tokio task once without blocking and returns `Some(Result<T, JoinError>)` when ready.
    ///
    /// # Example
    /// ```no_run
    /// use bevy_server_in_docker_poc::tokio_task::TokioTask;
    ///
    /// fn poll(task: &mut TokioTask<u32>) -> Option<Result<u32, tokio::task::JoinError>> {
    ///     task.try_take()
    /// }
    /// ```
    pub fn try_take(&mut self) -> Option<Result<T, JoinError>> {
        check_ready(&mut self.0)
    }
}
