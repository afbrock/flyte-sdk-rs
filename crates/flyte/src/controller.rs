//! Adapter over the rs_controller crate (`flyte_controller_base`).
//!
//! This is one half of the swap boundary (the other is `idl.rs`): every
//! rs_controller/pyo3 touchpoint lives here, and the API speaks only SDK-owned
//! types. Replacing the pyo3-linked controller with a pure-Rust one later
//! changes this file only.

use std::sync::Arc;
use std::time::Duration;

use flyte_controller_base::action::{Action, ActionType};
use flyte_controller_base::auth::{
    AuthConfig, AuthLayer, AuthService, ClientCredentialsAuthenticator,
};
use flyte_controller_base::core::CoreBaseController;
use flyteidl2::flyteidl::common::{Filter, ListRequest, ProjectIdentifier, Sort, filter, sort};
use flyteidl2::flyteidl::task::task_service_client::TaskServiceClient;
use flyteidl2::flyteidl::task::{
    GetTaskDetailsRequest, GetTaskDetailsResponse, ListTasksRequest, ListTasksResponse,
    TaskIdentifier, list_tasks_request,
};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::Layer as _;

use crate::error::{ConditionOutcome, Error};
use crate::idl::{
    ActionIdentifier, ActionPhase, ConditionAction, ExecutionError, Literal, Message as _,
    RunIdentifier, TaskSpec, execution_error,
};

/// What the SDK needs to know about a previously recorded action.
pub struct RecordedAction {
    pub failed: bool,
    /// Full URI of the recorded `outputs.pb`, when outputs were recorded.
    pub outputs_uri: Option<String>,
}

/// Everything needed to record a finished trace as a child action.
pub struct TraceRecord {
    pub parent_action_name: String,
    pub action_name: String,
    pub friendly_name: String,
    /// From [`crate::context::group`]; `None` outside one.
    pub group: Option<String>,
    pub inputs_uri: String,
    /// Empty string when the trace has no outputs.
    pub outputs_uri: String,
    pub start: f64,
    pub end: f64,
    pub run_output_base: String,
    pub typed_interface_bytes: Vec<u8>,
}

/// Everything needed to register a condition as a child action.
pub struct ConditionRecord {
    pub parent_action_name: String,
    pub action_name: String,
    pub spec: ConditionAction,
    pub group: Option<String>,
    /// Placeholder path: conditions have no inputs and nothing is written here,
    /// but the enqueue path requires a non-empty value.
    pub inputs_uri: String,
    pub run_output_base: String,
}

/// Everything needed to launch a task as a child action.
pub struct TaskRecord {
    pub parent_action_name: String,
    pub action_name: String,
    pub spec: TaskSpec,
    pub group: Option<String>,
    pub inputs_uri: String,
    pub run_output_base: String,
    /// Set when the child's spec is discoverable (cached).
    pub cache_key: Option<String>,
}

/// How a child task action finished.
pub enum TaskResult {
    /// Succeeded or recovered. `None` when the child recorded no outputs.
    Succeeded {
        outputs_uri: Option<String>,
    },
    Failed(TaskFailure),
}

/// A child task action that did not succeed.
pub struct TaskFailure {
    pub phase: Option<ActionPhase>,
    /// True when the backend attributes the failure to user code.
    pub user: bool,
    /// Empty when the backend reported no error document.
    pub code: String,
    pub message: String,
}

/// An in-flight child task: what [`Controller::cancel_task`] needs to stop it.
pub struct StartedTask(Action);

/// Cloning shares the connection, worker pool and informer cache; only the run
/// binding is per-clone. See [`Controller::for_run`].
#[derive(Clone)]
pub struct Controller {
    inner: Arc<CoreBaseController>,
    tasks: TaskClient,
    run_id: RunIdentifier,
}

/// TaskService, for fetching deployed tasks. flyte_core keeps its channel to
/// itself, so this is a second one to the same endpoint, built the same way
/// (TLS, keepalive, the same API-key auth), and connected on first use.
#[derive(Clone)]
enum TaskClient {
    Plain(TaskServiceClient<Channel>),
    Authenticated(TaskServiceClient<AuthService<Channel>>),
}

/// The same ping cadence as flyte_core's channel (vendor/flyte_core/PATCHES.md).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);

fn lazy_channel(url: &str) -> Result<Channel, Error> {
    let err = |e: tonic::transport::Error| Error::Controller(format!("endpoint {url}: {e}"));
    let mut endpoint = Endpoint::from_shared(url.to_string())
        .map_err(err)?
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT);
    if url.starts_with("https://") {
        endpoint = endpoint
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .map_err(err)?;
    }
    Ok(endpoint.connect_lazy())
}

impl TaskClient {
    fn with_auth() -> Result<Self, Error> {
        let api_key = std::env::var("_UNION_EAGER_API_KEY")
            .map_err(|_| Error::Controller("_UNION_EAGER_API_KEY must be set".into()))?;
        let config = AuthConfig::new_from_api_key(&api_key)
            .map_err(|e| Error::Controller(format!("auth config: {e:?}")))?;
        let channel = lazy_channel(&config.endpoint)?;
        let auth = AuthLayer::new(
            Arc::new(ClientCredentialsAuthenticator::new(config)),
            channel.clone(),
        );
        Ok(TaskClient::Authenticated(TaskServiceClient::new(
            auth.layer(channel),
        )))
    }

    /// `Ok(None)` when the backend has no such task.
    async fn get_task_details(
        &self,
        req: GetTaskDetailsRequest,
    ) -> Result<Option<GetTaskDetailsResponse>, Error> {
        let resp = match self.clone() {
            TaskClient::Plain(mut c) => c.get_task_details(req).await,
            TaskClient::Authenticated(mut c) => c.get_task_details(req).await,
        };
        match resp {
            Ok(resp) => Ok(Some(resp.into_inner())),
            Err(s) if s.code() == tonic::Code::NotFound => Ok(None),
            Err(s) => Err(Error::Controller(format!("GetTaskDetails failed: {s}"))),
        }
    }

    async fn list_tasks(&self, req: ListTasksRequest) -> Result<ListTasksResponse, Error> {
        match self.clone() {
            TaskClient::Plain(mut c) => c.list_tasks(req).await,
            TaskClient::Authenticated(mut c) => c.list_tasks(req).await,
        }
        .map(tonic::Response::into_inner)
        .map_err(|s| Error::Controller(format!("ListTasks failed: {s}")))
    }
}

impl Controller {
    /// Auth from `_UNION_EAGER_API_KEY`. Must be called from a non-async thread:
    /// the underlying constructor blocks on the shared runtime.
    pub fn with_auth(run_id: RunIdentifier, workers: usize) -> Result<Self, Error> {
        let inner = CoreBaseController::new_with_auth(workers)
            .map_err(|e| Error::Controller(format!("controller init (auth) failed: {e:?}")))?;
        let tasks = {
            let _rt = runtime().enter();
            TaskClient::with_auth()?
        };
        Ok(Controller {
            inner,
            tasks,
            run_id,
        })
    }

    /// Unauthenticated endpoint (devbox/local). Same non-async-thread constraint.
    pub fn without_auth(
        run_id: RunIdentifier,
        endpoint: String,
        workers: usize,
    ) -> Result<Self, Error> {
        let tasks = {
            let _rt = runtime().enter();
            TaskClient::Plain(TaskServiceClient::new(lazy_channel(&endpoint)?))
        };
        let inner = CoreBaseController::new_without_auth(endpoint, workers)
            .map_err(|e| Error::Controller(format!("controller init failed: {e:?}")))?;
        Ok(Controller {
            inner,
            tasks,
            run_id,
        })
    }

    pub fn run_id(&self) -> &RunIdentifier {
        &self.run_id
    }

    /// A view of this controller bound to a different run, sharing the same
    /// connection, worker pool and informer cache.
    ///
    /// `run_id` is only ever used to build `ActionIdentifier`s and to key the
    /// informer cache -- which `flyte_core` names `"{run_name}.{parent_action}"`
    /// -- so one `CoreBaseController` serves many runs. That is what lets a
    /// reusable container pay for the connection once and then handle whatever
    /// runs the backend assigns it. (Org/project/domain are absent from that key
    /// but fixed for the lifetime of a reusable environment: they are part of
    /// the fasttask queue id, so replicas only ever see one of them.)
    ///
    /// Note that [`Self::watch_for_errors`] belongs to the underlying
    /// controller, not to the view -- call it once, on the controller the pool
    /// built, not per run.
    pub fn for_run(&self, run_id: RunIdentifier) -> Self {
        Controller {
            inner: self.inner.clone(),
            tasks: self.tasks.clone(),
            run_id,
        }
    }

    fn action_id(&self, action_name: &str) -> ActionIdentifier {
        ActionIdentifier {
            run: Some(self.run_id.clone()),
            name: action_name.to_string(),
        }
    }

    /// Informer-cache lookup of a previously recorded child action. Creates the
    /// informer (and its watch stream) on first use for `parent_action_name`.
    pub async fn lookup_action(
        &self,
        action_name: &str,
        parent_action_name: &str,
    ) -> Result<Option<RecordedAction>, Error> {
        let found = self
            .inner
            .get_action(self.action_id(action_name), parent_action_name)
            .await
            .map_err(|e| Error::Controller(format!("get_action failed: {e:?}")))?;
        Ok(found.map(|a| RecordedAction {
            failed: a.phase == Some(ActionPhase::Failed) || a.err.is_some(),
            outputs_uri: a.realized_outputs_uri.filter(|u| !u.is_empty()),
        }))
    }

    /// Record a finished trace as a child action (enqueue + bounded wait for the
    /// server echo; `AlreadyExists` counts as success, so re-recording on retry
    /// is safe).
    pub async fn record_trace(&self, rec: TraceRecord) -> Result<(), Error> {
        let action_id_bytes = self.action_id(&rec.action_name).encode_to_vec();
        let action = Action::from_trace(
            rec.parent_action_name,
            &action_id_bytes,
            rec.friendly_name,
            rec.group,
            rec.inputs_uri,
            rec.outputs_uri,
            rec.start,
            rec.end,
            rec.run_output_base,
            None, // report_uri
            Some(&rec.typed_interface_bytes),
        )
        .map_err(|e| Error::Controller(format!("building trace action failed: {e}")))?;
        self.inner
            .submit_action(action)
            .await
            .map_err(|e| Error::Controller(format!("trace submit failed: {e:?}")))?;
        Ok(())
    }

    /// Register a condition as a child action. Returns as soon as it is
    /// enqueued: the condition exists and is answerable from that point, and
    /// [`Self::wait_condition`] collects the answer later.
    pub async fn start_condition(&self, rec: ConditionRecord) -> Result<(), Error> {
        let action_id_bytes = self.action_id(&rec.action_name).encode_to_vec();
        let action = Action::from_condition(
            rec.parent_action_name,
            &action_id_bytes,
            &rec.spec.encode_to_vec(),
            rec.inputs_uri,
            rec.run_output_base,
            rec.group,
        )
        .map_err(|e| Error::Controller(format!("building condition action failed: {e}")))?;
        self.inner
            .start_action(action)
            .await
            .map_err(|e| Error::Controller(format!("condition submit failed: {e:?}")))
    }

    /// Wait for a condition registered by [`Self::start_condition`] to be
    /// signalled, and return the value it carried.
    ///
    /// `Ok(None)` means it succeeded without a value, which the backend permits.
    /// A terminal-but-unsuccessful phase becomes [`Error::Condition`] rather than
    /// a controller error: it is a real answer, not a transport failure.
    pub async fn wait_condition(
        &self,
        condition_name: &str,
        parent_action_name: &str,
        action_name: &str,
    ) -> Result<Option<Literal>, Error> {
        let action = self
            .inner
            .wait_for_action(
                &self.run_id,
                parent_action_name,
                action_name,
                ActionType::Condition,
            )
            .await
            .map_err(|e| Error::Controller(format!("condition wait failed: {e:?}")))?;

        // `Recovered` counts as success here: the condition was adopted from a
        // prior run and its value is valid.
        if action.is_action_successful() {
            return Ok(action.condition_output);
        }

        let outcome = match action.phase {
            Some(ActionPhase::TimedOut) => ConditionOutcome::TimedOut,
            Some(ActionPhase::Aborted) => ConditionOutcome::Aborted,
            Some(ActionPhase::Failed) => ConditionOutcome::Failed,
            _ => ConditionOutcome::Unknown,
        };
        let message = action
            .err
            .as_ref()
            .map(|e| e.message.clone())
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| format!("phase {:?}", action.phase));
        Err(Error::Condition {
            name: condition_name.to_string(),
            outcome,
            message,
        })
    }

    /// Enqueue a task as a child action without waiting for it. `AlreadyExists`
    /// is adopted rather than failed, so a retry or recovery picks the earlier
    /// action back up.
    pub async fn start_task(&self, rec: TaskRecord) -> Result<StartedTask, Error> {
        let action_id_bytes = self.action_id(&rec.action_name).encode_to_vec();
        let action = Action::from_task(
            &action_id_bytes,
            rec.parent_action_name,
            rec.group,
            &rec.spec.encode_to_vec(),
            rec.inputs_uri,
            rec.run_output_base,
            rec.cache_key,
            None, // queue: Python's `_submit_task_ref` passes none either
        )
        .map_err(|e| Error::Controller(format!("building task action failed: {e}")))?;
        self.inner
            .start_action(action.clone())
            .await
            .map_err(|e| Error::Controller(format!("task submit failed: {e:?}")))?;
        Ok(StartedTask(action))
    }

    /// Wait for a task started by [`Self::start_task`] to reach a terminal phase.
    pub async fn wait_task(
        &self,
        parent_action_name: &str,
        action_name: &str,
    ) -> Result<TaskResult, Error> {
        let action = self
            .inner
            .wait_for_action(
                &self.run_id,
                parent_action_name,
                action_name,
                ActionType::Task,
            )
            .await
            .map_err(|e| Error::Controller(format!("task wait failed: {e:?}")))?;
        if action.is_action_successful() && action.err.is_none() {
            return Ok(TaskResult::Succeeded {
                outputs_uri: action.realized_outputs_uri.filter(|u| !u.is_empty()),
            });
        }
        let (user, code, message) = match (&action.err, &action.client_err) {
            (
                Some(ExecutionError {
                    code,
                    message,
                    kind,
                    ..
                }),
                _,
            ) => (
                *kind == execution_error::ErrorKind::User as i32,
                code.clone(),
                message.clone(),
            ),
            (None, Some(client_err)) => (false, "RustControllerError".into(), client_err.clone()),
            (None, None) => (false, String::new(), format!("phase {:?}", action.phase)),
        };
        Ok(TaskResult::Failed(TaskFailure {
            phase: action.phase,
            user,
            code,
            message,
        }))
    }

    /// Cancel a child task, e.g. because its caller stopped waiting for it.
    ///
    /// Releases the waiter and, through ActionsService (`_U_USE_ACTIONS=1`),
    /// aborts the child server-side as Python's controller does. Over
    /// StateService the child runs on until it finishes or the run ends.
    pub async fn cancel_task(&self, task: StartedTask) -> Result<(), Error> {
        let parent = task.0.parent_action_name.clone();
        // The informer's copy knows whether the child already finished, so
        // prefer it. Neither copy is marked started (only the launching
        // worker's is), and cancel_action aborts only a started action: mark it,
        // since start_task submitted it. If the enqueue is still in flight, the
        // abort finds nothing and the child is launched anyway.
        let mut action = match self
            .inner
            .get_action(task.0.action_id.clone(), &parent)
            .await
        {
            Ok(Some(live)) => live,
            _ => task.0,
        };
        action.mark_started();
        self.inner
            .cancel_action(&mut action)
            .await
            .map_err(|e| Error::Controller(format!("task cancel failed: {e:?}")))
    }

    /// The serialized `TaskSpec` of a deployed task -- Python's
    /// `TaskDetails.get`. `Ok(None)` when no such task (or version) exists.
    pub async fn deployed_task_spec(&self, id: TaskIdentifier) -> Result<Option<Vec<u8>>, Error> {
        let req = GetTaskDetailsRequest { task_id: Some(id) };
        let Some(resp) = self.tasks.get_task_details(req).await? else {
            return Ok(None);
        };
        resp.details
            .and_then(|d| d.spec)
            .map(|spec| Some(spec.encode_to_vec()))
            .ok_or_else(|| Error::Controller("GetTaskDetails returned no spec".into()))
    }

    /// The most recently created version of a deployed task, as Python's
    /// `auto_version="latest"` picks it. `Ok(None)` when it has none.
    pub async fn latest_task_version(
        &self,
        org: &str,
        project: &str,
        domain: &str,
        name: &str,
    ) -> Result<Option<String>, Error> {
        let req = ListTasksRequest {
            request: Some(ListRequest {
                limit: 1,
                sort_by_fields: vec![Sort {
                    key: "created_at".into(),
                    direction: sort::Direction::Descending as i32,
                }],
                filters: vec![Filter {
                    function: filter::Function::Equal as i32,
                    field: "name".into(),
                    values: vec![name.into()],
                }],
                ..Default::default()
            }),
            scope_by: Some(list_tasks_request::ScopeBy::ProjectId(ProjectIdentifier {
                organization: org.into(),
                domain: domain.into(),
                name: project.into(),
            })),
            ..Default::default()
        };
        let resp = self.tasks.list_tasks(req).await?;
        Ok(resp
            .tasks
            .into_iter()
            .next()
            .and_then(|t| t.task_id)
            .map(|id| id.version))
    }

    pub async fn finalize(&self, parent_action_name: &str) {
        self.inner
            .finalize_parent_action(&self.run_id, parent_action_name)
            .await;
    }

    /// Surfaces informer/worker-pool failures. May be called at most once.
    pub async fn watch_for_errors(&self) -> Result<(), Error> {
        self.inner
            .watch_for_errors()
            .await
            .map_err(|e| Error::Controller(format!("controller background failure: {e:?}")))
    }
}

/// The shared tokio runtime the controller's background workers run on. All SDK
/// async work must run on this runtime (worker_main / run block on it).
pub fn runtime() -> &'static tokio::runtime::Runtime {
    pyo3_async_runtimes::tokio::get_runtime()
}
