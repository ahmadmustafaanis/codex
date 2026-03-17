use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;

use crate::client::ModelClient;
use crate::client::ModelClientSession;
use crate::client_common::Prompt;
use crate::codex::INITIAL_SUBMIT_ID;
use crate::codex::Session;
use crate::codex::TurnContext;
use crate::codex::build_prompt;
use crate::codex::built_tools;
use crate::codex::run_turn;
use crate::error::Result as CodexResult;
use crate::protocol::EventMsg;
use crate::protocol::TurnStartedEvent;
use crate::state::TaskKind;
use codex_otel::SessionTelemetry;
use codex_otel::metrics::names::STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC;
use codex_otel::metrics::names::STARTUP_PREWARM_DURATION_METRIC;
use codex_protocol::models::BaseInstructions;
use codex_protocol::user_input::UserInput;
use tracing::Instrument;
use tracing::trace_span;

use super::SessionTask;
use super::SessionTaskContext;

pub(crate) struct StartupPrewarmHandle {
    task: JoinHandle<CodexResult<ModelClientSession>>,
    started_at: Instant,
    timeout: Duration,
}

enum StartupPrewarmResolution {
    Cancelled,
    Ready(Box<ModelClientSession>),
    Unavailable {
        status: &'static str,
        prewarm_duration: Option<Duration>,
    },
}

impl StartupPrewarmHandle {
    pub(crate) fn new(
        task: JoinHandle<CodexResult<ModelClientSession>>,
        started_at: Instant,
        timeout: Duration,
    ) -> Self {
        Self {
            task,
            started_at,
            timeout,
        }
    }

    async fn resolve(
        self,
        session_telemetry: &SessionTelemetry,
        cancellation_token: &CancellationToken,
    ) -> StartupPrewarmResolution {
        let Self {
            mut task,
            started_at,
            timeout,
        } = self;
        let age_at_first_turn = started_at.elapsed();
        let remaining = timeout.saturating_sub(age_at_first_turn);

        let resolution = if task.is_finished() {
            Self::resolution_from_join_result(task.await, started_at)
        } else {
            match tokio::select! {
                _ = cancellation_token.cancelled() => None,
                result = tokio::time::timeout(remaining, &mut task) => Some(result),
            } {
                Some(Ok(result)) => Self::resolution_from_join_result(result, started_at),
                Some(Err(_elapsed)) => {
                    task.abort();
                    info!("startup websocket prewarm timed out before the first turn could use it");
                    StartupPrewarmResolution::Unavailable {
                        status: "timed_out",
                        prewarm_duration: Some(started_at.elapsed()),
                    }
                }
                None => {
                    task.abort();
                    session_telemetry.record_duration(
                        STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC,
                        age_at_first_turn,
                        &[("status", "cancelled")],
                    );
                    session_telemetry.record_duration(
                        STARTUP_PREWARM_DURATION_METRIC,
                        started_at.elapsed(),
                        &[("status", "cancelled")],
                    );
                    return StartupPrewarmResolution::Cancelled;
                }
            }
        };

        match resolution {
            StartupPrewarmResolution::Cancelled => StartupPrewarmResolution::Cancelled,
            StartupPrewarmResolution::Ready(prewarmed_session) => {
                session_telemetry.record_duration(
                    STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC,
                    age_at_first_turn,
                    &[("status", "consumed")],
                );
                StartupPrewarmResolution::Ready(prewarmed_session)
            }
            StartupPrewarmResolution::Unavailable {
                status,
                prewarm_duration,
            } => {
                session_telemetry.record_duration(
                    STARTUP_PREWARM_AGE_AT_FIRST_TURN_METRIC,
                    age_at_first_turn,
                    &[("status", status)],
                );
                if let Some(prewarm_duration) = prewarm_duration {
                    session_telemetry.record_duration(
                        STARTUP_PREWARM_DURATION_METRIC,
                        prewarm_duration,
                        &[("status", status)],
                    );
                }
                StartupPrewarmResolution::Unavailable {
                    status,
                    prewarm_duration,
                }
            }
        }
    }

    fn resolution_from_join_result(
        result: std::result::Result<CodexResult<ModelClientSession>, tokio::task::JoinError>,
        started_at: Instant,
    ) -> StartupPrewarmResolution {
        match result {
            Ok(Ok(prewarmed_session)) => {
                StartupPrewarmResolution::Ready(Box::new(prewarmed_session))
            }
            Ok(Err(err)) => {
                warn!("startup websocket prewarm setup failed: {err:#}");
                StartupPrewarmResolution::Unavailable {
                    status: "failed",
                    prewarm_duration: None,
                }
            }
            Err(err) => {
                warn!("startup websocket prewarm setup join failed: {err}");
                StartupPrewarmResolution::Unavailable {
                    status: "join_failed",
                    prewarm_duration: Some(started_at.elapsed()),
                }
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct RegularTask;

impl RegularTask {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) async fn schedule_startup_prewarm(session: Arc<Session>, base_instructions: String) {
        let session_telemetry = session.services.session_telemetry.clone();
        let websocket_connect_timeout = session.provider().await.websocket_connect_timeout();
        let started_at = Instant::now();
        let startup_prewarm_session = Arc::clone(&session);
        let startup_prewarm = tokio::spawn(async move {
            let result =
                Self::schedule_startup_prewarm_inner(startup_prewarm_session, base_instructions)
                    .await;
            let status = if result.is_ok() { "ready" } else { "failed" };
            session_telemetry.record_duration(
                STARTUP_PREWARM_DURATION_METRIC,
                started_at.elapsed(),
                &[("status", status)],
            );
            result
        });
        session
            .set_startup_prewarm(StartupPrewarmHandle::new(
                startup_prewarm,
                started_at,
                websocket_connect_timeout,
            ))
            .await;
    }

    async fn schedule_startup_prewarm_inner(
        session: Arc<Session>,
        base_instructions: String,
    ) -> CodexResult<ModelClientSession> {
        let startup_turn_context = session
            .new_default_turn_with_sub_id(INITIAL_SUBMIT_ID.to_owned())
            .await;
        let startup_cancellation_token = CancellationToken::new();
        let startup_router = built_tools(
            session.as_ref(),
            startup_turn_context.as_ref(),
            &[],
            &HashSet::new(),
            /*skills_outcome*/ None,
            &startup_cancellation_token,
        )
        .await?;
        let startup_prompt = build_prompt(
            Vec::new(),
            startup_router.as_ref(),
            startup_turn_context.as_ref(),
            BaseInstructions {
                text: base_instructions,
            },
        );
        let startup_turn_metadata_header = startup_turn_context
            .turn_metadata_state
            .current_header_value();
        Self::with_startup_prewarm(
            session.services.model_client.clone(),
            startup_prompt,
            startup_turn_context,
            startup_turn_metadata_header,
        )
        .await
    }

    pub(crate) async fn with_startup_prewarm(
        model_client: ModelClient,
        prompt: Prompt,
        turn_context: Arc<TurnContext>,
        turn_metadata_header: Option<String>,
    ) -> CodexResult<ModelClientSession> {
        let mut client_session = model_client.new_session();
        client_session
            .prewarm_websocket(
                &prompt,
                &turn_context.model_info,
                &turn_context.session_telemetry,
                turn_context.reasoning_effort,
                turn_context.reasoning_summary,
                turn_context.config.service_tier,
                turn_metadata_header.as_deref(),
            )
            .await?;

        Ok(client_session)
    }

    async fn take_prewarmed_session(
        &self,
        session: &Session,
        cancellation_token: &CancellationToken,
    ) -> StartupPrewarmResolution {
        let Some(startup_prewarm) = session.take_startup_prewarm().await else {
            return StartupPrewarmResolution::Unavailable {
                status: "not_scheduled",
                prewarm_duration: None,
            };
        };
        startup_prewarm
            .resolve(&session.services.session_telemetry, cancellation_token)
            .await
    }
}

#[async_trait]
impl SessionTask for RegularTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }

    fn span_name(&self) -> &'static str {
        "session_task.turn"
    }

    async fn run(
        self: Arc<Self>,
        session: Arc<SessionTaskContext>,
        ctx: Arc<TurnContext>,
        input: Vec<UserInput>,
        cancellation_token: CancellationToken,
    ) -> Option<String> {
        let sess = session.clone_session();
        let run_turn_span = trace_span!("run_turn");
        // Regular turns emit `TurnStarted` inline so first-turn lifecycle does
        // not wait on startup prewarm resolution.
        let event = EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: ctx.sub_id.clone(),
            model_context_window: ctx.model_context_window(),
            collaboration_mode_kind: ctx.collaboration_mode.mode,
        });
        sess.send_event(ctx.as_ref(), event).await;
        sess.set_server_reasoning_included(/*included*/ false).await;
        let prewarmed_client_session = match self
            .take_prewarmed_session(&sess, &cancellation_token)
            .await
        {
            StartupPrewarmResolution::Cancelled => return None,
            StartupPrewarmResolution::Unavailable { .. } => None,
            StartupPrewarmResolution::Ready(prewarmed_client_session) => {
                Some(*prewarmed_client_session)
            }
        };
        run_turn(
            sess,
            ctx,
            input,
            prewarmed_client_session,
            cancellation_token,
        )
        .instrument(run_turn_span)
        .await
    }
}
