use super::*;

#[derive(Clone, Debug)]
pub(super) struct BtwThreadState {
    /// Thread to return to when the current BTW thread is dismissed.
    pub(super) return_thread_id: ThreadId,
    /// Pretty parent label for the next synthetic fork banner, consumed on first attach.
    pub(super) pending_fork_banner_label: Option<String>,
}

impl App {
    /// Shows or clears the BTW footer hint based on the currently displayed thread.
    ///
    /// BTW threads form a return chain: each child points at the thread the user should return to
    /// on `Esc`. The footer reflects the visible nesting depth and disables rename actions while
    /// the ephemeral BTW transcript is in the foreground.
    pub(super) fn sync_btw_footer_hint(&mut self) {
        let Some(active_thread_id) = self.current_displayed_thread_id() else {
            self.chat_widget.set_footer_hint_override(None);
            self.chat_widget.set_thread_rename_enabled(true);
            return;
        };
        let Some(mut return_thread_id) = self
            .btw_threads
            .get(&active_thread_id)
            .map(|state| state.return_thread_id)
        else {
            self.chat_widget.set_footer_hint_override(None);
            self.chat_widget.set_thread_rename_enabled(true);
            return;
        };
        self.chat_widget.set_thread_rename_enabled(false);
        let mut depth = 1usize;
        while let Some(next_return_thread_id) = self
            .btw_threads
            .get(&return_thread_id)
            .map(|state| state.return_thread_id)
        {
            depth += 1;
            return_thread_id = next_return_thread_id;
        }
        let repeated_prefix = "BTW from ".repeat(depth.saturating_sub(1));
        let label = if self.primary_thread_id == Some(return_thread_id) {
            format!("from {repeated_prefix}main thread · Esc to return")
        } else {
            let parent_label = self.thread_label(return_thread_id);
            format!("from {repeated_prefix}parent thread ({parent_label}) · Esc to return")
        };
        self.chat_widget
            .set_footer_hint_override(Some(vec![("BTW".to_string(), label)]));
    }

    pub(super) fn active_btw_return_thread(&self) -> Option<ThreadId> {
        self.current_displayed_thread_id()
            .and_then(|thread_id| self.btw_threads.get(&thread_id))
            .map(|state| state.return_thread_id)
    }

    /// Shuts down and forgets one ephemeral BTW thread.
    ///
    /// This removes the thread from the core thread manager, aborts its listener task, clears any
    /// TUI bookkeeping for replay/navigation, and recomputes the footer state. Callers that are
    /// leaving a nested BTW stack are responsible for discarding the whole hidden chain in the
    /// correct order.
    pub(super) async fn discard_btw_thread(&mut self, thread_id: ThreadId) {
        if self.chat_widget.thread_id() == Some(thread_id) {
            self.backtrack.pending_rollback = None;
            self.suppress_shutdown_complete_thread_id = Some(thread_id);
            self.chat_widget.submit_op(Op::Shutdown);
        } else if let Ok(thread) = self.server.get_thread(thread_id).await {
            let _ = thread.submit(Op::Shutdown).await;
        }
        self.server.remove_thread(&thread_id).await;
        self.abort_thread_event_listener(thread_id);
        self.thread_event_channels.remove(&thread_id);
        self.btw_threads.remove(&thread_id);
        if self.active_thread_id == Some(thread_id) {
            self.clear_active_thread().await;
        }
        self.sync_active_agent_label();
    }

    pub(super) async fn handle_start_btw(
        &mut self,
        tui: &mut tui::Tui,
        parent_thread_id: ThreadId,
        user_message: crate::chatwidget::UserMessage,
    ) -> Result<AppRunControl> {
        self.session_telemetry
            .counter("codex.thread.btw", 1, &[("source", "slash_command")]);
        self.refresh_in_memory_config_from_disk_best_effort("starting a BTW subagent")
            .await;
        let path = match self.server.get_thread(parent_thread_id).await {
            Ok(thread) => thread.rollout_path().filter(|path| path.exists()),
            Err(err) => {
                self.chat_widget.add_error_message(format!(
                    "Failed to fork BTW thread from {parent_thread_id}: {err}"
                ));
                return Ok(AppRunControl::Continue);
            }
        };
        let Some(path) = path else {
            self.chat_widget.add_error_message(
                "A thread must contain at least one turn before /btw can fork it.".to_string(),
            );
            return Ok(AppRunControl::Continue);
        };

        match self
            .server
            .fork_thread(usize::MAX, self.config.clone(), path.clone(), false, None)
            .await
        {
            Ok(forked) => {
                let child_thread_id = forked.thread_id;
                let parent_label = self.thread_label(parent_thread_id);
                let pending_fork_banner_label = if self.chat_widget.thread_id()
                    == Some(parent_thread_id)
                {
                    self.chat_widget
                        .thread_name()
                        .filter(|name| !name.trim().is_empty())
                } else if let Some(channel) = self.thread_event_channels.get(&parent_thread_id) {
                    let store = channel.store.lock().await;
                    match store.session_configured.as_ref().map(|event| &event.msg) {
                        Some(EventMsg::SessionConfigured(session)) => session
                            .thread_name
                            .clone()
                            .filter(|name| !name.trim().is_empty()),
                        _ => None,
                    }
                } else {
                    None
                };
                self.attach_live_thread(
                    child_thread_id,
                    Arc::clone(&forked.thread),
                    forked.session_configured,
                    false,
                )
                .await?;
                self.btw_threads.insert(
                    child_thread_id,
                    BtwThreadState {
                        return_thread_id: parent_thread_id,
                        pending_fork_banner_label,
                    },
                );
                self.select_agent_thread(tui, child_thread_id).await?;
                if self.active_thread_id == Some(child_thread_id) {
                    // Use turn-local developer instructions rather than mutating forked history so
                    // the side exchange stays isolated from the parent thread unless the user
                    // explicitly shares it later.
                    let developer_instructions = format!(
                        "<btw_context>\n\
You are a forked subagent answering a side question about the parent thread ({parent_label}).\n\
The parent model will not automatically see this exchange unless the user explicitly shares it later.\n\
Use the forked thread history as your source of truth.\n\
If the parent thread appears to be missing some very recent in-flight progress, say that briefly instead of inventing it.\n\
Answer the user's side question directly and concisely.\n\
</btw_context>"
                    );
                    if let Some(op) = self
                        .chat_widget
                        .submit_user_message_with_developer_instructions(
                            user_message,
                            developer_instructions,
                        )
                    {
                        self.note_active_thread_outbound_op(&op).await;
                    }
                } else {
                    self.btw_threads.remove(&child_thread_id);
                    self.chat_widget.add_error_message(format!(
                        "Failed to switch into BTW thread {child_thread_id}."
                    ));
                }
            }
            Err(err) => {
                let path_display = path.display();
                self.chat_widget.add_error_message(format!(
                    "Failed to start BTW thread from {path_display}: {err}"
                ));
            }
        }

        Ok(AppRunControl::Continue)
    }
}
