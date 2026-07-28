#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use chrono::NaiveDate;

#[cfg(test)]
use tokscale_core::{ClientId, GroupBy, UsageQuery};

mod overview;
pub(crate) use overview::{CacheRate, OverviewSummary};

// The aggregation engine produces these immutable projection types directly.
pub use tokscale_core::usage_views::{
    AgentEntry, ContributionDay, ContributionGrade, DailyClientInfo, DailyUsage, HourlyUsage,
    PeriodKind, PeriodUsage, UsageGraphData, UsageModelEntry, UsageTokenBreakdown, UsageView,
};
#[cfg(test)]
pub use tokscale_core::usage_views::{DailyModelInfo, HourlyModelInfo};
pub use tokscale_core::{aggregate_by_period, build_period_usage, find_peak_hour};

/// Bound glibc's process-wide arena count before the TUI starts worker threads.
/// The TUI explicitly trims after snapshot replacement, and one arena prevents
/// short-lived background folds from leaving otherwise unreachable arenas at
/// their high-water RSS (ADR 0008).
pub(super) fn configure_allocator() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    if std::env::var_os("MALLOC_ARENA_MAX").is_none() {
        unsafe {
            libc::mallopt(libc::M_ARENA_MAX, 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::GenerationLoader;
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::TempDir;
    use tokscale_core::{build_contribution_graph_for_today, calculate_streaks_for_today};

    async fn load_usage(
        loader: &GenerationLoader,
        enabled_clients: &[ClientId],
        group_by: GroupBy,
    ) -> Result<UsageView> {
        let prepared = loader.prepare(enabled_clients)?;
        let generation = loader.build(prepared).await?;
        generation
            .project(&UsageQuery::full(generation.universe(), group_by))
            .map_err(anyhow::Error::new)
    }

    #[test]
    fn test_client_all() {
        let clients = ClientId::ALL;
        let iterated_clients: Vec<ClientId> = ClientId::iter().collect();
        assert_eq!(clients, iterated_clients.as_slice());

        let pi_index = clients
            .iter()
            .position(|client| *client == ClientId::Pi)
            .unwrap();
        assert_eq!(clients[pi_index + 1], ClientId::Omp);
        assert_eq!(clients[pi_index + 2], ClientId::Kimi);
        let codebuff_index = clients
            .iter()
            .position(|client| *client == ClientId::Codebuff)
            .unwrap();
        assert_eq!(clients[codebuff_index + 1], ClientId::CodeBuddy);
        let zed_index = clients
            .iter()
            .position(|client| *client == ClientId::Zed)
            .unwrap();
        assert_eq!(clients[zed_index + 1], ClientId::Zcode);
        assert_eq!(clients[zed_index + 2], ClientId::Kiro);
        assert_eq!(clients[clients.len() - 3], ClientId::CommandCode);
        assert_eq!(clients[clients.len() - 2], ClientId::Grok);
        assert_eq!(clients.last(), Some(&ClientId::Devin));
    }

    #[test]
    fn test_client_as_str() {
        assert_eq!(ClientId::short_name(ClientId::OpenCode), "OpenCode");
        assert_eq!(ClientId::short_name(ClientId::Claude), "Claude");
        assert_eq!(ClientId::short_name(ClientId::Codex), "Codex");
        assert_eq!(ClientId::short_name(ClientId::Copilot), "Copilot");
        assert_eq!(ClientId::short_name(ClientId::Gemini), "Gemini");
        assert_eq!(ClientId::short_name(ClientId::Amp), "Amp");
        assert_eq!(ClientId::short_name(ClientId::Droid), "Droid");
        assert_eq!(ClientId::short_name(ClientId::OpenClaw), "OpenClaw");
        assert_eq!(ClientId::short_name(ClientId::Pi), "Pi");
        assert_eq!(ClientId::short_name(ClientId::Omp), "OMP");
        assert_eq!(ClientId::short_name(ClientId::Kimi), "Kimi");
        assert_eq!(ClientId::short_name(ClientId::Qwen), "Qwen");
        assert_eq!(ClientId::short_name(ClientId::RooCode), "Roo Code");
        assert_eq!(ClientId::short_name(ClientId::Mux), "Mux");
        assert_eq!(ClientId::short_name(ClientId::Kilo), "Kilo");
        assert_eq!(ClientId::short_name(ClientId::Hermes), "Hermes");
        assert_eq!(ClientId::short_name(ClientId::Codebuff), "Codebuff");
        assert_eq!(ClientId::short_name(ClientId::CodeBuddy), "CodeBuddy");
        assert_eq!(ClientId::short_name(ClientId::Antigravity), "Antigravity");
        assert_eq!(ClientId::short_name(ClientId::Zed), "Zed Agent");
        assert_eq!(ClientId::short_name(ClientId::Zcode), "ZCode");
        assert_eq!(ClientId::short_name(ClientId::Kiro), "Kiro");
        assert_eq!(ClientId::short_name(ClientId::Cline), "Cline");
    }

    #[test]
    fn test_token_breakdown_total() {
        let breakdown = UsageTokenBreakdown {
            input: 100,
            output: 200,
            cache_read: 50,
            cache_write: 25,
            reasoning: 10,
        };
        assert_eq!(breakdown.total(), 385);
    }

    #[test]
    #[should_panic(expected = "TUI token total exceeds u64::MAX")]
    fn test_token_breakdown_total_rejects_overflow() {
        let breakdown = UsageTokenBreakdown {
            input: u64::MAX,
            output: 1,
            cache_read: 0,
            cache_write: 0,
            reasoning: 0,
        };
        let _ = breakdown.total();
    }

    #[test]
    fn test_token_breakdown_default() {
        let breakdown = UsageTokenBreakdown::default();
        assert_eq!(breakdown.input, 0);
        assert_eq!(breakdown.output, 0);
        assert_eq!(breakdown.cache_read, 0);
        assert_eq!(breakdown.cache_write, 0);
        assert_eq!(breakdown.reasoning, 0);
        assert_eq!(breakdown.total(), 0);
    }

    #[test]
    fn test_build_contribution_graph_uses_provided_today() {
        let today = NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();
        let graph = build_contribution_graph_for_today(&[], today);
        assert!(graph.weeks.is_empty());

        let daily = vec![DailyUsage {
            date: NaiveDate::from_ymd_opt(2026, 3, 2).unwrap(),
            tokens: UsageTokenBreakdown::default(),
            cost: 0.0,
            client_breakdown: BTreeMap::new(),
            message_count: 0,
            turn_count: 0,
        }];
        let graph = build_contribution_graph_for_today(&daily, today);
        let last_day = graph
            .weeks
            .last()
            .and_then(|week| week.last())
            .and_then(|day| day.as_ref())
            .map(|day| day.date);
        assert_eq!(last_day, Some(today));
    }

    #[tokio::test]
    async fn generation_loader_loads_agent_usage_from_roocode_files() {
        let temp_dir = TempDir::new().unwrap();
        let task_root = temp_dir
            .path()
            .join(".config/Code/User/globalStorage/rooveterinaryinc.roo-cline/tasks");

        let architect_dir = task_root.join("task-architect");
        fs::create_dir_all(&architect_dir).unwrap();
        fs::write(
            architect_dir.join("ui_messages.json"),
            r#"[
  {
    "type": "say",
    "say": "api_req_started",
    "ts": "2026-03-07T16:00:00Z",
    "text": "{\"cost\":8.4,\"tokensIn\":420000,\"tokensOut\":120000,\"cacheReads\":32000,\"cacheWrites\":0,\"apiProtocol\":\"anthropic\"}"
  },
  {
    "type": "say",
    "say": "api_req_started",
    "ts": "2026-03-07T16:05:00Z",
    "text": "{\"cost\":3.1,\"tokensIn\":90000,\"tokensOut\":60000,\"cacheReads\":12000,\"cacheWrites\":0,\"apiProtocol\":\"anthropic\"}"
  }
]"#,
        )
        .unwrap();
        fs::write(
            architect_dir.join("api_conversation_history.json"),
            r#"before
<environment_details>
<model>claude-sonnet-4</model>
<slug>architect</slug>
<name>Architect</name>
</environment_details>
after"#,
        )
        .unwrap();

        let reviewer_dir = task_root.join("task-reviewer");
        fs::create_dir_all(&reviewer_dir).unwrap();
        fs::write(
            reviewer_dir.join("ui_messages.json"),
            r#"[
  {
    "type": "say",
    "say": "api_req_started",
    "ts": "2026-03-07T17:00:00Z",
    "text": "{\"cost\":1.8,\"tokensIn\":70000,\"tokensOut\":26000,\"cacheReads\":8000,\"cacheWrites\":0,\"apiProtocol\":\"anthropic\"}"
  },
  {
    "type": "say",
    "say": "api_req_started",
    "ts": "2026-03-07T17:09:00Z",
    "text": "{\"cost\":0.9,\"tokensIn\":22000,\"tokensOut\":18000,\"cacheReads\":3000,\"cacheWrites\":0,\"apiProtocol\":\"anthropic\"}"
  }
]"#,
        )
        .unwrap();
        fs::write(
            reviewer_dir.join("api_conversation_history.json"),
            r#"before
<environment_details>
<model>claude-haiku-4</model>
<slug>reviewer</slug>
<name>Reviewer</name>
</environment_details>
after"#,
        )
        .unwrap();

        let loader =
            GenerationLoader::with_filters(Some(temp_dir.path().to_path_buf()), None, None, None);
        let usage = load_usage(&loader, &[ClientId::RooCode], GroupBy::Model)
            .await
            .unwrap();

        assert_eq!(usage.agents.len(), 2);
        assert_eq!(usage.agents[0].agent, "Architect");
        assert_eq!(usage.agents[0].client, ClientId::RooCode);
        assert_eq!(usage.agents[0].message_count, 2);
        assert_eq!(usage.agents[0].tokens.total(), 734_000);

        assert_eq!(usage.agents[1].agent, "Reviewer");
        assert_eq!(usage.agents[1].message_count, 2);
        assert_eq!(usage.agents[1].tokens.total(), 147_000);
    }

    #[tokio::test]
    async fn generation_loader_keeps_gateway_model_under_its_client() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join(".local/share/opencode");
        fs::create_dir_all(&data_dir).unwrap();
        let conn = rusqlite::Connection::open(data_dir.join("opencode.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT NOT NULL);
             CREATE TABLE message (
                 id TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL,
                 data TEXT NOT NULL
             );",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message (id, session_id, data) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                "msg-1",
                "session-1",
                r#"{"id":"msg-1","role":"assistant","modelID":"accounts/fireworks/models/deepseek-v3-0324","providerID":"fireworks","cost":0.25,"tokens":{"input":10,"output":5,"reasoning":0,"cache":{"read":0,"write":0}},"time":{"created":1733011200000}}"#
            ],
        )
        .unwrap();
        drop(conn);

        let loader =
            GenerationLoader::with_filters(Some(temp_dir.path().to_path_buf()), None, None, None);
        let usage = load_usage(&loader, &[ClientId::OpenCode], GroupBy::ClientProviderModel)
            .await
            .unwrap();

        assert_eq!(usage.models.len(), 1);
        assert_eq!(usage.models[0].clients, [ClientId::OpenCode]);
        assert_eq!(usage.models[0].provider, "fireworks");
        assert_eq!(usage.models[0].model_id, "deepseek-v3");
        assert_eq!(usage.models[0].display_name, "deepseek-v3");
        assert_eq!(usage.models[0].tokens.total(), 15);
    }

    #[test]
    fn test_calculate_streaks_uses_provided_today() {
        let today = NaiveDate::from_ymd_opt(2026, 3, 3).unwrap();
        let daily = vec![
            DailyUsage {
                date: NaiveDate::from_ymd_opt(2026, 3, 2).unwrap(),
                tokens: UsageTokenBreakdown::default(),
                cost: 0.0,
                client_breakdown: BTreeMap::new(),
                message_count: 0,
                turn_count: 0,
            },
            DailyUsage {
                date: NaiveDate::from_ymd_opt(2026, 3, 3).unwrap(),
                tokens: UsageTokenBreakdown::default(),
                cost: 0.0,
                client_breakdown: BTreeMap::new(),
                message_count: 0,
                turn_count: 0,
            },
        ];
        let (current, longest) = calculate_streaks_for_today(&daily, today);
        assert_eq!(current, 2);
        assert_eq!(longest, 2);
    }

    fn period_day(date: &str, input_tokens: u64, cost: f64) -> DailyUsage {
        let tokens = UsageTokenBreakdown {
            input: input_tokens,
            ..UsageTokenBreakdown::default()
        };
        let mut models = BTreeMap::new();
        models.insert(
            "claude-sonnet-4".to_string(),
            DailyModelInfo {
                provider: "anthropic".to_string(),
                model_id: "claude-sonnet-4".to_string(),
                display_name: "claude-sonnet-4".to_string(),
                workspace_key: None,
                workspace_label: None,
                tokens: tokens.clone(),
                cost,
                messages: 1,
            },
        );

        let mut client_breakdown = BTreeMap::new();
        client_breakdown.insert(
            ClientId::Claude,
            DailyClientInfo {
                tokens: tokens.clone(),
                cost,
                models,
            },
        );

        DailyUsage {
            date: NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap(),
            tokens,
            cost,
            client_breakdown,
            message_count: 1,
            turn_count: 1,
        }
    }

    #[test]
    fn test_build_monthly_period_usage_groups_by_calendar_year() {
        let periods = build_period_usage(
            &[
                period_day("2026-06-02", 10, 1.0),
                period_day("2026-06-14", 20, 2.0),
                period_day("2026-05-01", 5, 0.5),
            ],
            PeriodKind::Monthly,
        );

        assert_eq!(periods.len(), 2);
        assert_eq!(periods[0].section_label, "2026");
        assert_eq!(periods[0].label, "June");
        assert_eq!(periods[0].short_label, "Jun");
        assert_eq!(periods[0].start_date.to_string(), "2026-06-01");
        assert_eq!(periods[0].end_date.to_string(), "2026-06-30");
        assert_eq!(periods[0].active_days, 2);
        assert_eq!(periods[0].tokens.input, 30);
        assert_eq!(periods[0].cost, 3.0);
        assert_eq!(
            periods[0].client_breakdown[&ClientId::Claude].models["claude-sonnet-4"].messages,
            2
        );
    }

    #[test]
    fn test_build_period_usage_counts_zero_token_message_days_as_active() {
        let periods = build_period_usage(&[period_day("2026-06-02", 0, 0.0)], PeriodKind::Monthly);

        assert_eq!(periods.len(), 1);
        assert_eq!(periods[0].active_days, 1);
        assert_eq!(periods[0].message_count, 1);
        assert_eq!(periods[0].tokens.total(), 0);
    }

    #[test]
    fn test_build_weekly_period_usage_uses_iso_week_year_for_cross_year_week() {
        let periods = build_period_usage(
            &[
                period_day("2026-01-04", 20, 2.0),
                period_day("2025-12-29", 10, 1.0),
                period_day("2025-12-28", 5, 0.5),
            ],
            PeriodKind::Weekly,
        );

        assert_eq!(periods.len(), 2);
        assert_eq!(periods[0].section_label, "2026");
        assert_eq!(periods[0].label, "W01 Dec 29 - Jan 04");
        assert_eq!(periods[0].short_label, "W01");
        assert_eq!(periods[0].start_date.to_string(), "2025-12-29");
        assert_eq!(periods[0].end_date.to_string(), "2026-01-04");
        assert_eq!(periods[0].active_days, 2);
        assert_eq!(periods[0].tokens.input, 30);
        assert_eq!(periods[1].section_label, "2025");
        assert_eq!(periods[1].label, "W52 Dec 22 - Dec 28");
    }
}
