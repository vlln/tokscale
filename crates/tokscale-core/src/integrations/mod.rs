//! Vertical client integrations and their shared acquisition machinery.

pub(crate) mod amp;
pub(crate) mod antigravity;
pub(crate) mod cache;
pub(crate) mod claude;
pub(crate) mod cline;
pub(crate) mod codebuddy;
pub(crate) mod codebuff;
pub(crate) mod codex;
pub(crate) mod commandcode;
pub(crate) mod copilot;
mod decoder;
pub(crate) mod devin;
pub(crate) mod discover;
pub(crate) mod droid;
pub(crate) mod error;
pub(crate) mod file;
pub(crate) mod gemini;
pub(crate) mod goose;
pub(crate) mod grok;
pub(crate) mod hermes;
pub(crate) mod junie;
pub(crate) mod kilo;
pub(crate) mod kimi;
pub(crate) mod kiro;
pub(crate) mod mux;
pub(crate) mod openclaw;
pub(crate) mod opencode;

pub(crate) mod omp;
pub(crate) mod pi;
pub(crate) mod qwen;
pub(crate) mod roocode;
mod runtime;
mod source;
pub(crate) mod warp;
pub(crate) mod zcode;
pub(crate) mod zed;

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use rayon::prelude::*;

use crate::clients::ClientId;
use crate::input_health::{DataHealth, InputFailure, InputHealth, InputStatus, RejectionSummary};
#[cfg(test)]
use crate::message_cache::DecoderId;
use crate::message_cache::{DecoderRevision, InputFileIdentity};
use crate::{
    message_cache, pricing, records::ParsedMessage, scanner, ClientUniverse, UnifiedMessage,
};

pub(crate) use decoder::{DecoderRoute, DecoderSpec};
pub(crate) use error::{
    InputDiscoveryError, InputParseError, InputPipelineError, InputPlanningError,
};
pub(crate) use runtime::{BoundMessageSink, FoldContext};
pub(crate) use source::SourceSpec;

pub(crate) const MODEL_ID_CANONICALIZATION_REVISION: DecoderRevision = 3;
// Record-level rejection and parsed Agent identity changes alter the cached
// scan outcome, so old OpenCode shards must be rebuilt.
pub(crate) const OPENCODE_CURRENT_SQLITE_REVISION: DecoderRevision =
    MODEL_ID_CANONICALIZATION_REVISION + 6;
pub(crate) const EXPLICIT_TOKEN_OVERFLOW_REVISION: DecoderRevision =
    MODEL_ID_CANONICALIZATION_REVISION + 1;
pub(crate) const ZED_RECORD_FILTER_REVISION: DecoderRevision = EXPLICIT_TOKEN_OVERFLOW_REVISION + 2;
// Codex Agent roles use the shared neutral text normalizer. Keep cached
// messages aligned when that parser-owned identity changes.
pub(crate) const CODEX_EXEC_IDENTITY_REVISION: DecoderRevision =
    MODEL_ID_CANONICALIZATION_REVISION + 4;

pub(crate) trait ClientIntegration: Sync {
    fn client(&self) -> ClientId;

    fn discover_checked(
        &self,
        ctx: &DiscoveryContext<'_>,
    ) -> Result<Vec<InputUnit>, InputDiscoveryError>;

    fn parse_checked(&self, units: Vec<InputUnit>, ctx: &ParseContext<'_>) -> Vec<ParsedUnit>;

    fn plan_cache_hit(
        &self,
        unit: InputUnit,
        _input_cache: &message_cache::InputMessageCache,
    ) -> Result<CacheHitPlan, InputPlanningError> {
        Ok(CacheHitPlan::Miss(unit))
    }

    fn fold(
        &self,
        parsed: Vec<ParsedUnit>,
        ctx: &mut FoldContext<'_>,
        sink: &mut BoundMessageSink<'_>,
    ) -> Result<(), InputPipelineError>;

    fn fold_batches(
        &self,
        batches: &mut ParsedBatchInput<'_>,
        ctx: &mut FoldContext<'_>,
        sink: &mut BoundMessageSink<'_>,
    ) -> Result<(), InputPipelineError> {
        while let Some(parsed) = batches.next(ctx)? {
            self.fold(parsed, ctx, sink)?;
        }
        Ok(())
    }
}

pub(crate) struct DiscoveryContext<'a> {
    pub home_dir: &'a Path,
    pub scanner_settings: &'a scanner::ScannerSettings,
}

pub(crate) struct ParseContext<'a> {
    pub pricing: Option<&'a pricing::PricingService>,
}

pub(crate) trait UnifiedMessageSink {
    fn push_message(&mut self, message: UnifiedMessage);
}

impl UnifiedMessageSink for Vec<UnifiedMessage> {
    fn push_message(&mut self, message: UnifiedMessage) {
        self.push(message);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InputUnit {
    pub path: PathBuf,
    pub fingerprint_policy: FingerprintPolicy,
    pub decoder: DecoderSpec,
    prepared_snapshot: Option<message_cache::InputSnapshot>,
    snapshot_confirmed_for_execution: bool,
    planned_cache_meta: Option<message_cache::CachedInputMeta>,
    cache_lookup_completed_no_hit: bool,
}

impl InputUnit {
    pub(crate) fn plain_file(path: PathBuf, decoder: DecoderSpec) -> Self {
        Self {
            path,
            fingerprint_policy: FingerprintPolicy::PlainFile,
            decoder,
            prepared_snapshot: None,
            snapshot_confirmed_for_execution: false,
            planned_cache_meta: None,
            cache_lookup_completed_no_hit: false,
        }
    }

    pub(crate) fn sqlite_with_wal(path: PathBuf, decoder: DecoderSpec) -> Self {
        Self {
            path,
            fingerprint_policy: FingerprintPolicy::SqliteWithWal,
            decoder,
            prepared_snapshot: None,
            snapshot_confirmed_for_execution: false,
            planned_cache_meta: None,
            cache_lookup_completed_no_hit: false,
        }
    }

    pub(crate) fn no_message_cache(path: PathBuf, decoder: DecoderSpec) -> Self {
        Self {
            path,
            fingerprint_policy: FingerprintPolicy::NoMessageCache,
            decoder,
            prepared_snapshot: None,
            snapshot_confirmed_for_execution: false,
            planned_cache_meta: None,
            cache_lookup_completed_no_hit: false,
        }
    }

    pub(crate) fn claude_code(path: PathBuf, home_dir: PathBuf, decoder: DecoderSpec) -> Self {
        Self {
            path,
            fingerprint_policy: FingerprintPolicy::ClaudeCodeWithHome {
                home_dir,
                parent_session_path: None,
            },
            decoder,
            prepared_snapshot: None,
            snapshot_confirmed_for_execution: false,
            planned_cache_meta: None,
            cache_lookup_completed_no_hit: false,
        }
    }

    pub(crate) fn with_dependency(mut self, dependency_path: PathBuf) -> Self {
        self.fingerprint_policy = FingerprintPolicy::PrimaryWithDependency {
            dependency_path,
            related_failure_policy: message_cache::RelatedInputFailurePolicy::FailInput,
        };
        self
    }

    pub(crate) fn with_optional_dependency(mut self, dependency_path: PathBuf) -> Self {
        self.fingerprint_policy = FingerprintPolicy::PrimaryWithDependency {
            dependency_path,
            related_failure_policy: message_cache::RelatedInputFailurePolicy::PreservePrimary,
        };
        self
    }

    pub(crate) fn preserves_primary_on_related_failure(&self) -> bool {
        match &self.fingerprint_policy {
            FingerprintPolicy::PrimaryWithSiblings {
                related_failure_policy,
                ..
            }
            | FingerprintPolicy::PrimaryWithDependency {
                related_failure_policy,
                ..
            } => {
                *related_failure_policy == message_cache::RelatedInputFailurePolicy::PreservePrimary
            }
            _ => false,
        }
    }

    pub(crate) fn with_claude_parent_session(mut self, parent_session_path: PathBuf) -> Self {
        let FingerprintPolicy::ClaudeCodeWithHome {
            parent_session_path: configured_parent,
            ..
        } = &mut self.fingerprint_policy
        else {
            unreachable!("Claude parent dependency requires a Claude fingerprint policy");
        };
        *configured_parent = Some(parent_session_path);
        self
    }

    pub(crate) fn prepare_snapshot(mut self) -> Result<Self, message_cache::InputSnapshotError> {
        if self.prepared_snapshot.is_none() {
            self.prepared_snapshot = Some(self.input_policy().snapshot()?);
        }
        Ok(self)
    }

    pub(crate) fn revalidate_snapshot_for_cache_decision(
        &mut self,
    ) -> Result<(), message_cache::InputSnapshotError> {
        if self.snapshot_confirmed_for_execution {
            return Ok(());
        }
        self.prepared_snapshot = Some(self.input_policy().snapshot()?);
        self.snapshot_confirmed_for_execution = true;
        Ok(())
    }

    pub(crate) fn refresh_prepared_snapshot_for_inventory_probe(
        &mut self,
    ) -> Result<(), message_cache::InputSnapshotError> {
        self.prepared_snapshot = Some(self.input_policy().snapshot()?);
        // An inventory probe may decide that no execution is needed. If this
        // unit is executed, pricing or another await can still follow, so the
        // cache-hit planner must confirm the snapshot again at its boundary.
        self.snapshot_confirmed_for_execution = false;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn digest_paths(&self) -> Vec<PathBuf> {
        self.input_policy().paths()
    }

    pub(crate) fn take_input_snapshot(
        &mut self,
    ) -> Result<message_cache::InputSnapshot, message_cache::InputSnapshotError> {
        self.snapshot_confirmed_for_execution = false;
        match self.prepared_snapshot.take() {
            Some(snapshot) => Ok(snapshot),
            None => self.input_policy().snapshot(),
        }
    }

    pub(crate) fn prepared_input_snapshot(&self) -> Option<&message_cache::InputSnapshot> {
        self.prepared_snapshot.as_ref()
    }

    pub(crate) fn release_prepared_snapshot(&mut self) {
        self.prepared_snapshot = None;
        self.snapshot_confirmed_for_execution = false;
    }

    pub(crate) fn set_planned_cache_meta(&mut self, meta: message_cache::CachedInputMeta) {
        self.planned_cache_meta = Some(meta);
    }

    pub(crate) fn take_planned_cache_meta(&mut self) -> Option<message_cache::CachedInputMeta> {
        self.planned_cache_meta.take()
    }

    pub(crate) fn mark_cache_lookup_completed_no_hit(&mut self) {
        self.cache_lookup_completed_no_hit = true;
    }

    pub(crate) fn take_cache_lookup_completed_no_hit(&mut self) -> bool {
        std::mem::take(&mut self.cache_lookup_completed_no_hit)
    }

    pub(crate) fn update_inventory_signature(&self, hasher: &mut sha2::Sha256) {
        use sha2::Digest;

        let snapshot = self
            .prepared_snapshot
            .as_ref()
            .expect("inventory units must carry a prepared input snapshot");
        message_cache::hash_inventory_bytes(
            hasher,
            self.decoder.version().decoder_id.stable_name().as_bytes(),
        );
        hasher.update(self.decoder.version().revision.to_le_bytes());
        self.update_decoder_inventory_signature(hasher);
        self.update_policy_inventory_signature(hasher);
        self.input_policy()
            .update_inventory_signature(snapshot, hasher);
    }

    pub(crate) fn inventory_signature_digest(&self) -> [u8; 32] {
        use sha2::Digest;

        let mut hasher = sha2::Sha256::new();
        message_cache::hash_inventory_bytes(&mut hasher, b"tokscale/input-inventory-unit");
        self.update_inventory_signature(&mut hasher);
        hasher.finalize().into()
    }

    fn update_decoder_inventory_signature(&self, hasher: &mut sha2::Sha256) {
        let (name, detail) = match self.decoder.route() {
            DecoderRoute::None => ("none", None),
            DecoderRoute::OpenCodeSqlite => ("opencode-sqlite", None),
            DecoderRoute::AntigravityCliSqlite => ("antigravity-cli-sqlite", None),
            DecoderRoute::KiroFile => ("kiro-file", None),
            DecoderRoute::KiroSqlite => ("kiro-sqlite", None),
            DecoderRoute::KiroGlobalStorage => ("kiro-global-storage", None),
            DecoderRoute::CodeBuddyJsonl => ("codebuddy-jsonl", None),
            DecoderRoute::CodeBuddyExtensionLog { origin } => (
                "codebuddy-extension-log",
                Some(match origin {
                    CodeBuddyLogOrigin::Extension => "extension",
                    CodeBuddyLogOrigin::Host => "host",
                }),
            ),
            DecoderRoute::Codex => ("codex", None),
        };
        message_cache::hash_inventory_bytes(hasher, name.as_bytes());
        message_cache::hash_inventory_bytes(hasher, detail.unwrap_or("").as_bytes());
    }

    fn update_policy_inventory_signature(&self, hasher: &mut sha2::Sha256) {
        match &self.fingerprint_policy {
            FingerprintPolicy::PlainFile => {
                message_cache::hash_inventory_bytes(hasher, b"plain-file");
            }
            FingerprintPolicy::SqliteWithWal => {
                message_cache::hash_inventory_bytes(hasher, b"sqlite-with-wal");
            }
            FingerprintPolicy::ClaudeCodeWithHome {
                home_dir,
                parent_session_path,
                ..
            } => {
                message_cache::hash_inventory_bytes(hasher, b"claude-code-with-home");
                message_cache::hash_inventory_path(hasher, home_dir);
                message_cache::hash_inventory_bytes(
                    hasher,
                    if parent_session_path.is_some() {
                        b"parent-session"
                    } else {
                        b"no-parent-session"
                    },
                );
                if let Some(parent_session_path) = parent_session_path {
                    message_cache::hash_inventory_path(hasher, parent_session_path);
                }
            }
            FingerprintPolicy::PrimaryWithSiblings {
                sibling_names,
                related_failure_policy,
            } => {
                message_cache::hash_inventory_bytes(hasher, b"primary-with-siblings");
                message_cache::hash_inventory_bytes(
                    hasher,
                    match related_failure_policy {
                        message_cache::RelatedInputFailurePolicy::FailInput => b"fail-input",
                        message_cache::RelatedInputFailurePolicy::PreservePrimary => {
                            b"preserve-primary"
                        }
                    },
                );
                message_cache::hash_inventory_len(hasher, sibling_names.len());
                for name in *sibling_names {
                    message_cache::hash_inventory_bytes(hasher, name.as_bytes());
                }
            }
            FingerprintPolicy::PrimaryWithDependency {
                dependency_path,
                related_failure_policy,
            } => {
                message_cache::hash_inventory_bytes(hasher, b"primary-with-dependency");
                message_cache::hash_inventory_bytes(
                    hasher,
                    match related_failure_policy {
                        message_cache::RelatedInputFailurePolicy::FailInput => b"fail-input",
                        message_cache::RelatedInputFailurePolicy::PreservePrimary => {
                            b"preserve-primary"
                        }
                    },
                );
                message_cache::hash_inventory_path(hasher, dependency_path);
            }
            FingerprintPolicy::NoMessageCache => {
                message_cache::hash_inventory_bytes(hasher, b"no-message-cache");
            }
        }
    }

    pub(crate) fn input_policy(&self) -> message_cache::InputPolicy {
        match &self.fingerprint_policy {
            FingerprintPolicy::PlainFile | FingerprintPolicy::NoMessageCache => {
                message_cache::InputPolicy::plain(&self.path)
            }
            FingerprintPolicy::SqliteWithWal => {
                message_cache::InputPolicy::sqlite_with_wal(&self.path)
            }
            FingerprintPolicy::ClaudeCodeWithHome {
                parent_session_path,
                ..
            } => message_cache::InputPolicy::claude_code(&self.path, parent_session_path.clone()),
            FingerprintPolicy::PrimaryWithSiblings {
                sibling_names,
                related_failure_policy,
            } => {
                message_cache::InputPolicy::with_siblings(&self.path, sibling_names.iter().copied())
                    .with_related_failure_policy(*related_failure_policy)
            }
            FingerprintPolicy::PrimaryWithDependency {
                dependency_path,
                related_failure_policy,
            } => message_cache::InputPolicy::with_dependency(&self.path, dependency_path.clone())
                .with_related_failure_policy(*related_failure_policy),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodeBuddyLogOrigin {
    Extension,
    Host,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FingerprintPolicy {
    PlainFile,
    SqliteWithWal,
    ClaudeCodeWithHome {
        home_dir: PathBuf,
        parent_session_path: Option<PathBuf>,
    },
    PrimaryWithSiblings {
        sibling_names: &'static [&'static str],
        related_failure_policy: message_cache::RelatedInputFailurePolicy,
    },
    PrimaryWithDependency {
        dependency_path: PathBuf,
        related_failure_policy: message_cache::RelatedInputFailurePolicy,
    },
    NoMessageCache,
}

#[derive(Debug)]
pub(crate) enum UnitMessagePayload {
    Fresh(Vec<ParsedMessage>),
    CodexFresh(Vec<ParsedMessage>),
    CacheHit(message_cache::CacheReadPlan),
    CodexCacheHit(message_cache::CacheReadPlan),
    CodexAppend(Box<codex::CodexAppendInput>),
}

/// Scan status and record rejections for one parsed unit, boxed to keep
/// `ParsedUnit`'s inline size close to `InputUnit`'s.
#[derive(Debug, Default)]
pub(crate) struct UnitScanHealth {
    pub status: InputStatus,
    pub rejections: RejectionSummary,
}

#[derive(Debug)]
pub(crate) struct ParsedUnit {
    pub unit: InputUnit,
    pub messages: UnitMessagePayload,
    pub cache_write: Option<Box<message_cache::CacheWritePlan>>,
    pub invalidate_cache: bool,
    pub health: Box<UnitScanHealth>,
}

impl ParsedUnit {
    /// A unit whose scan finished without input-level damage. Record-level
    /// rejections, if any, are attached separately by the scan seam.
    pub(crate) fn healthy(
        unit: InputUnit,
        messages: UnitMessagePayload,
        cache_write: Option<Box<message_cache::CacheWritePlan>>,
        invalidate_cache: bool,
    ) -> Self {
        Self {
            unit,
            messages,
            cache_write,
            invalidate_cache,
            health: Box::default(),
        }
    }

    /// A unit whose input could not be read at all. It contributes no
    /// messages and leaves any previously cached shard untouched: that shard
    /// is only served again if the input's fingerprint matches, in which
    /// case its content is still authoritative.
    pub(crate) fn unavailable(mut unit: InputUnit, failure: InputFailure) -> Self {
        unit.release_prepared_snapshot();
        Self {
            unit,
            messages: UnitMessagePayload::Fresh(Vec::new()),
            cache_write: None,
            invalidate_cache: false,
            health: Box::new(UnitScanHealth {
                status: InputStatus::Unavailable { failure },
                rejections: RejectionSummary::default(),
            }),
        }
    }
}

pub(crate) fn integration_for(client: ClientId) -> &'static dyn ClientIntegration {
    match client {
        ClientId::OpenCode => &opencode::INTEGRATION,
        ClientId::Claude => &claude::INTEGRATION,
        ClientId::Codex => &codex::INTEGRATION,
        ClientId::Gemini => &gemini::INTEGRATION,
        ClientId::Amp => &amp::INTEGRATION,
        ClientId::Droid => &droid::INTEGRATION,
        ClientId::OpenClaw => &openclaw::INTEGRATION,
        ClientId::Pi => &pi::INTEGRATION,
        ClientId::Omp => &omp::INTEGRATION,
        ClientId::Kimi => &kimi::INTEGRATION,
        ClientId::Qwen => &qwen::INTEGRATION,
        ClientId::RooCode => &roocode::INTEGRATION,
        ClientId::Mux => &mux::INTEGRATION,
        ClientId::Kilo => &kilo::INTEGRATION,
        ClientId::Hermes => &hermes::INTEGRATION,
        ClientId::Copilot => &copilot::INTEGRATION,
        ClientId::Goose => &goose::INTEGRATION,
        ClientId::Codebuff => &codebuff::INTEGRATION,
        ClientId::CodeBuddy => &codebuddy::INTEGRATION,
        ClientId::Antigravity => &antigravity::INTEGRATION,
        ClientId::Zed => &zed::INTEGRATION,
        ClientId::Zcode => &zcode::INTEGRATION,
        ClientId::Kiro => &kiro::INTEGRATION,
        ClientId::Junie => &junie::INTEGRATION,
        ClientId::Warp => &warp::INTEGRATION,
        ClientId::Cline => &cline::INTEGRATION,
        ClientId::CommandCode => &commandcode::INTEGRATION,
        ClientId::Grok => &grok::INTEGRATION,
        ClientId::Devin => &devin::INTEGRATION,
    }
}

pub(crate) fn selected_integrations(
    clients: &ClientUniverse,
) -> Vec<&'static dyn ClientIntegration> {
    clients.iter().map(integration_for).collect()
}

pub(crate) struct PreparedIntegrationInputs {
    pub integration: &'static dyn ClientIntegration,
    pub units: Vec<InputUnit>,
}

pub(crate) struct ConfirmedIntegrationInputs {
    pub client: ClientId,
    pub unit_digests: Vec<[u8; 32]>,
    pub present_files: Vec<(InputFileIdentity, u64)>,
}

pub(crate) struct ParsedBatchInput<'a> {
    integration: &'a dyn ClientIntegration,
    units: Option<Vec<InputUnit>>,
    planned: VecDeque<PlannedInputUnit>,
    confirmed_inventory_digests: Vec<[u8; 32]>,
    confirmed_present_files: HashMap<InputFileIdentity, u64>,
    failed_health: Vec<InputHealth>,
    batch_width: usize,
}

enum PlannedInputUnit {
    Hit(ParsedUnit),
    Miss(InputUnit),
}

#[derive(Debug)]
pub(crate) enum CacheHitPlan {
    Hit(ParsedUnit),
    Miss(InputUnit),
}

enum BatchSlot {
    Hit,
    Miss,
}

impl<'a> ParsedBatchInput<'a> {
    fn new(integration: &'a dyn ClientIntegration, units: Vec<InputUnit>) -> Self {
        Self {
            integration,
            units: Some(units),
            planned: VecDeque::new(),
            confirmed_inventory_digests: Vec::new(),
            confirmed_present_files: HashMap::new(),
            failed_health: Vec::new(),
            batch_width: rayon::current_num_threads().max(1),
        }
    }

    fn next(
        &mut self,
        ctx: &FoldContext<'_>,
    ) -> Result<Option<Vec<ParsedUnit>>, InputPipelineError> {
        self.plan_remaining_units(ctx)?;
        if self.planned.is_empty() {
            return Ok(None);
        }

        let mut slots = Vec::new();
        let mut hit_units = VecDeque::new();
        let mut miss_units = Vec::new();
        while let Some(next) = self.planned.front() {
            if matches!(next, PlannedInputUnit::Miss(_)) && miss_units.len() == self.batch_width {
                break;
            }

            let next = self.planned.pop_front().ok_or_else(|| {
                InputPipelineError::contract("planned input disappeared before batching")
            })?;
            match next {
                PlannedInputUnit::Hit(parsed) => {
                    hit_units.push_back(parsed);
                    slots.push(BatchSlot::Hit);
                }
                PlannedInputUnit::Miss(unit) => {
                    miss_units.push(unit);
                    slots.push(BatchSlot::Miss);
                }
            }
        }

        let parsed_misses = if miss_units.is_empty() {
            Vec::new()
        } else {
            self.integration.parse_checked(
                miss_units,
                &ParseContext {
                    pricing: ctx.pricing,
                },
            )
        };
        let mut parsed_misses = parsed_misses.into_iter();
        let mut parsed = Vec::with_capacity(slots.len());
        for slot in slots {
            let unit = match slot {
                BatchSlot::Hit => hit_units
                    .pop_front()
                    .ok_or_else(|| InputPipelineError::contract("planned cache hit disappeared"))?,
                BatchSlot::Miss => parsed_misses.next().ok_or_else(|| {
                    InputPipelineError::contract(
                        "adapter returned fewer parsed units than input misses",
                    )
                })?,
            };
            parsed.push(unit);
        }
        if !hit_units.is_empty() {
            return Err(InputPipelineError::contract(
                "planned cache-hit count did not match batch slots",
            ));
        }
        if parsed_misses.next().is_some() {
            return Err(InputPipelineError::contract(
                "adapter returned more parsed units than input misses",
            ));
        }
        Ok(Some(parsed))
    }

    fn take_all_planned_units(
        &mut self,
        ctx: &FoldContext<'_>,
    ) -> Result<Vec<CacheHitPlan>, InputPipelineError> {
        self.plan_remaining_units(ctx)?;
        Ok(self
            .planned
            .drain(..)
            .map(|planned| match planned {
                PlannedInputUnit::Hit(parsed) => CacheHitPlan::Hit(parsed),
                PlannedInputUnit::Miss(unit) => CacheHitPlan::Miss(unit),
            })
            .collect())
    }

    fn batch_width(&self) -> usize {
        self.batch_width
    }

    fn take_failed_health(&mut self) -> Vec<InputHealth> {
        std::mem::take(&mut self.failed_health)
    }

    fn plan_remaining_units(&mut self, ctx: &FoldContext<'_>) -> Result<(), InputPipelineError> {
        let Some(units) = self.units.take() else {
            return Ok(());
        };
        // A unit whose input snapshot fails is isolated as unavailable. A
        // cache-planning failure belongs to tokscale's cache infrastructure
        // and must remain a pipeline error instead of being attributed to
        // third-party input data.
        #[allow(clippy::large_enum_variant)] // transient per-unit planning slot
        enum PlannedOrFailed {
            Planned(PlannedInputUnit, [u8; 32], Vec<(InputFileIdentity, u64)>),
            Failed(InputHealth),
            PipelineError(InputPlanningError),
        }
        let planned: Vec<PlannedOrFailed> = units
            .into_par_iter()
            .map(|mut unit| {
                let path = unit.path.clone();
                if let Err(source) = unit.revalidate_snapshot_for_cache_decision() {
                    return PlannedOrFailed::Failed(InputHealth {
                        client: self.integration.client(),
                        path,
                        status: InputStatus::Unavailable {
                            failure: InputFailure::new(
                                "snapshot input metadata for cache planning",
                                source.to_string(),
                            ),
                        },
                        rejections: RejectionSummary::default(),
                    });
                }
                let inventory_digest = unit.inventory_signature_digest();
                let mut present_files = Vec::new();
                unit.prepared_input_snapshot()
                    .expect("revalidated input unit must retain its confirmed snapshot")
                    .visit_present_files(|identity, size| present_files.push((identity, size)));
                match self.integration.plan_cache_hit(unit, &*ctx.input_cache) {
                    Ok(plan) => {
                        let planned = match plan {
                            CacheHitPlan::Hit(parsed) => PlannedInputUnit::Hit(parsed),
                            CacheHitPlan::Miss(unit) => PlannedInputUnit::Miss(unit),
                        };
                        PlannedOrFailed::Planned(planned, inventory_digest, present_files)
                    }
                    Err(error) => PlannedOrFailed::PipelineError(error),
                }
            })
            .collect();
        for outcome in planned {
            match outcome {
                PlannedOrFailed::Planned(planned, digest, present_files) => {
                    self.confirmed_inventory_digests.push(digest);
                    for (identity, size) in present_files {
                        self.confirmed_present_files.entry(identity).or_insert(size);
                    }
                    self.planned.push_back(planned);
                }
                PlannedOrFailed::Failed(health) => self.failed_health.push(health),
                PlannedOrFailed::PipelineError(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

pub(crate) fn run_prepared_integrations(
    prepared: Vec<PreparedIntegrationInputs>,
    input_cache: &mut message_cache::InputMessageCache,
    pricing: Option<&pricing::PricingService>,
    sink: &mut dyn UnifiedMessageSink,
    health: &mut DataHealth,
) -> Result<Vec<ConfirmedIntegrationInputs>, InputPipelineError> {
    let mut confirmed = Vec::with_capacity(prepared.len());
    for PreparedIntegrationInputs { integration, units } in prepared {
        let mut batches = ParsedBatchInput::new(integration, units);
        let mut fold_ctx = FoldContext::new(integration, input_cache, pricing);
        let mut bound_sink = BoundMessageSink::new(integration, sink);
        integration.fold_batches(&mut batches, &mut fold_ctx, &mut bound_sink)?;
        health.merge(fold_ctx.take_health());
        for failed in batches.take_failed_health() {
            health.record(failed);
        }
        confirmed.push(ConfirmedIntegrationInputs {
            client: integration.client(),
            unit_digests: batches.confirmed_inventory_digests,
            present_files: batches.confirmed_present_files.into_iter().collect(),
        });
    }
    Ok(confirmed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};

    use super::*;

    struct RecordingAdapter {
        batch_sizes: Mutex<Vec<usize>>,
    }

    struct BatchLifetimeAdapter {
        previous_batch_message: Mutex<Option<Weak<str>>>,
    }

    struct PlannedWeaveAdapter {
        parse_batch_sizes: Mutex<Vec<usize>>,
        planner_calls: AtomicUsize,
    }

    fn amp_decoder() -> DecoderSpec {
        DecoderSpec::plain(DecoderId::Amp, 0)
    }

    #[test]
    fn client_integration_dispatch_preserves_catalog_identity() {
        let integrations: Vec<ClientId> = ClientId::iter()
            .map(|client| integration_for(client).client())
            .collect();
        let unique: HashSet<ClientId> = integrations.iter().copied().collect();
        let catalog: HashSet<ClientId> = ClientId::iter().collect();

        assert_eq!(
            integrations.len(),
            unique.len(),
            "each catalog client must have exactly one local integration"
        );
        assert_eq!(
            unique, catalog,
            "catalog and integration dispatch must preserve the same clients"
        );
    }

    #[test]
    fn bound_sink_attributes_messages_with_the_registry_client() {
        let integration = integration_for(ClientId::Amp);
        let mut messages = Vec::new();
        let mut sink = BoundMessageSink::new(integration, &mut messages);

        sink.emit(ParsedMessage::new(
            "model",
            "provider",
            "session",
            1,
            crate::TokenBreakdown::default(),
            0.0,
        ));

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].client, ClientId::Amp);
    }

    impl ClientIntegration for RecordingAdapter {
        fn client(&self) -> ClientId {
            ClientId::Amp
        }

        fn discover_checked(
            &self,
            _ctx: &DiscoveryContext<'_>,
        ) -> Result<Vec<InputUnit>, InputDiscoveryError> {
            unreachable!("test adapter does not discover inputs")
        }

        fn parse_checked(&self, units: Vec<InputUnit>, _ctx: &ParseContext<'_>) -> Vec<ParsedUnit> {
            self.batch_sizes.lock().unwrap().push(units.len());
            units
                .into_iter()
                .enumerate()
                .map(|(index, unit)| {
                    let message = ParsedMessage::new(
                        "model",
                        "provider",
                        unit.path.to_string_lossy(),
                        index as i64,
                        crate::TokenBreakdown::default(),
                        0.0,
                    );
                    ParsedUnit::healthy(unit, UnitMessagePayload::Fresh(vec![message]), None, false)
                })
                .collect()
        }

        fn fold(
            &self,
            parsed: Vec<ParsedUnit>,
            ctx: &mut FoldContext<'_>,
            sink: &mut BoundMessageSink<'_>,
        ) -> Result<(), InputPipelineError> {
            cache::fold_units(parsed, ctx, sink)
        }
    }

    impl ClientIntegration for BatchLifetimeAdapter {
        fn client(&self) -> ClientId {
            ClientId::Amp
        }

        fn discover_checked(
            &self,
            _ctx: &DiscoveryContext<'_>,
        ) -> Result<Vec<InputUnit>, InputDiscoveryError> {
            unreachable!("test adapter does not discover inputs")
        }

        fn parse_checked(&self, units: Vec<InputUnit>, _ctx: &ParseContext<'_>) -> Vec<ParsedUnit> {
            let mut previous = self.previous_batch_message.lock().unwrap();
            assert!(
                previous
                    .as_ref()
                    .is_none_or(|message| message.upgrade().is_none()),
                "the previous parsed batch must be folded and dropped before parsing the next"
            );

            let mut parsed = Vec::new();
            for unit in units {
                let session_id: Arc<str> = Arc::from(unit.path.to_string_lossy().into_owned());
                *previous = Some(Arc::downgrade(&session_id));
                let mut message = ParsedMessage::new(
                    "model",
                    "provider",
                    "placeholder",
                    1,
                    crate::TokenBreakdown::default(),
                    0.0,
                );
                message.session_id = session_id;
                parsed.push(ParsedUnit::healthy(
                    unit,
                    UnitMessagePayload::Fresh(vec![message]),
                    None,
                    false,
                ));
            }
            parsed
        }

        fn fold(
            &self,
            parsed: Vec<ParsedUnit>,
            ctx: &mut FoldContext<'_>,
            sink: &mut BoundMessageSink<'_>,
        ) -> Result<(), InputPipelineError> {
            cache::fold_units(parsed, ctx, sink)
        }
    }

    impl ClientIntegration for PlannedWeaveAdapter {
        fn client(&self) -> ClientId {
            ClientId::Amp
        }

        fn discover_checked(
            &self,
            _ctx: &DiscoveryContext<'_>,
        ) -> Result<Vec<InputUnit>, InputDiscoveryError> {
            unreachable!("test adapter does not discover inputs")
        }

        fn parse_checked(&self, units: Vec<InputUnit>, _ctx: &ParseContext<'_>) -> Vec<ParsedUnit> {
            self.parse_batch_sizes.lock().unwrap().push(units.len());
            units
                .into_iter()
                .map(|unit| {
                    let message = ParsedMessage::new(
                        "model",
                        "provider",
                        unit.path.file_name().unwrap().to_string_lossy(),
                        1,
                        crate::TokenBreakdown {
                            input: 1,
                            ..Default::default()
                        },
                        0.0,
                    );
                    ParsedUnit::healthy(unit, UnitMessagePayload::Fresh(vec![message]), None, false)
                })
                .collect()
        }

        fn plan_cache_hit(
            &self,
            unit: InputUnit,
            input_cache: &message_cache::InputMessageCache,
        ) -> Result<CacheHitPlan, InputPlanningError> {
            self.planner_calls.fetch_add(1, Ordering::Relaxed);
            cache::plan_cache_hit(unit, input_cache)
        }

        fn fold(
            &self,
            parsed: Vec<ParsedUnit>,
            ctx: &mut FoldContext<'_>,
            sink: &mut BoundMessageSink<'_>,
        ) -> Result<(), InputPipelineError> {
            cache::fold_units(parsed, ctx, sink)
        }
    }

    struct DroppingSink;

    impl UnifiedMessageSink for DroppingSink {
        fn push_message(&mut self, _message: UnifiedMessage) {}
    }

    #[test]
    fn bounded_batches_use_rayon_width_and_preserve_unit_order() {
        for (thread_count, expected_batch_sizes) in [(1, vec![1; 7]), (3, vec![3, 3, 1])] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(thread_count)
                .build()
                .unwrap();
            let adapter = RecordingAdapter {
                batch_sizes: Mutex::new(Vec::new()),
            };
            let dir = tempfile::TempDir::new().unwrap();
            let input_paths: Vec<_> = (0..7)
                .map(|index| {
                    let path = dir.path().join(index.to_string());
                    std::fs::write(&path, format!("input {index}")).unwrap();
                    path
                })
                .collect();
            let units = input_paths
                .iter()
                .cloned()
                .map(|path| InputUnit::plain_file(path, amp_decoder()))
                .collect();

            let sessions = pool.install(|| {
                let mut cache = message_cache::InputMessageCache::default();
                let mut sink = Vec::new();
                let integration = &adapter;
                let mut batches = ParsedBatchInput::new(integration, units);
                let mut fold_ctx = FoldContext::new(integration, &mut cache, None);
                let mut bound_sink = BoundMessageSink::new(integration, &mut sink);
                adapter
                    .fold_batches(&mut batches, &mut fold_ctx, &mut bound_sink)
                    .unwrap();
                sink.into_iter()
                    .map(|message| message.session_id.to_string())
                    .collect::<Vec<_>>()
            });

            assert_eq!(*adapter.batch_sizes.lock().unwrap(), expected_batch_sizes);
            assert_eq!(
                sessions,
                input_paths
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn parsed_batch_is_dropped_before_the_next_batch_is_parsed() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
            .install(|| {
                let adapter = BatchLifetimeAdapter {
                    previous_batch_message: Mutex::new(None),
                };
                let dir = tempfile::TempDir::new().unwrap();
                let units = (0..5)
                    .map(|index| {
                        let path = dir.path().join(index.to_string());
                        std::fs::write(&path, format!("input {index}")).unwrap();
                        InputUnit::plain_file(path, amp_decoder())
                    })
                    .collect();
                let mut cache = message_cache::InputMessageCache::default();
                let integration = &adapter;
                let mut batches = ParsedBatchInput::new(integration, units);
                let mut fold_ctx = FoldContext::new(integration, &mut cache, None);
                let mut dropping_sink = DroppingSink;
                let mut bound_sink = BoundMessageSink::new(integration, &mut dropping_sink);
                adapter
                    .fold_batches(&mut batches, &mut fold_ctx, &mut bound_sink)
                    .unwrap();
            });
    }

    #[test]
    fn one_planning_pass_weaves_hits_with_bounded_misses_in_input_order() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
            .install(|| {
                let dir = tempfile::TempDir::new().unwrap();
                let units: Vec<_> = (0..6)
                    .map(|index| {
                        let path = dir.path().join(index.to_string());
                        std::fs::write(&path, format!("input {index}")).unwrap();
                        InputUnit::plain_file(path, amp_decoder())
                            .prepare_snapshot()
                            .unwrap()
                    })
                    .collect();
                let adapter = PlannedWeaveAdapter {
                    parse_batch_sizes: Mutex::new(Vec::new()),
                    planner_calls: AtomicUsize::new(0),
                };
                let mut cache = message_cache::InputMessageCache::default();
                for index in [0, 2, 4] {
                    let unit = &units[index];
                    cache.insert(message_cache::CachedInputEntry::new_with_version(
                        &unit.path,
                        unit.decoder.version(),
                        unit.input_policy().fingerprint().unwrap(),
                        vec![ParsedMessage::new(
                            "model",
                            "provider",
                            index.to_string(),
                            1,
                            crate::TokenBreakdown {
                                input: 1,
                                ..Default::default()
                            },
                            0.0,
                        )],
                        None,
                    ));
                }
                let mut sink = Vec::new();
                let integration = &adapter;
                let mut batches = ParsedBatchInput::new(integration, units);
                let mut fold_ctx = FoldContext::new(integration, &mut cache, None);
                let mut bound_sink = BoundMessageSink::new(integration, &mut sink);
                adapter
                    .fold_batches(&mut batches, &mut fold_ctx, &mut bound_sink)
                    .unwrap();

                assert_eq!(adapter.planner_calls.load(Ordering::Relaxed), 6);
                assert_eq!(*adapter.parse_batch_sizes.lock().unwrap(), [2, 1]);
                assert_eq!(
                    sink.into_iter()
                        .map(|message| message.session_id.to_string())
                        .collect::<Vec<_>>(),
                    ["0", "1", "2", "3", "4", "5"]
                );
            });
    }

    #[test]
    fn all_planned_cache_hits_skip_adapter_parse() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
            .install(|| {
                let dir = tempfile::TempDir::new().unwrap();
                let units: Vec<_> = (0..4)
                    .map(|index| {
                        let path = dir.path().join(index.to_string());
                        std::fs::write(&path, format!("input {index}")).unwrap();
                        InputUnit::plain_file(path, amp_decoder())
                            .prepare_snapshot()
                            .unwrap()
                    })
                    .collect();
                let adapter = PlannedWeaveAdapter {
                    parse_batch_sizes: Mutex::new(Vec::new()),
                    planner_calls: AtomicUsize::new(0),
                };
                let mut cache = message_cache::InputMessageCache::default();
                for (index, unit) in units.iter().enumerate() {
                    cache.insert(message_cache::CachedInputEntry::new_with_version(
                        &unit.path,
                        unit.decoder.version(),
                        unit.input_policy().fingerprint().unwrap(),
                        vec![ParsedMessage::new(
                            "model",
                            "provider",
                            index.to_string(),
                            1,
                            crate::TokenBreakdown {
                                input: 1,
                                ..Default::default()
                            },
                            0.0,
                        )],
                        None,
                    ));
                    message_cache::reset_input_read_stats(&unit.path);
                }
                let mut sink = Vec::new();
                let integration = &adapter;
                let mut batches = ParsedBatchInput::new(integration, units);
                let mut fold_ctx = FoldContext::new(integration, &mut cache, None);
                let mut bound_sink = BoundMessageSink::new(integration, &mut sink);
                adapter
                    .fold_batches(&mut batches, &mut fold_ctx, &mut bound_sink)
                    .unwrap();

                assert_eq!(adapter.planner_calls.load(Ordering::Relaxed), 4);
                assert!(adapter.parse_batch_sizes.lock().unwrap().is_empty());
                assert_eq!(
                    sink.into_iter()
                        .map(|message| message.session_id.to_string())
                        .collect::<Vec<_>>(),
                    ["0", "1", "2", "3"]
                );
            });
    }

    #[test]
    fn future_cache_format_reparses_across_batch_planning() {
        let input_dir = tempfile::TempDir::new().unwrap();
        let cache_dir = tempfile::TempDir::new().unwrap();
        let path = input_dir.path().join("future-cache-input");
        std::fs::write(&path, b"input").unwrap();
        let unit = InputUnit::plain_file(path.clone(), amp_decoder())
            .prepare_snapshot()
            .unwrap();
        let adapter = PlannedWeaveAdapter {
            parse_batch_sizes: Mutex::new(Vec::new()),
            planner_calls: AtomicUsize::new(0),
        };
        let mut seeded = message_cache::InputMessageCache::with_cache_dir(cache_dir.path());
        seeded.insert(message_cache::CachedInputEntry::new_with_version(
            &path,
            unit.decoder.version(),
            unit.input_policy().fingerprint().unwrap(),
            vec![ParsedMessage::new(
                "model",
                "provider",
                "cached-session",
                1,
                crate::TokenBreakdown::default(),
                0.0,
            )],
            None,
        ));
        seeded.save_if_dirty().unwrap();
        message_cache::mark_current_key_shard_as_future_format_for_test(
            cache_dir.path(),
            &path,
            unit.decoder.version(),
        );

        let mut cache = message_cache::InputMessageCache::with_cache_dir(cache_dir.path());
        let integration = &adapter;
        let mut batches = ParsedBatchInput::new(integration, vec![unit]);
        let mut fold_ctx = FoldContext::new(integration, &mut cache, None);
        let mut dropping_sink = DroppingSink;
        let mut bound_sink = BoundMessageSink::new(integration, &mut dropping_sink);
        adapter
            .fold_batches(&mut batches, &mut fold_ctx, &mut bound_sink)
            .expect("a newer cache shard must be discarded and reparsed");
    }

    #[test]
    fn direct_parse_adapter_ignores_seeded_input_shard() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("direct-input");
        std::fs::write(&path, b"direct input").unwrap();
        let unit = InputUnit::plain_file(path.clone(), amp_decoder())
            .prepare_snapshot()
            .unwrap();
        let mut cache = message_cache::InputMessageCache::default();
        cache.insert(message_cache::CachedInputEntry::new_with_version(
            &path,
            unit.decoder.version(),
            unit.input_policy().fingerprint().unwrap(),
            vec![ParsedMessage::new(
                "model",
                "provider",
                "cached-session",
                1,
                crate::TokenBreakdown {
                    input: 1,
                    ..Default::default()
                },
                0.0,
            )],
            None,
        ));
        let adapter = RecordingAdapter {
            batch_sizes: Mutex::new(Vec::new()),
        };
        let mut sink = Vec::new();
        let integration = &adapter;
        let mut batches = ParsedBatchInput::new(integration, vec![unit]);
        let mut fold_ctx = FoldContext::new(integration, &mut cache, None);
        let mut bound_sink = BoundMessageSink::new(integration, &mut sink);

        adapter
            .fold_batches(&mut batches, &mut fold_ctx, &mut bound_sink)
            .unwrap();

        assert_eq!(*adapter.batch_sizes.lock().unwrap(), [1]);
        assert_eq!(sink.len(), 1);
        assert_ne!(sink[0].session_id.as_ref(), "cached-session");
    }

    #[test]
    fn custom_batch_planning_records_confirmed_inventory_digests() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("input.jsonl");
        std::fs::write(&path, b"input").unwrap();
        let adapter = RecordingAdapter {
            batch_sizes: Mutex::new(Vec::new()),
        };
        let mut batches =
            ParsedBatchInput::new(&adapter, vec![InputUnit::plain_file(path, amp_decoder())]);
        let mut cache = message_cache::InputMessageCache::default();
        let fold_context = FoldContext::new(&adapter, &mut cache, None);

        let planned = batches.take_all_planned_units(&fold_context).unwrap();

        assert_eq!(planned.len(), 1);
        assert_eq!(batches.confirmed_inventory_digests.len(), 1);
        let CacheHitPlan::Miss(unit) = &planned[0] else {
            panic!("recording adapter must use its default cache-miss plan");
        };
        assert_eq!(
            batches.confirmed_inventory_digests[0],
            unit.inventory_signature_digest()
        );
    }

    #[test]
    fn plain_db_input_does_not_guess_a_wal_input() {
        let path = PathBuf::from("/tmp/plain-history.db");
        let unit = InputUnit::plain_file(path.clone(), amp_decoder());

        assert_eq!(unit.digest_paths(), vec![path]);
    }

    #[test]
    fn dynamic_dependency_participates_in_digest_paths_and_inventory() {
        let dir = tempfile::TempDir::new().unwrap();
        let primary = dir.path().join("child.jsonl");
        let dependency = dir.path().join("parent.jsonl");
        std::fs::write(&primary, b"child").unwrap();

        let mut unit =
            InputUnit::plain_file(primary.clone(), DecoderSpec::plain(DecoderId::Omp, 0))
                .with_dependency(dependency.clone());
        assert_eq!(
            unit.digest_paths(),
            vec![primary.clone(), dependency.clone()]
        );
        unit.refresh_prepared_snapshot_for_inventory_probe()
            .unwrap();
        let absent = unit.inventory_signature_digest();

        std::fs::write(&dependency, b"reviewer").unwrap();
        unit.refresh_prepared_snapshot_for_inventory_probe()
            .unwrap();
        let reviewer = unit.inventory_signature_digest();
        assert_ne!(absent, reviewer);

        std::fs::write(&dependency, b"oracle-agent").unwrap();
        unit.refresh_prepared_snapshot_for_inventory_probe()
            .unwrap();
        assert_ne!(reviewer, unit.inventory_signature_digest());
    }
}
