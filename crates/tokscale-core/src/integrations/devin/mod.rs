pub(crate) mod decode;

use std::collections::HashSet;

use rayon::prelude::*;

use crate::clients::ClientId;
use crate::integrations::cache as adapter_cache;
use crate::integrations::discover as adapter_discover;
use crate::integrations::{
    BoundMessageSink, ClientIntegration, DecoderSpec, DiscoveryContext, FingerprintPolicy,
    FoldContext, InputDiscoveryError, InputUnit, ParseContext, ParsedBatchInput, ParsedUnit,
    SourceSpec,
};
use crate::message_cache::DecoderId;
#[cfg(test)]
use crate::message_cache::DecoderVersion;

const DEVIN_RECORD_REJECTION_REVISION: u32 = 1;
const SOURCE: SourceSpec = SourceSpec::local_share("devin/cli/sessions.db", "sessions.db");

pub(crate) struct Integration;

impl ClientIntegration for Integration {
    fn client(&self) -> ClientId {
        ClientId::Devin
    }

    fn discover_checked(
        &self,
        ctx: &DiscoveryContext<'_>,
    ) -> Result<Vec<InputUnit>, InputDiscoveryError> {
        let client = self.client();
        let mut paths = Vec::new();

        adapter_discover::push_existing_file(client, SOURCE.resolve(ctx.home_dir), &mut paths)?;
        paths.extend(adapter_discover::scan_roots(
            client,
            adapter_discover::extra_roots_for_client(client, ctx)?,
            SOURCE.pattern(),
        )?);

        let units = adapter_discover::input_units_from_paths_preserving_order(
            client,
            paths,
            FingerprintPolicy::SqliteWithWal,
            DecoderSpec::plain(DecoderId::Devin, DEVIN_RECORD_REJECTION_REVISION),
        )?;
        Ok(units)
    }

    fn parse_checked(&self, units: Vec<InputUnit>, ctx: &ParseContext<'_>) -> Vec<ParsedUnit> {
        units
            .into_par_iter()
            .map(|unit| adapter_cache::parse_uncached_unit(unit, ctx, decode::parse_devin_sqlite))
            .collect()
    }

    fn fold(
        &self,
        parsed: Vec<ParsedUnit>,
        ctx: &mut FoldContext<'_>,
        sink: &mut BoundMessageSink<'_>,
    ) -> Result<(), crate::integrations::InputPipelineError> {
        let mut seen = HashSet::new();
        fold_devin_units(parsed, ctx, sink, &mut seen)
    }

    fn fold_batches(
        &self,
        batches: &mut ParsedBatchInput<'_>,
        ctx: &mut FoldContext<'_>,
        sink: &mut BoundMessageSink<'_>,
    ) -> Result<(), crate::integrations::InputPipelineError> {
        let mut seen = HashSet::new();
        while let Some(parsed) = batches.next(ctx)? {
            fold_devin_units(parsed, ctx, sink, &mut seen)?;
        }
        Ok(())
    }
}

fn fold_devin_units(
    parsed: Vec<ParsedUnit>,
    ctx: &mut FoldContext<'_>,
    sink: &mut BoundMessageSink<'_>,
    seen: &mut HashSet<u64>,
) -> Result<(), crate::integrations::InputPipelineError> {
    adapter_cache::fold_units_with_filter(parsed, ctx, sink, |_, messages| {
        messages
            .into_iter()
            .filter(|message| crate::should_keep_deduped_message(seen, message))
            .collect()
    })
}

pub(crate) static INTEGRATION: Integration = Integration;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn devin_adapter_discovers_default_then_extra_session_dbs() {
        let home = tempfile::TempDir::new().unwrap();
        let default_db = home.path().join(".local/share/devin/cli/sessions.db");
        let extra_root = home.path().join("devin-profiles");
        let profile_db = extra_root.join("profile-a/sessions.db");
        for path in [&default_db, &profile_db] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "").unwrap();
        }
        let mut extra_scan_paths = BTreeMap::new();
        extra_scan_paths.insert("devin".to_string(), vec![extra_root]);
        let settings = crate::scanner::ScannerSettings {
            extra_scan_paths,
            ..Default::default()
        };
        let ctx = DiscoveryContext {
            home_dir: home.path(),
            scanner_settings: &settings,
        };

        let units = INTEGRATION.discover_checked(&ctx).unwrap();
        let paths: Vec<_> = units.iter().map(|unit| unit.path.clone()).collect();

        assert_eq!(paths, vec![default_db, profile_db]);
        assert!(units.iter().all(|unit| {
            unit.decoder.version()
                == DecoderVersion::new(DecoderId::Devin, DEVIN_RECORD_REJECTION_REVISION)
        }));
        assert!(units
            .iter()
            .all(|unit| unit.fingerprint_policy == FingerprintPolicy::SqliteWithWal));
    }
}
