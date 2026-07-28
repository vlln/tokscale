use crate::integrations::codex::decode::CodexParseState;
use crate::records::ParsedMessage;
use bincode::Options;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
#[cfg(test)]
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::UNIX_EPOCH;

#[cfg(not(any(unix, windows)))]
compile_error!("input-message cache requires stable Unix or Windows file identity");

// Input-message cache shards split serialization layout from decoder/input
// semantics. Bump this only when the shard bincode layout changes; decoder-only
// fixes should bump the relevant InputUnit decoder revision instead.
const CACHE_FORMAT_VERSION: u32 = 12;
#[cfg(test)]
const UNSUPPORTED_CACHE_FORMAT_VERSION: u32 = CACHE_FORMAT_VERSION - 1;
const SHARD_MAGIC: [u8; 8] = *b"TOKSHRD\0";
const SHARD_KEY_FORMAT_VERSION: u32 = 1;
const SHARDS_DIRNAME: &str = "shards";
const MAX_CACHE_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_SHARD_HEADER_BYTES: u64 = 16 * 1024 * 1024;
const HASH_BUFFER_BYTES: usize = 64 * 1024;

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct InputReadStats {
    pub bytes: u64,
    pub hash_passes: u64,
}

#[cfg(test)]
fn input_read_stats() -> &'static std::sync::Mutex<HashMap<PathBuf, InputReadStats>> {
    static STATS: std::sync::OnceLock<std::sync::Mutex<HashMap<PathBuf, InputReadStats>>> =
        std::sync::OnceLock::new();
    STATS.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn reset_input_read_stats(path: &Path) {
    input_read_stats().lock().unwrap().remove(path);
}

#[cfg(test)]
pub(crate) fn get_input_read_stats(path: &Path) -> InputReadStats {
    input_read_stats()
        .lock()
        .unwrap()
        .get(path)
        .copied()
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn record_input_hash_start(path: &Path) {
    input_read_stats()
        .lock()
        .unwrap()
        .entry(path.to_path_buf())
        .or_default()
        .hash_passes += 1;
}

#[cfg(test)]
pub(crate) fn record_input_bytes(path: &Path, bytes: usize) {
    input_read_stats()
        .lock()
        .unwrap()
        .entry(path.to_path_buf())
        .or_default()
        .bytes += bytes as u64;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InputCachePruneStats {
    pub scanned: usize,
    pub removed: usize,
    pub retained: usize,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InputCacheError {
    #[cfg(test)]
    #[error("input cache directory is unavailable: {source}")]
    CacheDirectoryUnavailable {
        #[source]
        source: crate::paths::ConfigDirUnavailable,
    },
    #[error("failed to {operation} `{path}`: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl InputCacheError {
    fn io(operation: &'static str, path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            operation,
            path: path.to_path_buf(),
            source,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum InputSnapshotError {
    #[error("failed to {operation} `{path}`: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read modification time for `{path}`: {source}")]
    ModifiedTime {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("modification time for `{path}` predates the Unix epoch: {source}")]
    ModifiedBeforeEpoch {
        path: PathBuf,
        #[source]
        source: std::time::SystemTimeError,
    },
    #[error("modification time for `{path}` exceeds the supported nanosecond range")]
    ModifiedTimeOutOfRange { path: PathBuf },
    #[error("scan input `{path}` is not a regular file")]
    NotARegularFile { path: PathBuf },
    #[error("invalid input snapshot for `{path}`: {detail}")]
    InvalidSnapshot { path: PathBuf, detail: String },
    #[error("input fingerprint has no primary input")]
    MissingPrimaryInput,
    #[error("optional related scan input `{path}` is unavailable: {failure}")]
    OptionalRelatedInputUnavailable { path: PathBuf, failure: String },
}

impl InputSnapshotError {
    fn io(operation: &'static str, path: &Path, source: std::io::Error) -> InputSnapshotError {
        Self::Io {
            operation,
            path: path.to_path_buf(),
            source,
        }
    }

    fn invalid(path: &Path, detail: impl Into<String>) -> InputSnapshotError {
        Self::InvalidSnapshot {
            path: path.to_path_buf(),
            detail: detail.into(),
        }
    }

    fn optional_related_input_unavailable(
        path: &Path,
        source: &InputSnapshotError,
    ) -> InputSnapshotError {
        Self::OptionalRelatedInputUnavailable {
            path: path.to_path_buf(),
            failure: source.to_string(),
        }
    }

    pub(crate) fn is_optional_related_input_unavailable(&self) -> bool {
        matches!(self, Self::OptionalRelatedInputUnavailable { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelatedInputFailurePolicy {
    FailInput,
    PreservePrimary,
}

#[derive(Debug, thiserror::Error)]
pub enum InputCachePruneError {
    #[error("input cache directory is unavailable: {source}")]
    CacheDirectoryUnavailable {
        #[source]
        source: crate::paths::ConfigDirUnavailable,
    },
    #[error("failed to {operation} `{path}`: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to {operation} `{path}` for shard format v{format_version}: {source}")]
    CurrentFormatIo {
        operation: &'static str,
        path: PathBuf,
        format_version: u32,
        #[source]
        source: std::io::Error,
    },
    #[error("input cache shard `{path}` is {actual} bytes; limit is {limit} bytes")]
    TooLarge {
        path: PathBuf,
        actual: u64,
        limit: u64,
    },
    #[error("input cache shard `{path}` has unrecognized magic {actual:?}")]
    UnknownMagic { path: PathBuf, actual: [u8; 8] },
    #[error(
        "input cache shard `{path}` has unsupported format version {actual}; current format is {current}"
    )]
    UnsupportedFormat {
        path: PathBuf,
        actual: u32,
        current: u32,
    },
    #[error("input cache shard `{path}` has invalid v{format_version} header length {actual}")]
    InvalidHeaderLength {
        path: PathBuf,
        format_version: u32,
        actual: u64,
    },
    #[error("failed to decode input cache shard `{path}` v{format_version} header: {source}")]
    Decode {
        path: PathBuf,
        format_version: u32,
        #[source]
        source: bincode::Error,
    },
}

impl InputCachePruneError {
    fn io(operation: &'static str, path: &Path, source: std::io::Error) -> Self {
        Self::Io {
            operation,
            path: path.to_path_buf(),
            source,
        }
    }

    fn current_format_io(
        operation: &'static str,
        path: &Path,
        format_version: u32,
        source: std::io::Error,
    ) -> Self {
        Self::CurrentFormatIo {
            operation,
            path: path.to_path_buf(),
            format_version,
            source,
        }
    }
}

pub(crate) type DecoderRevision = u32;

macro_rules! define_decoder_ids {
    ($($variant:ident => ($stable_name:literal, $plain:literal)),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub(crate) enum DecoderId {
            $($variant),+
        }

        impl DecoderId {
            pub(crate) const fn stable_name(self) -> &'static str {
                match self {
                    $(Self::$variant => $stable_name),+
                }
            }

            pub(crate) fn from_stable_name(stable_name: &str) -> Option<Self> {
                match stable_name {
                    $($stable_name => Some(Self::$variant),)+
                    _ => None,
                }
            }

            pub(crate) const fn supports_plain_route(self) -> bool {
                match self {
                    $(Self::$variant => $plain),+
                }
            }
        }

        impl Serialize for DecoderId {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(self.stable_name())
            }
        }

        impl<'de> Deserialize<'de> for DecoderId {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let stable_name = String::deserialize(deserializer)?;
                Self::from_stable_name(&stable_name).ok_or_else(|| {
                    serde::de::Error::unknown_variant(
                        &stable_name,
                        &[$($stable_name),+],
                    )
                })
            }
        }
    };
}

define_decoder_ids! {
    OpenCodeSqlite => ("opencode-sqlite", false),
    Claude => ("claude", true),
    Codex => ("codex", false),
    Gemini => ("gemini", true),
    Amp => ("amp", true),
    Droid => ("droid", true),
    OpenClaw => ("openclaw", true),
    Pi => ("pi", true),
    Omp => ("omp", true),
    Kimi => ("kimi", true),
    Qwen => ("qwen", true),
    RooCode => ("roo-code", true),
    Mux => ("mux", true),
    Kilo => ("kilo", true),
    Hermes => ("hermes", true),
    Copilot => ("copilot", true),
    Goose => ("goose", true),
    Codebuff => ("codebuff", true),
    AntigravityCliSqlite => ("antigravity-cli-sqlite", false),
    Zed => ("zed", true),
    Kiro => ("kiro", true),
    KiroFile => ("kiro-file", false),
    KiroSqlite => ("kiro-sqlite", false),
    KiroGlobalStorage => ("kiro-global-storage", false),
    Junie => ("junie", true),
    Cline => ("cline", true),
    CommandCode => ("command-code", true),
    Grok => ("grok", true),
    Zcode => ("zcode", true),
    Warp => ("warp", true),
    CodeBuddy => ("codebuddy", false),
    Devin => ("devin", true),
    OmpParentHealth => ("omp-parent-health", true),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct DecoderVersion {
    pub decoder_id: DecoderId,
    pub revision: DecoderRevision,
}

impl DecoderVersion {
    pub(crate) const fn new(decoder_id: DecoderId, revision: DecoderRevision) -> Self {
        Self {
            decoder_id,
            revision,
        }
    }
}

fn cache_dir() -> Result<PathBuf, crate::paths::ConfigDirUnavailable> {
    crate::paths::try_get_cache_dir()
}

fn ensure_cache_dir(dir: &Path) -> std::io::Result<()> {
    match fs::symlink_metadata(dir) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
                return Err(std::io::Error::other(
                    "cache directory is not a real directory",
                ));
            }
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => return Err(source),
    }
    fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn initialize_input_shards(cache_dir: &Path) -> std::io::Result<()> {
    ensure_cache_dir(cache_dir)?;
    ensure_cache_dir(&cache_dir.join(SHARDS_DIRNAME))
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct CachedPath(Vec<u8>);

#[cfg(unix)]
impl CachedPath {
    pub(crate) fn from_path(path: &Path) -> Self {
        use std::os::unix::ffi::OsStrExt;

        Self(path.as_os_str().as_bytes().to_vec())
    }

    pub(crate) fn to_path_buf(&self) -> PathBuf {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        PathBuf::from(OsString::from_vec(self.0.clone()))
    }

    fn update_shard_key(&self, hasher: &mut Sha256) {
        hasher.update(b"unix");
        hash_inventory_bytes(hasher, &self.0);
    }
}

#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct CachedPath(Vec<u16>);

#[cfg(windows)]
impl CachedPath {
    pub(crate) fn from_path(path: &Path) -> Self {
        use std::os::windows::ffi::OsStrExt;

        Self(path.as_os_str().encode_wide().collect())
    }

    pub(crate) fn to_path_buf(&self) -> PathBuf {
        use std::ffi::OsString;
        use std::os::windows::ffi::OsStringExt;

        PathBuf::from(OsString::from_wide(&self.0))
    }

    fn update_shard_key(&self, hasher: &mut Sha256) {
        hasher.update(b"windows");
        hash_inventory_len(
            hasher,
            self.0
                .len()
                .checked_mul(std::mem::size_of::<u16>())
                .expect("cached Windows path byte length exceeds usize"),
        );
        for code_unit in &self.0 {
            hasher.update(code_unit.to_le_bytes());
        }
    }
}

#[cfg(not(any(unix, windows)))]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct CachedPath(String);

#[cfg(not(any(unix, windows)))]
impl CachedPath {
    pub(crate) fn from_path(path: &Path) -> Self {
        Self(path.to_string_lossy().into_owned())
    }

    pub(crate) fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.0)
    }

    fn update_shard_key(&self, hasher: &mut Sha256) {
        hasher.update(b"other");
        hash_inventory_bytes(hasher, self.0.as_bytes());
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InputFileStamp {
    label: String,
    path: CachedPath,
    present: bool,
    size: u64,
    modified_ns: u64,
    identity: Option<InputFileIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InputStamp {
    files: Vec<InputFileStamp>,
}

impl InputStamp {
    pub(crate) fn primary_size(&self) -> Option<u64> {
        self.files
            .first()
            .filter(|file| file.present)
            .map(|file| file.size)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum InputFileIdentity {
    Unix {
        device: u64,
        inode: u64,
    },
    Windows {
        volume_serial_number: u64,
        file_index: u64,
    },
}

impl InputFileIdentity {
    fn update_inventory_signature(self, hasher: &mut Sha256) {
        match self {
            Self::Unix { device, inode } => {
                hasher.update([1]);
                hasher.update(device.to_le_bytes());
                hasher.update(inode.to_le_bytes());
            }
            Self::Windows {
                volume_serial_number,
                file_index,
            } => {
                hasher.update([2]);
                hasher.update(volume_serial_number.to_le_bytes());
                hasher.update(file_index.to_le_bytes());
            }
        }
    }
}

#[cfg(unix)]
pub(crate) fn input_file_identity(metadata: &fs::Metadata) -> InputFileIdentity {
    use std::os::unix::fs::MetadataExt;

    InputFileIdentity::Unix {
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(windows)]
fn input_file_identity(file: &File) -> std::io::Result<InputFileIdentity> {
    let information = winapi_util::file::information(file)?;

    Ok(InputFileIdentity::Windows {
        volume_serial_number: information.volume_serial_number(),
        file_index: information.file_index(),
    })
}

#[cfg(windows)]
fn input_metadata_and_identity(path: &Path) -> std::io::Result<(fs::Metadata, InputFileIdentity)> {
    let file = File::open(path)?;
    let metadata = file.metadata()?;
    let identity = input_file_identity(&file)?;
    Ok((metadata, identity))
}

#[cfg(not(windows))]
fn input_metadata_and_identity(path: &Path) -> std::io::Result<(fs::Metadata, InputFileIdentity)> {
    let metadata = fs::metadata(path)?;
    let identity = input_file_identity(&metadata);
    Ok((metadata, identity))
}

pub(crate) fn input_file_identity_from_open_file(
    file: &File,
) -> std::io::Result<InputFileIdentity> {
    #[cfg(windows)]
    {
        input_file_identity(file)
    }
    #[cfg(not(windows))]
    {
        let metadata = file.metadata()?;
        Ok(input_file_identity(&metadata))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InputFileSnapshot {
    Present {
        size: u64,
        modified_ns: u64,
        identity: InputFileIdentity,
    },
    Absent,
    Unavailable {
        failure: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputSnapshot {
    files: Vec<InputFileSnapshot>,
}

impl InputSnapshot {
    pub(crate) fn primary_identity(&self) -> Option<InputFileIdentity> {
        match self.files.first() {
            Some(InputFileSnapshot::Present { identity, .. }) => Some(*identity),
            _ => None,
        }
    }

    pub(crate) fn primary_size(&self) -> Option<u64> {
        match self.files.first() {
            Some(InputFileSnapshot::Present { size, .. }) => Some(*size),
            _ => None,
        }
    }

    pub(crate) fn input_matches_single_file_snapshot(
        &self,
        input_index: usize,
        single_file_snapshot: &Self,
    ) -> bool {
        single_file_snapshot.files.len() == 1
            && self.files.get(input_index) == single_file_snapshot.files.first()
    }

    pub(crate) fn visit_present_files(&self, mut visit: impl FnMut(InputFileIdentity, u64)) {
        for file in &self.files {
            if let InputFileSnapshot::Present { size, identity, .. } = file {
                visit(*identity, *size);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn primary_modified_ms(&self) -> Option<i64> {
        match self.files.first() {
            Some(InputFileSnapshot::Present { modified_ns, .. }) => Some(
                i64::try_from(modified_ns / 1_000_000)
                    .expect("input mtime milliseconds exceed i64"),
            ),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InputPolicy {
    inputs: Vec<(String, PathBuf)>,
    related_failure_policy: RelatedInputFailurePolicy,
}

impl InputPolicy {
    pub(crate) fn plain(path: &Path) -> Self {
        Self::with_related(path, std::iter::empty())
    }

    pub(crate) fn sqlite_with_wal(path: &Path) -> Self {
        Self::with_related(
            path,
            [("-wal".to_string(), append_path_suffix(path, "-wal"))],
        )
    }

    pub(crate) fn with_siblings<'a, I>(path: &Path, sibling_names: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        Self::with_related(
            path,
            sibling_names
                .into_iter()
                .map(|name| (name.to_string(), parent.join(name))),
        )
    }

    pub(crate) fn with_dependency(path: &Path, dependency_path: PathBuf) -> Self {
        Self::with_related(path, [("dependency".to_string(), dependency_path)])
    }

    pub(crate) fn claude_code(path: &Path, parent_session_path: Option<PathBuf>) -> Self {
        let mut related = Vec::new();
        if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
            related.push((
                ".meta.json".to_string(),
                path.with_file_name(format!("{stem}.meta.json")),
            ));
        }
        if let Some(parent_session_path) = parent_session_path {
            related.push(("parent-session".to_string(), parent_session_path));
        }
        Self::with_related(path, related)
    }

    pub(crate) fn with_related_failure_policy(mut self, policy: RelatedInputFailurePolicy) -> Self {
        self.related_failure_policy = policy;
        self
    }

    fn with_related<I>(path: &Path, related: I) -> Self
    where
        I: IntoIterator<Item = (String, PathBuf)>,
    {
        let mut inputs = vec![("primary".to_string(), path.to_path_buf())];
        let mut related: Vec<_> = related.into_iter().collect();
        related.sort_by(|left, right| left.0.cmp(&right.0));
        inputs.extend(related);
        Self {
            inputs,
            related_failure_policy: RelatedInputFailurePolicy::FailInput,
        }
    }

    #[cfg(test)]
    pub(crate) fn paths(&self) -> Vec<PathBuf> {
        self.inputs.iter().map(|(_, path)| path.clone()).collect()
    }

    pub(crate) fn update_inventory_signature(&self, snapshot: &InputSnapshot, hasher: &mut Sha256) {
        hash_inventory_len(hasher, self.inputs.len());
        for (index, (policy_label, path)) in self.inputs.iter().enumerate() {
            let file = snapshot.files.get(index);
            hash_inventory_bytes(hasher, policy_label.as_bytes());
            hash_inventory_path(hasher, path);
            match file {
                None => hasher.update([0]),
                Some(InputFileSnapshot::Present {
                    size,
                    modified_ns,
                    identity,
                }) => {
                    hasher.update([1]);
                    hasher.update(size.to_le_bytes());
                    hasher.update(modified_ns.to_le_bytes());
                    identity.update_inventory_signature(hasher);
                }
                Some(InputFileSnapshot::Absent) => hasher.update([2]),
                Some(InputFileSnapshot::Unavailable { failure }) => {
                    hasher.update([3]);
                    hash_inventory_bytes(hasher, failure.as_bytes());
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn stamp(&self) -> Result<InputStamp, InputSnapshotError> {
        let snapshot = self.snapshot()?;
        self.stamp_from_snapshot(&snapshot)
    }

    pub(crate) fn snapshot(&self) -> Result<InputSnapshot, InputSnapshotError> {
        let mut files = Vec::with_capacity(self.inputs.len());
        for (index, (_, path)) in self.inputs.iter().enumerate() {
            let file_result = match input_metadata_and_identity(path) {
                Ok((metadata, _)) if !metadata.is_file() => {
                    Err(InputSnapshotError::NotARegularFile {
                        path: path.to_path_buf(),
                    })
                }
                Ok((metadata, identity)) => {
                    modified_ns(path, &metadata).map(|modified_ns| InputFileSnapshot::Present {
                        size: metadata.len(),
                        modified_ns,
                        identity,
                    })
                }
                Err(error) if index > 0 && error.kind() == std::io::ErrorKind::NotFound => {
                    Ok(InputFileSnapshot::Absent)
                }
                Err(source) => Err(InputSnapshotError::io(
                    "read input metadata and file identity",
                    path,
                    source,
                )),
            };
            let file = match file_result {
                Ok(file) => file,
                Err(source)
                    if index > 0
                        && self.related_failure_policy
                            == RelatedInputFailurePolicy::PreservePrimary =>
                {
                    InputFileSnapshot::Unavailable {
                        failure: source.to_string(),
                    }
                }
                Err(source) => return Err(source),
            };
            files.push(file);
        }
        Ok(InputSnapshot { files })
    }

    pub(crate) fn stamp_from_snapshot(
        &self,
        snapshot: &InputSnapshot,
    ) -> Result<InputStamp, InputSnapshotError> {
        if snapshot.files.len() != self.inputs.len() {
            return Err(InputSnapshotError::invalid(
                &self.inputs[0].1,
                "file count does not match the input policy",
            ));
        }
        let files = self
            .inputs
            .iter()
            .zip(&snapshot.files)
            .map(|((label, path), snapshot)| match snapshot {
                InputFileSnapshot::Present {
                    size,
                    modified_ns,
                    identity,
                } => Ok(InputFileStamp {
                    label: label.clone(),
                    path: CachedPath::from_path(path),
                    present: true,
                    size: *size,
                    modified_ns: *modified_ns,
                    identity: Some(*identity),
                }),
                InputFileSnapshot::Absent => Ok(InputFileStamp {
                    label: label.clone(),
                    path: CachedPath::from_path(path),
                    present: false,
                    size: 0,
                    modified_ns: 0,
                    identity: None,
                }),
                InputFileSnapshot::Unavailable { failure } => {
                    Err(InputSnapshotError::OptionalRelatedInputUnavailable {
                        path: path.clone(),
                        failure: failure.clone(),
                    })
                }
            })
            .collect::<Result<_, _>>()?;
        Ok(InputStamp { files })
    }

    pub(crate) fn fingerprint_from_snapshot(
        &self,
        snapshot: &InputSnapshot,
    ) -> Result<InputFingerprint, InputSnapshotError> {
        self.fingerprint_from_stamp(self.stamp_from_snapshot(snapshot)?)
    }

    pub(crate) fn fingerprint_from_snapshot_with_primary_hash(
        &self,
        snapshot: &InputSnapshot,
        primary_hash: [u8; 32],
    ) -> Result<InputFingerprint, InputSnapshotError> {
        self.fingerprint_from_stamp_with(
            self.stamp_from_snapshot(snapshot)?,
            |index, path, size| {
                if index == 0 {
                    Ok(primary_hash)
                } else {
                    hash_prefix(path, size)
                }
            },
        )
    }

    pub(crate) fn fingerprint_from_snapshot_with_dependency_hash(
        &self,
        snapshot: &InputSnapshot,
        dependency_hash: [u8; 32],
    ) -> Result<InputFingerprint, InputSnapshotError> {
        if self.inputs.len() != 2 || self.inputs[1].0 != "dependency" {
            return Err(InputSnapshotError::invalid(
                &self.inputs[0].1,
                "precomputed dependency hash requires one dependency input",
            ));
        }
        self.fingerprint_from_stamp_with(
            self.stamp_from_snapshot(snapshot)?,
            |index, path, size| {
                if index == 1 {
                    Ok(dependency_hash)
                } else {
                    hash_prefix(path, size)
                }
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn fingerprint(&self) -> Result<InputFingerprint, InputSnapshotError> {
        let stamp = self.stamp()?;
        self.fingerprint_from_stamp(stamp)
    }

    pub(crate) fn fingerprint_from_stamp(
        &self,
        stamp: InputStamp,
    ) -> Result<InputFingerprint, InputSnapshotError> {
        self.fingerprint_from_stamp_with(stamp, |_, path, size| hash_prefix(path, size))
    }

    fn fingerprint_from_stamp_with(
        &self,
        stamp: InputStamp,
        mut hash_input: impl FnMut(usize, &Path, u64) -> Result<[u8; 32], InputSnapshotError>,
    ) -> Result<InputFingerprint, InputSnapshotError> {
        if stamp.files.len() != self.inputs.len()
            || self
                .inputs
                .iter()
                .zip(&stamp.files)
                .any(|((label, path), file)| {
                    file.label != *label || file.path != CachedPath::from_path(path)
                })
        {
            return Err(InputSnapshotError::invalid(
                &self.inputs[0].1,
                "stamp paths or labels do not match the input policy",
            ));
        }
        let size = stamp.primary_size().ok_or_else(|| {
            InputSnapshotError::invalid(&self.inputs[0].1, "primary input is absent")
        })?;
        let content_hash = hash_input(0, &self.inputs[0].1, size)?;
        let mut related_files = Vec::with_capacity(self.inputs.len().saturating_sub(1));
        for (index, ((label, path), file_stamp)) in self
            .inputs
            .iter()
            .skip(1)
            .zip(stamp.files.iter().skip(1))
            .enumerate()
        {
            let content_hash = if file_stamp.present {
                match hash_input(index + 1, path, file_stamp.size) {
                    Ok(content_hash) => Some(content_hash),
                    Err(source)
                        if self.related_failure_policy
                            == RelatedInputFailurePolicy::PreservePrimary =>
                    {
                        return Err(InputSnapshotError::optional_related_input_unavailable(
                            path, &source,
                        ));
                    }
                    Err(source) => return Err(source),
                }
            } else {
                None
            };
            related_files.push(RelatedFileFingerprint {
                label: label.clone(),
                content_hash,
            });
        }
        Ok(InputFingerprint {
            stamp,
            size,
            content_hash,
            related_files,
        })
    }
}

pub(crate) fn hash_inventory_len(hasher: &mut Sha256, len: usize) {
    hasher.update(
        u64::try_from(len)
            .expect("input inventory field length exceeds u64")
            .to_le_bytes(),
    );
}

pub(crate) fn hash_inventory_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hash_inventory_len(hasher, bytes.len());
    hasher.update(bytes);
}

#[cfg(unix)]
pub(crate) fn hash_inventory_path(hasher: &mut Sha256, path: &Path) {
    use std::os::unix::ffi::OsStrExt;

    hasher.update(b"unix");
    hash_inventory_bytes(hasher, path.as_os_str().as_bytes());
}

#[cfg(windows)]
pub(crate) fn hash_inventory_path(hasher: &mut Sha256, path: &Path) {
    use std::os::windows::ffi::OsStrExt;

    hasher.update(b"windows");
    let path_bytes: Vec<u8> = path
        .as_os_str()
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect();
    hash_inventory_bytes(hasher, &path_bytes);
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn hash_inventory_path(hasher: &mut Sha256, path: &Path) {
    hasher.update(b"other");
    hash_inventory_bytes(hasher, path.as_os_str().to_string_lossy().as_bytes());
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct InputFingerprint {
    pub stamp: InputStamp,
    pub size: u64,
    pub content_hash: [u8; 32],
    pub related_files: Vec<RelatedFileFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RelatedFileFingerprint {
    label: String,
    content_hash: Option<[u8; 32]>,
}

impl InputFingerprint {
    #[cfg(test)]
    pub(crate) fn from_path(path: &Path) -> Result<Self, InputSnapshotError> {
        InputPolicy::plain(path).fingerprint()
    }

    #[cfg(test)]
    pub(crate) fn from_sqlite_path(path: &Path) -> Result<Self, InputSnapshotError> {
        InputPolicy::sqlite_with_wal(path).fingerprint()
    }

    #[cfg(test)]
    pub(crate) fn from_path_with_siblings<'a, I>(
        path: &Path,
        sibling_names: I,
    ) -> Result<Self, InputSnapshotError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        InputPolicy::with_siblings(path, sibling_names).fingerprint()
    }

    #[cfg(test)]
    pub(crate) fn from_claude_code_path(path: &Path) -> Result<Self, InputSnapshotError> {
        InputPolicy::claude_code(path, None).fingerprint()
    }

    pub(crate) fn from_main_digest(
        stamp: InputStamp,
        content_hash: [u8; 32],
    ) -> Result<Self, InputSnapshotError> {
        let path = stamp
            .files
            .first()
            .map(|file| file.path.to_path_buf())
            .ok_or(InputSnapshotError::MissingPrimaryInput)?;
        let size = stamp
            .primary_size()
            .ok_or_else(|| InputSnapshotError::invalid(&path, "primary input is absent"))?;
        if stamp.files.len() != 1 {
            return Err(InputSnapshotError::invalid(
                &path,
                "main digest requires exactly one input file",
            ));
        }
        Ok(Self {
            stamp,
            size,
            content_hash,
            related_files: Vec::new(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CodexIncrementalCache {
    pub state: CodexParseState,
    pub consumed_offset: u64,
    pub ends_with_newline: bool,
    pub prefix_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CachedInputKey {
    path: CachedPath,
    decoder_version: DecoderVersion,
}

impl CachedInputKey {
    fn new(path: &Path, decoder_version: DecoderVersion) -> Self {
        Self {
            path: CachedPath::from_path(path),
            decoder_version,
        }
    }

    fn to_path_buf(&self) -> PathBuf {
        self.path.to_path_buf()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CacheReadPlan {
    key: CachedInputKey,
    fingerprint: InputFingerprint,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CacheReadFailureReason {
    #[error("cache shard was invalidated before its body was read")]
    Invalidated,
    #[error("cache shard body was already consumed during this scan")]
    AlreadyConsumed,
    #[error("in-memory cache fingerprint no longer matches the read plan")]
    FingerprintMismatch,
    #[error("failed to open shard: {source}")]
    Open {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to inspect shard: {source}")]
    Metadata {
        #[source]
        source: std::io::Error,
    },
    #[error("shard size {actual} exceeds the {limit}-byte limit")]
    TooLarge { actual: u64, limit: u64 },
    #[error("failed to read shard header: {source}")]
    HeaderRead {
        #[source]
        source: std::io::Error,
    },
    #[error("unrecognized shard magic {actual:?}")]
    InvalidMagic { actual: [u8; 8] },
    #[error("shard format version {actual} does not match current format {current}")]
    FormatMismatch { actual: u32, current: u32 },
    #[error("invalid shard header length {actual}")]
    InvalidHeaderLength { actual: u64 },
    #[error("failed to decode shard header: {source}")]
    HeaderDecode {
        #[source]
        source: bincode::Error,
    },
    #[error("shard input path no longer matches the read plan")]
    InputPathMismatch,
    #[error("shard decoder version no longer matches the read plan")]
    DecoderVersionMismatch,
    #[error("shard fingerprint no longer matches the read plan")]
    ShardFingerprintMismatch,
    #[error("failed to decode shard body: {source}")]
    BodyDecode {
        #[source]
        source: bincode::Error,
    },
    #[error("shard header declares {declared} messages but body contains {actual}")]
    MessageCountMismatch { declared: usize, actual: usize },
}

impl CacheReadFailureReason {
    fn preserves_shard_until_replacement(&self) -> bool {
        matches!(
            self,
            Self::Open { .. }
                | Self::Metadata { .. }
                | Self::TooLarge { .. }
                | Self::HeaderRead { .. }
                | Self::InvalidMagic { .. }
                | Self::FormatMismatch { .. }
                | Self::InvalidHeaderLength { .. }
                | Self::HeaderDecode { .. }
                | Self::InputPathMismatch
                | Self::DecoderVersionMismatch
        )
    }
}

#[derive(Debug)]
pub(crate) struct CacheReadFailure {
    pub(crate) input_path: PathBuf,
    pub(crate) decoder_version: DecoderVersion,
    pub(crate) shard_path: Option<PathBuf>,
    pub(crate) reason: CacheReadFailureReason,
}

impl CacheReadFailure {
    pub(crate) fn can_reparse_input(&self) -> bool {
        match self.reason {
            CacheReadFailureReason::Invalidated
            | CacheReadFailureReason::AlreadyConsumed
            | CacheReadFailureReason::FingerprintMismatch => false,
            CacheReadFailureReason::Open { .. }
            | CacheReadFailureReason::Metadata { .. }
            | CacheReadFailureReason::TooLarge { .. }
            | CacheReadFailureReason::HeaderRead { .. }
            | CacheReadFailureReason::InvalidMagic { .. }
            | CacheReadFailureReason::FormatMismatch { .. }
            | CacheReadFailureReason::InvalidHeaderLength { .. }
            | CacheReadFailureReason::HeaderDecode { .. }
            | CacheReadFailureReason::InputPathMismatch
            | CacheReadFailureReason::DecoderVersionMismatch
            | CacheReadFailureReason::ShardFingerprintMismatch
            | CacheReadFailureReason::BodyDecode { .. }
            | CacheReadFailureReason::MessageCountMismatch { .. } => true,
        }
    }

    pub(crate) fn requires_shard_removal(&self) -> bool {
        match &self.reason {
            CacheReadFailureReason::MessageCountMismatch { .. } => true,
            CacheReadFailureReason::BodyDecode { source } => match source.as_ref() {
                bincode::ErrorKind::Io(source) => {
                    source.kind() == std::io::ErrorKind::UnexpectedEof
                }
                _ => true,
            },
            CacheReadFailureReason::Invalidated
            | CacheReadFailureReason::AlreadyConsumed
            | CacheReadFailureReason::FingerprintMismatch
            | CacheReadFailureReason::Open { .. }
            | CacheReadFailureReason::Metadata { .. }
            | CacheReadFailureReason::TooLarge { .. }
            | CacheReadFailureReason::HeaderRead { .. }
            | CacheReadFailureReason::InvalidMagic { .. }
            | CacheReadFailureReason::FormatMismatch { .. }
            | CacheReadFailureReason::InvalidHeaderLength { .. }
            | CacheReadFailureReason::HeaderDecode { .. }
            | CacheReadFailureReason::InputPathMismatch
            | CacheReadFailureReason::DecoderVersionMismatch
            | CacheReadFailureReason::ShardFingerprintMismatch => false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CacheLookupFailure {
    pub(crate) input_path: PathBuf,
    pub(crate) decoder_version: DecoderVersion,
    pub(crate) shard_path: PathBuf,
    pub(crate) reason: CacheReadFailureReason,
}

impl std::fmt::Display for CacheLookupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "input cache v{} header read failed for `{}` with decoder {:?} at `{}`: {}",
            CACHE_FORMAT_VERSION,
            self.input_path.display(),
            self.decoder_version,
            self.shard_path.display(),
            self.reason
        )
    }
}

impl std::error::Error for CacheLookupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.reason)
    }
}

impl std::fmt::Display for CacheReadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "input cache body read failed for `{}` with decoder {:?}",
            self.input_path.display(),
            self.decoder_version
        )?;
        if let Some(shard_path) = &self.shard_path {
            write!(formatter, " at `{}`", shard_path.display())?;
        }
        write!(formatter, ": {}", self.reason)
    }
}

impl std::error::Error for CacheReadFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.reason)
    }
}

impl CacheReadFailure {
    fn new(
        plan: &CacheReadPlan,
        shard_path: Option<PathBuf>,
        reason: CacheReadFailureReason,
    ) -> Self {
        Self {
            input_path: plan.path(),
            decoder_version: plan.decoder_version(),
            shard_path,
            reason,
        }
    }
}

impl CacheReadPlan {
    pub(crate) fn new(
        path: &Path,
        decoder_version: DecoderVersion,
        fingerprint: InputFingerprint,
    ) -> Self {
        Self {
            key: CachedInputKey::new(path, decoder_version),
            fingerprint,
        }
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.key.to_path_buf()
    }

    pub(crate) fn decoder_version(&self) -> DecoderVersion {
        self.key.decoder_version
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CachedInputEntry {
    pub path: CachedPath,
    pub decoder_version: DecoderVersion,
    pub fingerprint: InputFingerprint,
    pub messages: Vec<ParsedMessage>,
    pub codex_incremental: Option<CodexIncrementalCache>,
    pub rejections: crate::input_health::RejectionSummary,
}

impl CachedInputEntry {
    #[cfg(test)]
    pub(crate) fn new(
        path: &Path,
        fingerprint: InputFingerprint,
        messages: Vec<ParsedMessage>,
        codex_incremental: Option<CodexIncrementalCache>,
    ) -> Self {
        Self::new_with_revision(path, 1, fingerprint, messages, codex_incremental)
    }

    #[cfg(test)]
    pub(crate) fn new_with_revision(
        path: &Path,
        decoder_revision: DecoderRevision,
        fingerprint: InputFingerprint,
        messages: Vec<ParsedMessage>,
        codex_incremental: Option<CodexIncrementalCache>,
    ) -> Self {
        Self::new_with_version(
            path,
            DecoderVersion::new(DecoderId::Amp, decoder_revision),
            fingerprint,
            messages,
            codex_incremental,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_version(
        path: &Path,
        decoder_version: DecoderVersion,
        fingerprint: InputFingerprint,
        messages: Vec<ParsedMessage>,
        codex_incremental: Option<CodexIncrementalCache>,
    ) -> Self {
        Self {
            path: CachedPath::from_path(path),
            decoder_version,
            fingerprint,
            messages,
            codex_incremental,
            rejections: Default::default(),
        }
    }

    fn plan(&self) -> CacheWritePlan {
        CacheWritePlan {
            path: self.path.clone(),
            decoder_version: self.decoder_version,
            fingerprint: self.fingerprint.clone(),
            codex_incremental: self.codex_incremental.clone(),
            rejections: self.rejections.clone(),
        }
    }

    #[cfg(test)]
    fn key(&self) -> CachedInputKey {
        CachedInputKey {
            path: self.path.clone(),
            decoder_version: self.decoder_version,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CacheWritePlan {
    path: CachedPath,
    decoder_version: DecoderVersion,
    fingerprint: InputFingerprint,
    codex_incremental: Option<CodexIncrementalCache>,
    rejections: crate::input_health::RejectionSummary,
}

impl CacheWritePlan {
    pub(crate) fn new(
        path: &Path,
        decoder_version: DecoderVersion,
        fingerprint: InputFingerprint,
        codex_incremental: Option<CodexIncrementalCache>,
    ) -> Self {
        Self {
            path: CachedPath::from_path(path),
            decoder_version,
            fingerprint,
            codex_incremental,
            rejections: Default::default(),
        }
    }

    /// Attach the scan's rejection summary so it persists with the shard and
    /// is restored on warm hits.
    pub(crate) fn with_rejections(
        mut self,
        rejections: crate::input_health::RejectionSummary,
    ) -> Self {
        self.rejections = rejections;
        self
    }

    fn key(&self) -> CachedInputKey {
        CachedInputKey {
            path: self.path.clone(),
            decoder_version: self.decoder_version,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedShardHeader {
    decoder_version: DecoderVersion,
    path: CachedPath,
    fingerprint: InputFingerprint,
    codex_incremental: Option<CodexIncrementalCache>,
    message_count: usize,
    rejections: crate::input_health::RejectionSummary,
}

#[derive(Debug, Serialize, Deserialize)]
struct CachedShardBody {
    messages: Vec<ParsedMessage>,
}

#[derive(Serialize)]
struct BorrowedCachedShardBody<'a> {
    messages: &'a [ParsedMessage],
}

#[derive(Debug, Clone)]
pub(crate) struct CachedInputMeta {
    pub fingerprint: InputFingerprint,
    pub codex_incremental: Option<CodexIncrementalCache>,
    pub rejections: crate::input_health::RejectionSummary,
}

pub(crate) struct InputMessageCache {
    cache_dir: PathBuf,
    dirty_entries: HashMap<CachedInputKey, CachedInputEntry>,
    deleted_paths: HashSet<CachedInputKey>,
    invalidated_read_paths: HashSet<CachedInputKey>,
    taken_paths: HashSet<CachedInputKey>,
    protected_paths: Mutex<HashSet<CachedInputKey>>,
    dirty: bool,
}

#[cfg(test)]
impl Default for InputMessageCache {
    fn default() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};

        static TEST_CACHE_ID: AtomicU64 = AtomicU64::new(0);
        let cache_dir = std::env::temp_dir().join(format!(
            "tokscale-input-cache-test-{}-{}",
            std::process::id(),
            TEST_CACHE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        Self::with_cache_dir(&cache_dir)
    }
}

impl InputMessageCache {
    #[cfg(test)]
    pub(crate) fn load() -> Result<Self, InputCacheError> {
        let cache_dir =
            cache_dir().map_err(|source| InputCacheError::CacheDirectoryUnavailable { source })?;
        Self::open(&cache_dir)
    }

    pub(crate) fn open(cache_dir: &Path) -> Result<Self, InputCacheError> {
        initialize_input_shards(cache_dir).map_err(|source| {
            InputCacheError::io("initialize input cache directory", cache_dir, source)
        })?;

        Ok(Self {
            cache_dir: cache_dir.to_path_buf(),
            dirty_entries: HashMap::new(),
            deleted_paths: HashSet::new(),
            invalidated_read_paths: HashSet::new(),
            taken_paths: HashSet::new(),
            protected_paths: Mutex::new(HashSet::new()),
            dirty: false,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_cache_dir(cache_dir: &Path) -> Self {
        Self::open(cache_dir).expect("test input cache directory must be usable")
    }

    #[cfg(test)]
    pub(crate) fn insert(&mut self, entry: CachedInputEntry) {
        let key = entry.key();
        self.dirty_entries.insert(key.clone(), entry);
        self.deleted_paths.remove(&key);
        self.invalidated_read_paths.remove(&key);
        self.taken_paths.remove(&key);
        self.dirty = true;
    }

    pub(crate) fn get_meta(
        &self,
        path: &Path,
        decoder_version: DecoderVersion,
    ) -> Result<Option<CachedInputMeta>, CacheLookupFailure> {
        let key = CachedInputKey::new(path, decoder_version);
        if self.deleted_paths.contains(&key) || self.taken_paths.contains(&key) {
            return Ok(None);
        }

        if let Some(entry) = self.dirty_entries.get(&key) {
            return Ok(Some(meta_from_entry(entry)));
        }

        let shard_path = self
            .shard_path_for_input_key(&key)
            .expect("configured input cache always has a shard path");
        let header = match read_shard_header(&shard_path) {
            Ok(Some(header)) => header,
            Ok(None) => return Ok(None),
            Err(reason) => {
                return Err(CacheLookupFailure {
                    input_path: key.to_path_buf(),
                    decoder_version: key.decoder_version,
                    shard_path,
                    reason,
                });
            }
        };
        if header.path != key.path || header.decoder_version != key.decoder_version {
            let reason = if header.path != key.path {
                CacheReadFailureReason::InputPathMismatch
            } else {
                CacheReadFailureReason::DecoderVersionMismatch
            };
            return Err(CacheLookupFailure {
                input_path: key.to_path_buf(),
                decoder_version: key.decoder_version,
                shard_path,
                reason,
            });
        }

        Ok(Some(meta_from_header(header)))
    }

    pub(crate) fn write_messages(
        &mut self,
        plan: CacheWritePlan,
        messages: &[ParsedMessage],
    ) -> Result<(), InputCacheError> {
        let key = plan.key();
        ensure_cache_dir(&self.cache_dir).map_err(|source| {
            InputCacheError::io("initialize input cache directory", &self.cache_dir, source)
        })?;
        let shard_path = shard_path_for_input_key(&self.cache_dir, &key);
        write_shard_borrowed(&self.cache_dir, &plan, messages).map_err(|source| {
            InputCacheError::io("atomically write input cache shard", &shard_path, source)
        })?;
        self.dirty_entries.remove(&key);
        self.deleted_paths.remove(&key);
        self.invalidated_read_paths.remove(&key);
        self.taken_paths.remove(&key);
        self.unprotect(&key);
        Ok(())
    }

    /// Move the messages out of a cache entry, leaving it empty. Safe for
    /// clean entries because shards are read lazily and callers must not
    /// re-read the same path's messages within one parse run.
    pub(crate) fn take_messages(
        &mut self,
        plan: &CacheReadPlan,
    ) -> Result<Vec<ParsedMessage>, CacheReadFailure> {
        let key = plan.key.clone();
        if self.deleted_paths.contains(&key) {
            return Err(CacheReadFailure::new(
                plan,
                self.shard_path_for_input_key(&key),
                CacheReadFailureReason::Invalidated,
            ));
        }
        if self.taken_paths.contains(&key) {
            let reason = if self.invalidated_read_paths.contains(&key) {
                CacheReadFailureReason::Invalidated
            } else {
                CacheReadFailureReason::AlreadyConsumed
            };
            return Err(CacheReadFailure::new(
                plan,
                self.shard_path_for_input_key(&key),
                reason,
            ));
        }

        if let Some(entry) = self.dirty_entries.get_mut(&key) {
            if entry.fingerprint != plan.fingerprint {
                return Err(CacheReadFailure::new(
                    plan,
                    self.shard_path_for_input_key(&key),
                    CacheReadFailureReason::FingerprintMismatch,
                ));
            }
            let messages = std::mem::take(&mut entry.messages);
            self.taken_paths.insert(key);
            return Ok(messages);
        }

        let shard_path = shard_path_for_input_key(&self.cache_dir, &key);
        let entry = match read_shard_entry_with_plan(&shard_path, plan) {
            Ok(entry) => entry,
            Err(reason) => {
                if reason.preserves_shard_until_replacement() {
                    self.protect(&key);
                }
                return Err(CacheReadFailure::new(plan, Some(shard_path), reason));
            }
        };
        self.taken_paths.insert(key);
        Ok(entry.messages)
    }

    pub(crate) fn remove(&mut self, path: &Path, decoder_version: DecoderVersion) {
        let key = CachedInputKey::new(path, decoder_version);
        if self.is_protected(&key) {
            return;
        }
        self.dirty_entries.remove(&key);
        self.invalidated_read_paths.remove(&key);
        self.taken_paths.remove(&key);
        self.deleted_paths.insert(key);
        self.dirty = true;
    }

    pub(crate) fn invalidate_read(&mut self, path: &Path, decoder_version: DecoderVersion) {
        let key = CachedInputKey::new(path, decoder_version);
        self.invalidated_read_paths.insert(key.clone());
        self.taken_paths.insert(key);
    }

    pub(crate) fn save_if_dirty(&mut self) -> Result<(), InputCacheError> {
        if !self.dirty {
            return Ok(());
        }

        let dir = self.cache_dir.clone();
        ensure_cache_dir(&dir).map_err(|source| {
            InputCacheError::io("initialize input cache directory", &dir, source)
        })?;

        for key in &self.deleted_paths {
            if self.is_protected(key) {
                continue;
            }
            let shard_path = shard_path_for_input_key(&dir, key);
            match fs::remove_file(&shard_path) {
                Ok(()) => sync_removed_shard_parent(&shard_path)?,
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(InputCacheError::io(
                        "remove invalid input cache shard",
                        &shard_path,
                        source,
                    ));
                }
            }
        }

        for (key, entry) in &self.dirty_entries {
            let shard_path = shard_path_for_input_key(&dir, key);
            write_shard_entry(&dir, entry).map_err(|source| {
                InputCacheError::io("atomically write input cache shard", &shard_path, source)
            })?;
            self.unprotect(key);
        }

        self.dirty = false;
        self.dirty_entries.clear();
        self.deleted_paths.clear();
        self.taken_paths.clear();
        Ok(())
    }

    fn shard_path_for_input_key(&self, key: &CachedInputKey) -> Option<PathBuf> {
        Some(shard_path_for_input_key(&self.cache_dir, key))
    }

    fn protect(&self, key: &CachedInputKey) -> bool {
        self.protected_paths
            .lock()
            .expect("input-cache protected-path lock poisoned")
            .insert(key.clone())
    }

    fn unprotect(&self, key: &CachedInputKey) {
        self.protected_paths
            .lock()
            .expect("input-cache protected-path lock poisoned")
            .remove(key);
    }

    fn is_protected(&self, key: &CachedInputKey) -> bool {
        self.protected_paths
            .lock()
            .expect("input-cache protected-path lock poisoned")
            .contains(key)
    }
}

struct PrunableShard {
    path: PathBuf,
    header: Option<CachedShardHeader>,
    input_exists: bool,
    canonical_path: bool,
}

/// Explicitly garbage-collect input-message cache shards.
///
/// Ordinary generation loads intentionally do not call this function. The
/// caller is responsible for exposing this potentially expensive full-cache
/// traversal as an explicit maintenance operation. Classification completes
/// before deletion, so unknown, future, or malformed-current envelopes cause
/// zero deletion. Once deletion starts, an unlink failure is returned
/// explicitly; already completed unlinks are not rolled back.
pub fn prune_input_message_cache() -> Result<InputCachePruneStats, InputCachePruneError> {
    let cache_dir =
        cache_dir().map_err(|source| InputCachePruneError::CacheDirectoryUnavailable { source })?;
    let shards_dir = cache_dir.join(SHARDS_DIRNAME);
    let shard_paths = shard_paths_for_prune(&shards_dir)?;
    let mut shards = Vec::with_capacity(shard_paths.len());
    let mut latest_revisions: HashMap<(CachedPath, DecoderId), DecoderRevision> = HashMap::new();
    let mut input_existence: HashMap<CachedPath, bool> = HashMap::new();

    for shard_path in shard_paths {
        let Some(header) = read_shard_header_for_prune(&shard_path)? else {
            shards.push(PrunableShard {
                path: shard_path,
                header: None,
                input_exists: false,
                canonical_path: false,
            });
            continue;
        };
        let key = CachedInputKey {
            path: header.path.clone(),
            decoder_version: header.decoder_version,
        };
        let canonical_path = shard_path_for_input_key(&cache_dir, &key) == shard_path;
        let input_exists = match input_existence.get(&header.path) {
            Some(exists) => *exists,
            None => {
                let input_path = header.path.to_path_buf();
                let exists = input_path.try_exists().map_err(|source| {
                    InputCachePruneError::io("inspect input path", &input_path, source)
                })?;
                input_existence.insert(header.path.clone(), exists);
                exists
            }
        };

        if input_exists && canonical_path {
            latest_revisions
                .entry((header.path.clone(), header.decoder_version.decoder_id))
                .and_modify(|revision| *revision = (*revision).max(header.decoder_version.revision))
                .or_insert(header.decoder_version.revision);
        }

        shards.push(PrunableShard {
            path: shard_path,
            header: Some(header),
            input_exists,
            canonical_path,
        });
    }

    let scanned = shards.len();
    let mut removed = 0;
    for shard in shards {
        let stale_revision = shard.header.as_ref().is_some_and(|header| {
            latest_revisions
                .get(&(header.path.clone(), header.decoder_version.decoder_id))
                .is_some_and(|latest| header.decoder_version.revision < *latest)
        });
        let should_remove = shard.header.is_none()
            || !shard.input_exists
            || !shard.canonical_path
            || stale_revision;
        if should_remove {
            fs::remove_file(&shard.path).map_err(|source| {
                InputCachePruneError::io("remove input cache shard", &shard.path, source)
            })?;
            removed += 1;
        }
    }

    Ok(InputCachePruneStats {
        scanned,
        removed,
        retained: scanned - removed,
    })
}

fn meta_from_entry(entry: &CachedInputEntry) -> CachedInputMeta {
    CachedInputMeta {
        fingerprint: entry.fingerprint.clone(),
        codex_incremental: entry.codex_incremental.clone(),
        rejections: entry.rejections.clone(),
    }
}

fn meta_from_header(header: CachedShardHeader) -> CachedInputMeta {
    CachedInputMeta {
        fingerprint: header.fingerprint,
        codex_incremental: header.codex_incremental,
        rejections: header.rejections,
    }
}

fn shard_key_for_input_key(key: &CachedInputKey) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"tokscale-input-shard-key");
    hasher.update(SHARD_KEY_FORMAT_VERSION.to_le_bytes());
    key.path.update_shard_key(&mut hasher);
    hash_inventory_bytes(
        &mut hasher,
        key.decoder_version.decoder_id.stable_name().as_bytes(),
    );
    hasher.update(key.decoder_version.revision.to_le_bytes());
    hasher.finalize().into()
}

#[cfg(test)]
fn shard_path(
    path: &Path,
    decoder_version: DecoderVersion,
) -> Result<PathBuf, crate::paths::ConfigDirUnavailable> {
    let dir = cache_dir()?;
    Ok(shard_path_for_input_key(
        &dir,
        &CachedInputKey::new(path, decoder_version),
    ))
}

fn shard_path_for_input_key(cache_dir: &Path, key: &CachedInputKey) -> PathBuf {
    let key = shard_key_for_input_key(key);
    let hex = hex_sha256(&key);
    cache_dir
        .join(SHARDS_DIRNAME)
        .join(&hex[..2])
        .join(format!("{hex}.bin"))
}

#[cfg(test)]
pub(crate) fn shard_path_for_test(
    cache_dir: &Path,
    input_path: &Path,
    decoder_version: DecoderVersion,
) -> PathBuf {
    shard_path_for_input_key(cache_dir, &CachedInputKey::new(input_path, decoder_version))
}

#[cfg(test)]
pub(crate) fn mark_current_key_shard_as_unsupported_format_for_test(
    cache_dir: &Path,
    input_path: &Path,
    decoder_version: DecoderVersion,
) -> PathBuf {
    let shard_path = shard_path_for_test(cache_dir, input_path, decoder_version);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&shard_path)
        .expect("test cache shard must exist");
    file.seek(SeekFrom::Start(SHARD_MAGIC.len() as u64))
        .expect("test shard format field must be seekable");
    file.write_all(&UNSUPPORTED_CACHE_FORMAT_VERSION.to_le_bytes())
        .expect("test shard format field must be writable");
    file.flush().expect("test shard format rewrite must flush");
    shard_path
}

#[cfg(test)]
pub(crate) fn mark_current_key_shard_as_future_format_for_test(
    cache_dir: &Path,
    input_path: &Path,
    decoder_version: DecoderVersion,
) -> PathBuf {
    let shard_path = shard_path_for_test(cache_dir, input_path, decoder_version);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&shard_path)
        .expect("test cache shard must exist");
    file.seek(SeekFrom::Start(SHARD_MAGIC.len() as u64))
        .expect("test shard format field must be seekable");
    file.write_all(&(CACHE_FORMAT_VERSION + 1).to_le_bytes())
        .expect("test shard format field must be writable");
    file.flush().expect("test shard format rewrite must flush");
    shard_path
}

#[cfg(test)]
pub(crate) fn truncate_shard_after_header_for_test(
    cache_dir: &Path,
    input_path: &Path,
    decoder_version: DecoderVersion,
) -> PathBuf {
    let shard_path = shard_path_for_test(cache_dir, input_path, decoder_version);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&shard_path)
        .expect("test cache shard must exist");
    let mut prefix = [0_u8; 20];
    file.read_exact(&mut prefix)
        .expect("test cache shard prefix must be readable");
    assert_eq!(&prefix[..8], &SHARD_MAGIC);
    assert_eq!(
        u32::from_le_bytes(prefix[8..12].try_into().unwrap()),
        CACHE_FORMAT_VERSION
    );
    let header_len = u64::from_le_bytes(prefix[12..20].try_into().unwrap());
    file.set_len(20 + header_len)
        .expect("test cache shard body must be truncatable");
    shard_path
}

#[cfg(test)]
pub(crate) fn replace_shard_message_count_for_test(
    cache_dir: &Path,
    input_path: &Path,
    decoder_version: DecoderVersion,
    message_count: usize,
) -> PathBuf {
    let shard_path = shard_path_for_test(cache_dir, input_path, decoder_version);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&shard_path)
        .expect("test cache shard must exist");
    let header =
        read_shard_header_from_file_result(&mut file).expect("test cache shard header must decode");
    let header_start = file.stream_position().unwrap();
    let original_header_len = header_start - 20;
    let mut replacement = header;
    replacement.message_count = message_count;
    let replacement_bytes = bincode::options().serialize(&replacement).unwrap();
    assert_eq!(
        replacement_bytes.len() as u64,
        original_header_len,
        "test replacement count must preserve encoded header length"
    );
    file.seek(SeekFrom::Start(20)).unwrap();
    file.write_all(&replacement_bytes).unwrap();
    file.flush().unwrap();
    shard_path
}

fn hex_sha256(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn header_from_plan(plan: &CacheWritePlan, message_count: usize) -> CachedShardHeader {
    CachedShardHeader {
        decoder_version: plan.decoder_version,
        path: plan.path.clone(),
        fingerprint: plan.fingerprint.clone(),
        codex_incremental: plan.codex_incremental.clone(),
        message_count,
        rejections: plan.rejections.clone(),
    }
}

fn read_shard_header(path: &Path) -> Result<Option<CachedShardHeader>, CacheReadFailureReason> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(CacheReadFailureReason::Open { source }),
    };
    let metadata = file
        .metadata()
        .map_err(|source| CacheReadFailureReason::Metadata { source })?;
    read_current_shard_envelope(&mut file)?;
    if metadata.len() > MAX_CACHE_FILE_BYTES {
        return Err(CacheReadFailureReason::TooLarge {
            actual: metadata.len(),
            limit: MAX_CACHE_FILE_BYTES,
        });
    }
    let header = read_current_shard_header(&mut file)?;

    Ok(Some(header))
}

fn read_shard_entry_with_plan(
    path: &Path,
    plan: &CacheReadPlan,
) -> Result<CachedInputEntry, CacheReadFailureReason> {
    let mut file = File::open(path).map_err(|source| CacheReadFailureReason::Open { source })?;
    let metadata = file
        .metadata()
        .map_err(|source| CacheReadFailureReason::Metadata { source })?;
    read_current_shard_envelope(&mut file)?;
    if metadata.len() > MAX_CACHE_FILE_BYTES {
        return Err(CacheReadFailureReason::TooLarge {
            actual: metadata.len(),
            limit: MAX_CACHE_FILE_BYTES,
        });
    }
    let header = read_current_shard_header(&mut file)?;
    if header.path != plan.key.path {
        return Err(CacheReadFailureReason::InputPathMismatch);
    }
    if header.decoder_version != plan.key.decoder_version {
        return Err(CacheReadFailureReason::DecoderVersionMismatch);
    }
    if header.fingerprint != plan.fingerprint {
        return Err(CacheReadFailureReason::ShardFingerprintMismatch);
    }
    let body: CachedShardBody = bincode::options()
        .with_limit(MAX_CACHE_FILE_BYTES)
        .deserialize_from(&mut file)
        .map_err(|source| CacheReadFailureReason::BodyDecode { source })?;
    if body.messages.len() != header.message_count {
        return Err(CacheReadFailureReason::MessageCountMismatch {
            declared: header.message_count,
            actual: body.messages.len(),
        });
    }

    Ok(CachedInputEntry {
        path: header.path,
        decoder_version: header.decoder_version,
        fingerprint: header.fingerprint,
        messages: body.messages,
        rejections: header.rejections,
        codex_incremental: header.codex_incremental,
    })
}

#[cfg(test)]
fn read_shard_header_from_file_result(
    file: &mut File,
) -> Result<CachedShardHeader, CacheReadFailureReason> {
    read_current_shard_envelope(file)?;
    read_current_shard_header(file)
}

fn read_current_shard_envelope(file: &mut File) -> Result<(), CacheReadFailureReason> {
    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic)
        .map_err(|source| CacheReadFailureReason::HeaderRead { source })?;
    if magic != SHARD_MAGIC {
        return Err(CacheReadFailureReason::InvalidMagic { actual: magic });
    }
    let mut version_bytes = [0_u8; 4];
    file.read_exact(&mut version_bytes)
        .map_err(|source| CacheReadFailureReason::HeaderRead { source })?;
    let version = u32::from_le_bytes(version_bytes);
    if version != CACHE_FORMAT_VERSION {
        return Err(CacheReadFailureReason::FormatMismatch {
            actual: version,
            current: CACHE_FORMAT_VERSION,
        });
    }
    Ok(())
}

fn read_current_shard_header(file: &mut File) -> Result<CachedShardHeader, CacheReadFailureReason> {
    let mut len_bytes = [0_u8; 8];
    file.read_exact(&mut len_bytes)
        .map_err(|source| CacheReadFailureReason::HeaderRead { source })?;
    let header_len = u64::from_le_bytes(len_bytes);
    if header_len == 0 || header_len > MAX_SHARD_HEADER_BYTES {
        return Err(CacheReadFailureReason::InvalidHeaderLength { actual: header_len });
    }

    let mut header_bytes = vec![0_u8; header_len as usize];
    file.read_exact(&mut header_bytes)
        .map_err(|source| CacheReadFailureReason::HeaderRead { source })?;
    bincode::options()
        .with_limit(MAX_SHARD_HEADER_BYTES)
        .deserialize(&header_bytes)
        .map_err(|source| CacheReadFailureReason::HeaderDecode { source })
}

fn write_shard_entry(cache_dir: &Path, entry: &CachedInputEntry) -> std::io::Result<()> {
    write_shard_borrowed(cache_dir, &entry.plan(), &entry.messages)
}

fn write_shard_borrowed(
    cache_dir: &Path,
    plan: &CacheWritePlan,
    messages: &[ParsedMessage],
) -> std::io::Result<()> {
    let final_path = shard_path_for_input_key(cache_dir, &plan.key());
    let parent = final_path
        .parent()
        .ok_or_else(|| std::io::Error::other("cache shard path has no parent"))?;
    ensure_cache_dir(parent)?;

    let header = header_from_plan(plan, messages.len());
    let header_bytes = bincode::options()
        .serialize(&header)
        .map_err(std::io::Error::other)?;
    let body = BorrowedCachedShardBody { messages };

    crate::fs_atomic::write_atomic_with(&final_path, |file| {
        let mut writer = BufWriter::new(file);
        writer.write_all(&SHARD_MAGIC)?;
        writer.write_all(&CACHE_FORMAT_VERSION.to_le_bytes())?;
        writer.write_all(&(header_bytes.len() as u64).to_le_bytes())?;
        writer.write_all(&header_bytes)?;
        bincode::options()
            .with_limit(MAX_CACHE_FILE_BYTES)
            .serialize_into(&mut writer, &body)
            .map_err(std::io::Error::other)?;
        writer.flush()?;
        Ok(())
    })
}

#[cfg(unix)]
fn sync_removed_shard_parent(shard_path: &Path) -> Result<(), InputCacheError> {
    let parent = shard_path.parent().ok_or_else(|| {
        InputCacheError::io(
            "locate removed input cache shard parent",
            shard_path,
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "input cache shard path has no parent",
            ),
        )
    })?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| {
            InputCacheError::io("sync removed input cache shard directory", parent, source)
        })
}

#[cfg(not(unix))]
fn sync_removed_shard_parent(_shard_path: &Path) -> Result<(), InputCacheError> {
    Ok(())
}

fn shard_paths_for_prune(shards_dir: &Path) -> Result<Vec<PathBuf>, InputCachePruneError> {
    let mut paths = Vec::new();
    let exists = shards_dir.try_exists().map_err(|source| {
        InputCachePruneError::io("inspect input cache shard directory", shards_dir, source)
    })?;
    if !exists {
        return Ok(paths);
    }

    let prefixes = fs::read_dir(shards_dir).map_err(|source| {
        InputCachePruneError::io("read input cache shard directory", shards_dir, source)
    })?;
    for prefix in prefixes {
        let prefix = prefix.map_err(|source| {
            InputCachePruneError::io("read input cache shard directory entry", shards_dir, source)
        })?;
        let prefix_path = prefix.path();
        let file_type = prefix.file_type().map_err(|source| {
            InputCachePruneError::io("inspect input cache shard prefix", &prefix_path, source)
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let files = fs::read_dir(&prefix_path).map_err(|source| {
            InputCachePruneError::io("read input cache shard prefix", &prefix_path, source)
        })?;
        for file in files {
            let file = file.map_err(|source| {
                InputCachePruneError::io("read input cache shard entry", &prefix_path, source)
            })?;
            let file_path = file.path();
            let file_type = file.file_type().map_err(|source| {
                InputCachePruneError::io("inspect input cache shard", &file_path, source)
            })?;
            if file_type.is_file()
                && file_path
                    .extension()
                    .is_some_and(|extension| extension == "bin")
            {
                paths.push(file_path);
            }
        }
    }
    paths.sort_unstable();
    Ok(paths)
}

fn read_shard_header_for_prune(
    path: &Path,
) -> Result<Option<CachedShardHeader>, InputCachePruneError> {
    let mut file = File::open(path)
        .map_err(|source| InputCachePruneError::io("open input cache shard", path, source))?;
    let file_len = file
        .metadata()
        .map_err(|source| InputCachePruneError::io("inspect input cache shard", path, source))?
        .len();

    let mut magic = [0_u8; 8];
    file.read_exact(&mut magic).map_err(|source| {
        InputCachePruneError::io("read input cache shard header", path, source)
    })?;
    if magic != SHARD_MAGIC {
        return Err(InputCachePruneError::UnknownMagic {
            path: path.to_path_buf(),
            actual: magic,
        });
    }

    let mut version_bytes = [0_u8; 4];
    file.read_exact(&mut version_bytes).map_err(|source| {
        InputCachePruneError::io("read input cache shard format version", path, source)
    })?;
    let format_version = u32::from_le_bytes(version_bytes);
    if format_version != CACHE_FORMAT_VERSION {
        return Err(InputCachePruneError::UnsupportedFormat {
            path: path.to_path_buf(),
            actual: format_version,
            current: CACHE_FORMAT_VERSION,
        });
    }
    if file_len > MAX_CACHE_FILE_BYTES {
        return Err(InputCachePruneError::TooLarge {
            path: path.to_path_buf(),
            actual: file_len,
            limit: MAX_CACHE_FILE_BYTES,
        });
    }

    let mut len_bytes = [0_u8; 8];
    file.read_exact(&mut len_bytes).map_err(|source| {
        InputCachePruneError::current_format_io(
            "read input cache shard header length",
            path,
            format_version,
            source,
        )
    })?;
    let header_len = u64::from_le_bytes(len_bytes);
    if header_len == 0 || header_len > MAX_SHARD_HEADER_BYTES {
        return Err(InputCachePruneError::InvalidHeaderLength {
            path: path.to_path_buf(),
            format_version,
            actual: header_len,
        });
    }

    let mut header_bytes = vec![0_u8; header_len as usize];
    file.read_exact(&mut header_bytes).map_err(|source| {
        InputCachePruneError::current_format_io(
            "read input cache shard header",
            path,
            format_version,
            source,
        )
    })?;
    bincode::options()
        .with_limit(MAX_SHARD_HEADER_BYTES)
        .deserialize(&header_bytes)
        .map(Some)
        .map_err(|source| InputCachePruneError::Decode {
            path: path.to_path_buf(),
            format_version,
            source,
        })
}

fn modified_ns(path: &Path, metadata: &fs::Metadata) -> Result<u64, InputSnapshotError> {
    let modified = metadata
        .modified()
        .map_err(|source| InputSnapshotError::ModifiedTime {
            path: path.to_path_buf(),
            source,
        })?;
    let nanos = modified
        .duration_since(UNIX_EPOCH)
        .map_err(|source| InputSnapshotError::ModifiedBeforeEpoch {
            path: path.to_path_buf(),
            source,
        })?
        .as_nanos();
    u64::try_from(nanos).map_err(|_| InputSnapshotError::ModifiedTimeOutOfRange {
        path: path.to_path_buf(),
    })
}

fn append_path_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut os = OsString::from(path.as_os_str());
    os.push(suffix);
    PathBuf::from(os)
}

fn hash_prefix(path: &Path, len: u64) -> Result<[u8; 32], InputSnapshotError> {
    let mut file = File::open(path)
        .map_err(|source| InputSnapshotError::io("open input for hashing", path, source))?;
    #[cfg(test)]
    record_input_hash_start(path);
    let mut hasher = Sha256::new();
    let mut remaining = len;
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];

    while remaining > 0 {
        let bytes_to_read = remaining.min(HASH_BUFFER_BYTES as u64) as usize;
        let read = file
            .read(&mut buffer[..bytes_to_read])
            .map_err(|source| InputSnapshotError::io("read input for hashing", path, source))?;
        if read == 0 {
            return Err(InputSnapshotError::io(
                "read complete input prefix for hashing",
                path,
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("input ended with {remaining} prefix bytes remaining"),
                ),
            ));
        }
        #[cfg(test)]
        record_input_bytes(path, read);
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }

    Ok(hasher.finalize().into())
}

#[cfg(test)]
fn hash_file_contents(path: &Path) -> Result<[u8; 32], InputSnapshotError> {
    let metadata = fs::metadata(path).map_err(|source| {
        InputSnapshotError::io("read input metadata for hashing", path, source)
    })?;
    hash_prefix(path, metadata.len())
}

pub(crate) fn build_codex_incremental_cache(
    consumed_offset: u64,
    state: CodexParseState,
    ends_with_newline: bool,
    content_hash: [u8; 32],
) -> Option<CodexIncrementalCache> {
    if !ends_with_newline {
        return None;
    }

    Some(CodexIncrementalCache {
        state,
        consumed_offset,
        ends_with_newline,
        prefix_hash: content_hash,
    })
}

#[cfg(test)]
pub(crate) fn codex_prefix_matches(
    path: &Path,
    cached: &CodexIncrementalCache,
) -> Result<bool, InputSnapshotError> {
    if cached.consumed_offset > 0 && !cached.ends_with_newline {
        return Ok(false);
    }

    Ok(hash_prefix(path, cached.consumed_offset)? == cached.prefix_hash)
}

pub(crate) fn codex_cache_meta_is_consistent(cached: &CachedInputMeta) -> bool {
    let Some(codex_incremental) = cached.codex_incremental.as_ref() else {
        return false;
    };
    codex_incremental.consumed_offset == cached.fingerprint.size
        && codex_incremental.ends_with_newline
        && codex_incremental.prefix_hash == cached.fingerprint.content_hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TokenBreakdown;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    fn restore_env_var(key: &str, value: Option<impl AsRef<std::ffi::OsStr>>) {
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }

    /// Pin every env var the cache resolvers consult so the test stays
    /// inside `temp_home`. CI runners can leak `XDG_CONFIG_HOME` /
    /// `XDG_CACHE_HOME` from the host, which would resolve outside the
    /// sandbox. Returns the saved values so the caller can restore them.
    fn sandbox_cache_env(
        temp_home: &std::path::Path,
    ) -> (
        Option<std::ffi::OsString>,
        Option<std::ffi::OsString>,
        Option<std::ffi::OsString>,
        Option<std::ffi::OsString>,
    ) {
        let prev_home = std::env::var_os("HOME");
        let prev_xdg_config = std::env::var_os("XDG_CONFIG_HOME");
        let prev_xdg_cache = std::env::var_os("XDG_CACHE_HOME");
        let prev_override = std::env::var_os("TOKSCALE_CONFIG_DIR");
        unsafe {
            std::env::set_var("HOME", temp_home);
            std::env::set_var("XDG_CONFIG_HOME", temp_home.join(".config"));
            std::env::set_var("XDG_CACHE_HOME", temp_home.join(".cache"));
            std::env::remove_var("TOKSCALE_CONFIG_DIR");
        }
        (prev_home, prev_xdg_config, prev_xdg_cache, prev_override)
    }

    fn restore_cache_env(
        prev: (
            Option<std::ffi::OsString>,
            Option<std::ffi::OsString>,
            Option<std::ffi::OsString>,
            Option<std::ffi::OsString>,
        ),
    ) {
        restore_env_var("HOME", prev.0);
        restore_env_var("XDG_CONFIG_HOME", prev.1);
        restore_env_var("XDG_CACHE_HOME", prev.2);
        restore_env_var("TOKSCALE_CONFIG_DIR", prev.3);
    }

    fn test_decoder_version(revision: DecoderRevision) -> DecoderVersion {
        DecoderVersion::new(DecoderId::Amp, revision)
    }

    #[test]
    fn decoder_id_bincode_uses_its_stable_name() {
        let encoded_id = bincode::options().serialize(&DecoderId::Amp).unwrap();
        let encoded_name = bincode::options()
            .serialize(DecoderId::Amp.stable_name())
            .unwrap();

        assert_eq!(encoded_id, encoded_name);
        assert_eq!(
            bincode::options()
                .deserialize::<DecoderId>(&encoded_id)
                .unwrap(),
            DecoderId::Amp
        );
    }

    fn test_cache_read_failure(reason: CacheReadFailureReason) -> CacheReadFailure {
        CacheReadFailure {
            input_path: PathBuf::from("/test/input"),
            decoder_version: test_decoder_version(1),
            shard_path: Some(PathBuf::from("/test/shard")),
            reason,
        }
    }

    #[test]
    fn current_shard_key_uses_stable_path_decoder_tag_and_revision_fields() {
        let path = Path::new("/test/input");
        let amp_v1 = CachedInputKey::new(path, DecoderVersion::new(DecoderId::Amp, 1));
        let amp_v2 = CachedInputKey::new(path, DecoderVersion::new(DecoderId::Amp, 2));
        let claude_v1 = CachedInputKey::new(path, DecoderVersion::new(DecoderId::Claude, 1));
        let other_path = CachedInputKey::new(
            Path::new("/test/other-input"),
            DecoderVersion::new(DecoderId::Amp, 1),
        );

        assert_eq!(
            shard_key_for_input_key(&amp_v1),
            shard_key_for_input_key(&CachedInputKey::new(
                path,
                DecoderVersion::new(DecoderId::Amp, 1),
            ))
        );
        assert_ne!(
            shard_key_for_input_key(&amp_v1),
            shard_key_for_input_key(&amp_v2)
        );
        assert_ne!(
            shard_key_for_input_key(&amp_v1),
            shard_key_for_input_key(&claude_v1)
        );
        assert_ne!(
            shard_key_for_input_key(&amp_v1),
            shard_key_for_input_key(&other_path)
        );
    }

    #[test]
    fn cache_read_removal_classification_preserves_non_body_failures() {
        for reason in [
            CacheReadFailureReason::Open {
                source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
            },
            CacheReadFailureReason::Metadata {
                source: std::io::Error::from(std::io::ErrorKind::Other),
            },
            CacheReadFailureReason::HeaderRead {
                source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
            },
            CacheReadFailureReason::BodyDecode {
                source: Box::new(bincode::ErrorKind::Io(std::io::Error::from(
                    std::io::ErrorKind::Other,
                ))),
            },
            CacheReadFailureReason::InvalidMagic {
                actual: *b"notmagic",
            },
            CacheReadFailureReason::FormatMismatch {
                actual: UNSUPPORTED_CACHE_FORMAT_VERSION,
                current: CACHE_FORMAT_VERSION,
            },
            CacheReadFailureReason::FormatMismatch {
                actual: CACHE_FORMAT_VERSION + 1,
                current: CACHE_FORMAT_VERSION,
            },
            CacheReadFailureReason::InvalidHeaderLength { actual: 0 },
            CacheReadFailureReason::HeaderDecode {
                source: Box::new(bincode::ErrorKind::Custom(
                    "invalid header structure".to_string(),
                )),
            },
            CacheReadFailureReason::InputPathMismatch,
            CacheReadFailureReason::DecoderVersionMismatch,
            CacheReadFailureReason::FingerprintMismatch,
            CacheReadFailureReason::ShardFingerprintMismatch,
        ] {
            assert!(
                !test_cache_read_failure(reason).requires_shard_removal(),
                "transient or replacement-race failures must not delete the shard"
            );
        }
    }

    #[test]
    fn cache_read_removal_classification_removes_proven_body_corruption() {
        for reason in [
            CacheReadFailureReason::BodyDecode {
                source: Box::new(bincode::ErrorKind::Io(std::io::Error::from(
                    std::io::ErrorKind::UnexpectedEof,
                ))),
            },
            CacheReadFailureReason::BodyDecode {
                source: Box::new(bincode::ErrorKind::Custom(
                    "invalid body structure".to_string(),
                )),
            },
            CacheReadFailureReason::MessageCountMismatch {
                declared: 2,
                actual: 1,
            },
        ] {
            assert!(
                test_cache_read_failure(reason).requires_shard_removal(),
                "structural corruption must remove the derived shard"
            );
        }
    }

    #[test]
    fn cache_read_faults_reparse_inputs_but_in_memory_contract_faults_do_not() {
        for reason in [
            CacheReadFailureReason::Open {
                source: std::io::Error::from(std::io::ErrorKind::NotFound),
            },
            CacheReadFailureReason::InvalidMagic {
                actual: *b"notmagic",
            },
            CacheReadFailureReason::FormatMismatch {
                actual: CACHE_FORMAT_VERSION + 1,
                current: CACHE_FORMAT_VERSION,
            },
            CacheReadFailureReason::ShardFingerprintMismatch,
            CacheReadFailureReason::MessageCountMismatch {
                declared: 2,
                actual: 1,
            },
        ] {
            assert!(test_cache_read_failure(reason).can_reparse_input());
        }

        for reason in [
            CacheReadFailureReason::Invalidated,
            CacheReadFailureReason::AlreadyConsumed,
            CacheReadFailureReason::FingerprintMismatch,
        ] {
            assert!(!test_cache_read_failure(reason).can_reparse_input());
        }
    }

    fn write_temp_file(content: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn input_file_identity_matches_hard_links_and_distinguishes_files() {
        let dir = TempDir::new().unwrap();
        let input = dir.path().join("input.jsonl");
        let hard_link = dir.path().join("hard-link.jsonl");
        let distinct = dir.path().join("distinct.jsonl");
        std::fs::write(&input, b"same-size").unwrap();
        std::fs::hard_link(&input, &hard_link).unwrap();
        std::fs::write(&distinct, b"same-size").unwrap();

        let identity = |path: &Path| {
            InputPolicy::plain(path)
                .snapshot()
                .unwrap()
                .primary_identity()
                .unwrap()
        };

        assert_eq!(identity(&input), identity(&hard_link));
        assert_ne!(identity(&input), identity(&distinct));
    }

    #[test]
    fn primary_snapshot_metadata_failure_is_typed_instead_of_becoming_no_cache() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("missing.jsonl");

        let error = InputPolicy::plain(&missing)
            .snapshot()
            .expect_err("a missing primary input must not degrade to an absent snapshot");

        assert!(matches!(
            error,
            InputSnapshotError::Io {
                operation: "read input metadata and file identity",
                path,
                source,
            } if path == missing && source.kind() == std::io::ErrorKind::NotFound
        ));
    }

    #[test]
    fn optional_related_directory_is_preserved_as_unavailable_snapshot_state() {
        let dir = TempDir::new().unwrap();
        let primary = dir.path().join("primary.jsonl");
        let related = dir.path().join("config.toml");
        std::fs::write(&primary, b"primary").unwrap();
        std::fs::create_dir(&related).unwrap();
        let policy = InputPolicy::with_dependency(&primary, related.clone())
            .with_related_failure_policy(RelatedInputFailurePolicy::PreservePrimary);

        let snapshot = policy
            .snapshot()
            .expect("optional related failures must remain in the snapshot");
        assert_eq!(snapshot, snapshot.clone());
        assert!(matches!(
            snapshot.files.as_slice(),
            [
                InputFileSnapshot::Present { .. },
                InputFileSnapshot::Unavailable { .. }
            ]
        ));

        let mut visited = Vec::new();
        snapshot.visit_present_files(|identity, size| visited.push((identity, size)));
        assert_eq!(visited.len(), 1);
        assert_eq!(visited[0].1, 7);

        let stamp_error = policy.stamp_from_snapshot(&snapshot).unwrap_err();
        assert!(stamp_error.is_optional_related_input_unavailable());
        assert!(matches!(
            stamp_error,
            InputSnapshotError::OptionalRelatedInputUnavailable { path, .. }
                if path == related
        ));
        assert!(policy
            .fingerprint_from_snapshot(&snapshot)
            .unwrap_err()
            .is_optional_related_input_unavailable());
    }

    #[test]
    fn required_related_directory_still_fails_the_snapshot() {
        let dir = TempDir::new().unwrap();
        let primary = dir.path().join("primary.jsonl");
        let related = dir.path().join("config.toml");
        std::fs::write(&primary, b"primary").unwrap();
        std::fs::create_dir(&related).unwrap();

        let error = InputPolicy::with_dependency(&primary, related.clone())
            .snapshot()
            .unwrap_err();
        assert!(error.to_string().contains(&related.display().to_string()));
        assert!(!error.is_optional_related_input_unavailable());
    }

    #[test]
    fn primary_directory_fails_even_when_related_failures_are_optional() {
        let dir = TempDir::new().unwrap();
        let primary = dir.path().join("primary.jsonl");
        std::fs::create_dir(&primary).unwrap();
        let policy =
            InputPolicy::with_dependency(&primary, dir.path().join("optional-config.toml"))
                .with_related_failure_policy(RelatedInputFailurePolicy::PreservePrimary);

        let error = policy.snapshot().unwrap_err();
        assert!(error.to_string().contains(&primary.display().to_string()));
        assert!(!error.is_optional_related_input_unavailable());
    }

    #[test]
    fn optional_related_hash_failure_is_precisely_classified() {
        let dir = TempDir::new().unwrap();
        let primary = dir.path().join("primary.jsonl");
        let related = dir.path().join("config.toml");
        std::fs::write(&primary, b"primary").unwrap();
        std::fs::write(&related, b"config").unwrap();
        let required_policy = InputPolicy::with_dependency(&primary, related.clone());
        let optional_policy = required_policy
            .clone()
            .with_related_failure_policy(RelatedInputFailurePolicy::PreservePrimary);
        let snapshot = optional_policy.snapshot().unwrap();
        std::fs::remove_file(&related).unwrap();

        let optional_error = optional_policy
            .fingerprint_from_snapshot(&snapshot)
            .unwrap_err();
        assert!(optional_error.is_optional_related_input_unavailable());
        let required_error = required_policy
            .fingerprint_from_snapshot(&snapshot)
            .unwrap_err();
        assert!(matches!(required_error, InputSnapshotError::Io { path, .. } if path == related));
    }

    #[test]
    fn inventory_signature_distinguishes_present_absent_and_unavailable_inputs() {
        let dir = TempDir::new().unwrap();
        let primary = dir.path().join("primary.jsonl");
        let related = dir.path().join("config.toml");
        std::fs::write(&primary, b"primary").unwrap();
        let policy = InputPolicy::with_dependency(&primary, related.clone())
            .with_related_failure_policy(RelatedInputFailurePolicy::PreservePrimary);
        let signature = |snapshot: &InputSnapshot| {
            let mut hasher = Sha256::new();
            policy.update_inventory_signature(snapshot, &mut hasher);
            <[u8; 32]>::from(hasher.finalize())
        };

        let absent = policy.snapshot().unwrap();
        std::fs::write(&related, b"config").unwrap();
        let present = policy.snapshot().unwrap();
        std::fs::remove_file(&related).unwrap();
        std::fs::create_dir(&related).unwrap();
        let unavailable = policy.snapshot().unwrap();

        assert_ne!(signature(&present), signature(&absent));
        assert_ne!(signature(&present), signature(&unavailable));
        assert_ne!(signature(&absent), signature(&unavailable));
    }

    #[test]
    fn input_stamp_changes_when_same_size_and_mtime_path_is_replaced() {
        let dir = TempDir::new().unwrap();
        let input = dir.path().join("input.jsonl");
        let replacement = dir.path().join("replacement.jsonl");
        std::fs::write(&input, b"aaaaaaaa").unwrap();
        let original_mtime = std::fs::metadata(&input).unwrap().modified().unwrap();

        let policy = InputPolicy::plain(&input);
        let before_snapshot = policy.snapshot().unwrap();
        let before_stamp = policy.stamp_from_snapshot(&before_snapshot).unwrap();

        std::fs::write(&replacement, b"bbbbbbbb").unwrap();
        std::fs::File::open(&replacement)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(original_mtime))
            .unwrap();
        #[cfg(windows)]
        std::fs::remove_file(&input).unwrap();
        std::fs::rename(&replacement, &input).unwrap();

        let after_snapshot = policy.snapshot().unwrap();
        let after_stamp = policy.stamp_from_snapshot(&after_snapshot).unwrap();

        assert_eq!(before_stamp.files[0].size, after_stamp.files[0].size);
        assert_eq!(
            before_stamp.files[0].modified_ns,
            after_stamp.files[0].modified_ns
        );
        assert_ne!(
            before_snapshot.primary_identity(),
            after_snapshot.primary_identity()
        );
        assert_ne!(before_stamp, after_stamp);
    }

    fn replace_preserving_size_and_mtime(path: &Path, replacement: &Path, bytes: &[u8]) {
        let original = std::fs::metadata(path).unwrap();
        assert_eq!(original.len(), bytes.len() as u64);
        let original_mtime = original.modified().unwrap();
        std::fs::write(replacement, bytes).unwrap();
        std::fs::File::open(replacement)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(original_mtime))
            .unwrap();
        #[cfg(windows)]
        std::fs::remove_file(path).unwrap();
        std::fs::rename(replacement, path).unwrap();
    }

    #[test]
    fn sqlite_main_and_wal_identities_invalidate_same_size_same_mtime_replacements() {
        let dir = TempDir::new().unwrap();
        let database = dir.path().join("usage.db");
        let wal = dir.path().join("usage.db-wal");
        std::fs::write(&database, b"database").unwrap();
        std::fs::write(&wal, b"wal-one!").unwrap();
        let policy = InputPolicy::sqlite_with_wal(&database);
        let before_main = policy.stamp().unwrap();

        replace_preserving_size_and_mtime(
            &database,
            &dir.path().join("replacement-database"),
            b"new-data",
        );

        let after_main = policy.stamp().unwrap();
        assert_eq!(before_main.files[0].size, after_main.files[0].size);
        assert_eq!(
            before_main.files[0].modified_ns,
            after_main.files[0].modified_ns
        );
        assert_ne!(before_main.files[0].identity, after_main.files[0].identity);
        assert_ne!(before_main, after_main);

        replace_preserving_size_and_mtime(&wal, &dir.path().join("replacement-wal"), b"wal-two!");

        let after_wal = policy.stamp().unwrap();
        assert_eq!(after_main.files[1].size, after_wal.files[1].size);
        assert_eq!(
            after_main.files[1].modified_ns,
            after_wal.files[1].modified_ns
        );
        assert_ne!(after_main.files[1].identity, after_wal.files[1].identity);
        assert_ne!(after_main, after_wal);
    }

    #[test]
    fn claude_meta_identity_invalidates_same_size_same_mtime_replacement() {
        let dir = TempDir::new().unwrap();
        let input = dir.path().join("session.jsonl");
        let meta = dir.path().join("session.meta.json");
        std::fs::write(&input, b"session!").unwrap();
        std::fs::write(&meta, b"meta-one").unwrap();
        let policy = InputPolicy::claude_code(&input, None);
        let before = policy.stamp().unwrap();

        replace_preserving_size_and_mtime(&meta, &dir.path().join("replacement-meta"), b"meta-two");

        let after = policy.stamp().unwrap();
        assert_eq!(before.files[1].modified_ns, after.files[1].modified_ns);
        assert_ne!(before.files[1].identity, after.files[1].identity);
        assert_ne!(before, after);
    }

    #[test]
    fn input_snapshot_entries_do_not_own_policy_labels_or_paths() {
        assert_eq!(
            std::mem::size_of::<InputSnapshot>(),
            std::mem::size_of::<Vec<InputFileSnapshot>>()
        );

        let dir = TempDir::new().unwrap();
        let primary = dir.path().join("primary.db");
        let related = dir.path().join("primary.db-wal");
        std::fs::write(&primary, b"primary").unwrap();
        std::fs::write(&related, b"wal").unwrap();
        let policy = InputPolicy::sqlite_with_wal(&primary);
        let snapshot = policy.snapshot().unwrap();

        assert_eq!(snapshot.files.len(), 2);
        assert_eq!(snapshot, snapshot.clone());
        let stamp = policy.stamp_from_snapshot(&snapshot).unwrap();
        assert_eq!(stamp.files[0].label, "primary");
        assert_eq!(stamp.files[0].path, CachedPath::from_path(&primary));
        assert_eq!(stamp.files[1].label, "-wal");
        assert_eq!(stamp.files[1].path, CachedPath::from_path(&related));
    }

    #[test]
    fn fingerprint_with_sibling_invalidates_on_sibling_only_change() {
        let dir = TempDir::new().unwrap();
        let primary = dir.path().join("ui_messages.json");
        let sibling = dir.path().join("api_conversation_history.json");
        std::fs::write(&primary, b"[]").unwrap();
        std::fs::write(&sibling, b"<model>claude-sonnet-4</model>").unwrap();

        let sibling_before =
            InputFingerprint::from_path_with_siblings(&primary, ["api_conversation_history.json"])
                .unwrap();
        let plain_before = InputFingerprint::from_path(&primary).unwrap();

        std::fs::write(&sibling, b"<model>claude-opus-4</model>").unwrap();

        let sibling_after =
            InputFingerprint::from_path_with_siblings(&primary, ["api_conversation_history.json"])
                .unwrap();
        let plain_after = InputFingerprint::from_path(&primary).unwrap();

        assert_ne!(sibling_before, sibling_after);
        assert_eq!(plain_before, plain_after);
    }

    #[test]
    fn fingerprint_with_dynamic_dependency_tracks_content_and_existence() {
        let dir = TempDir::new().unwrap();
        let child_dir = dir.path().join("parent-session");
        let primary = child_dir.join("0-ReviewFindings.jsonl");
        let dependency = dir.path().join("parent-session.jsonl");
        std::fs::create_dir_all(&child_dir).unwrap();
        std::fs::write(&primary, b"child").unwrap();

        let policy = InputPolicy::with_dependency(&primary, dependency.clone());
        assert_eq!(policy.paths(), vec![primary.clone(), dependency.clone()]);
        let absent = policy.fingerprint().unwrap();

        std::fs::write(&dependency, b"reviewer").unwrap();
        let reviewer = policy.fingerprint().unwrap();
        assert_ne!(absent, reviewer);

        std::fs::write(&dependency, b"oracle!!").unwrap();
        let oracle = policy.fingerprint().unwrap();
        assert_ne!(reviewer, oracle);

        std::fs::remove_file(&dependency).unwrap();
        assert_eq!(policy.fingerprint().unwrap(), absent);
    }

    #[test]
    fn precomputed_primary_and_dependency_hashes_preserve_fingerprint_identity() {
        let dir = TempDir::new().unwrap();
        let child_dir = dir.path().join("parent-session");
        let primary = child_dir.join("0-ReviewFindings.jsonl");
        let dependency = dir.path().join("parent-session.jsonl");
        std::fs::create_dir_all(&child_dir).unwrap();
        std::fs::write(&primary, b"child").unwrap();
        std::fs::write(&dependency, b"parent").unwrap();
        let policy = InputPolicy::with_dependency(&primary, dependency.clone());
        let snapshot = policy.snapshot().unwrap();

        let ordinary = policy.fingerprint_from_snapshot(&snapshot).unwrap();
        let with_primary = policy
            .fingerprint_from_snapshot_with_primary_hash(
                &snapshot,
                hash_file_contents(&primary).unwrap(),
            )
            .unwrap();
        let with_dependency = policy
            .fingerprint_from_snapshot_with_dependency_hash(
                &snapshot,
                hash_file_contents(&dependency).unwrap(),
            )
            .unwrap();

        assert_eq!(with_primary, ordinary);
        assert_eq!(with_dependency, ordinary);
    }

    #[test]
    fn related_input_stamp_tracks_add_delete_and_mtime_change() {
        let dir = TempDir::new().unwrap();
        let primary = dir.path().join("ui_messages.json");
        let sibling = dir.path().join("api_conversation_history.json");
        std::fs::write(&primary, b"[]").unwrap();
        let policy = InputPolicy::with_siblings(&primary, ["api_conversation_history.json"]);

        let absent = policy.stamp().unwrap();
        std::fs::write(&sibling, b"related").unwrap();
        std::fs::File::open(&sibling)
            .unwrap()
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(UNIX_EPOCH + std::time::Duration::from_secs(10)),
            )
            .unwrap();
        let added = policy.stamp().unwrap();
        assert_ne!(absent, added, "adding a related input must invalidate");

        std::fs::File::open(&sibling)
            .unwrap()
            .set_times(
                std::fs::FileTimes::new()
                    .set_modified(UNIX_EPOCH + std::time::Duration::from_secs(20)),
            )
            .unwrap();
        let mtime_changed = policy.stamp().unwrap();
        assert_ne!(added, mtime_changed, "related mtime must invalidate");

        std::fs::remove_file(&sibling).unwrap();
        let deleted = policy.stamp().unwrap();
        assert_ne!(
            mtime_changed, deleted,
            "deleting a related input must invalidate"
        );
        assert_eq!(absent, deleted);
    }

    #[test]
    fn test_codex_prefix_matches_appended_file() {
        let file = write_temp_file(b"line-1\nline-2\n");
        let fingerprint = InputFingerprint::from_path(file.path()).unwrap();
        let incremental_cache = build_codex_incremental_cache(
            fingerprint.size,
            CodexParseState::default(),
            true,
            fingerprint.content_hash,
        )
        .unwrap();

        let mut reopened = file.reopen().unwrap();
        reopened.seek(SeekFrom::End(0)).unwrap();
        reopened.write_all(b"line-3\n").unwrap();
        reopened.flush().unwrap();

        assert!(codex_prefix_matches(file.path(), &incremental_cache).unwrap());
    }

    #[test]
    fn test_input_fingerprint_changes_for_same_size_rewrite() {
        let file = write_temp_file(b"aaaa\nbbbb\ncccc\n");
        let before = InputFingerprint::from_path(file.path()).unwrap();

        std::fs::write(file.path(), b"aaaa\nzzzz\ncccc\n").unwrap();

        let after = InputFingerprint::from_path(file.path()).unwrap();
        assert_ne!(before, after);
    }

    #[test]
    fn test_input_fingerprint_changes_for_large_same_size_middle_rewrite() {
        let mut original = vec![b'a'; 128 * 1024];
        original.extend_from_slice(b"\n");
        let file = write_temp_file(&original);
        let before = InputFingerprint::from_path(file.path()).unwrap();

        let mut rewritten = original.clone();
        rewritten[73 * 1024] = b'z';
        std::fs::write(file.path(), &rewritten).unwrap();

        let after = InputFingerprint::from_path(file.path()).unwrap();
        assert_ne!(before, after);
    }

    #[test]
    fn test_sqlite_input_fingerprint_tracks_sidecar_changes() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("history.db");
        std::fs::write(&db_path, b"main-db").unwrap();

        let base = InputFingerprint::from_sqlite_path(&db_path).unwrap();

        let wal_path = append_path_suffix(&db_path, "-wal");
        std::fs::write(&wal_path, b"wal-1").unwrap();
        let with_wal = InputFingerprint::from_sqlite_path(&db_path).unwrap();
        assert_ne!(base, with_wal);

        std::fs::write(&wal_path, b"wal-2").unwrap();
        let updated_wal = InputFingerprint::from_sqlite_path(&db_path).unwrap();
        assert_ne!(with_wal, updated_wal);

        let before_shm = InputFingerprint::from_sqlite_path(&db_path).unwrap();
        let shm_path = append_path_suffix(&db_path, "-shm");
        std::fs::write(&shm_path, b"shm-1").unwrap();
        let with_shm = InputFingerprint::from_sqlite_path(&db_path).unwrap();
        assert_eq!(before_shm, with_shm);
    }

    #[test]
    fn test_claude_code_fingerprint_tracks_meta_sidecar_changes() {
        let dir = TempDir::new().unwrap();
        let jsonl_path = dir.path().join("agent-abc123.jsonl");
        std::fs::write(&jsonl_path, b"jsonl-content").unwrap();

        // No meta sidecar → baseline fingerprint
        let base = InputFingerprint::from_claude_code_path(&jsonl_path).unwrap();

        // Add meta sidecar → fingerprint changes
        let meta_path = dir.path().join("agent-abc123.meta.json");
        std::fs::write(&meta_path, br#"{"agentType":"explore"}"#).unwrap();
        let with_meta = InputFingerprint::from_claude_code_path(&jsonl_path).unwrap();
        assert_ne!(
            base, with_meta,
            "Adding meta sidecar should change fingerprint"
        );

        // Update meta sidecar → fingerprint changes again
        std::fs::write(&meta_path, br#"{"agentType":"executor"}"#).unwrap();
        let updated_meta = InputFingerprint::from_claude_code_path(&jsonl_path).unwrap();
        assert_ne!(
            with_meta, updated_meta,
            "Updating meta sidecar should change fingerprint"
        );

        // Main session file (no agent- prefix) → unaffected by unrelated meta files
        let main_path = dir.path().join("session-uuid.jsonl");
        std::fs::write(&main_path, b"main-session").unwrap();
        let main_fp1 = InputFingerprint::from_claude_code_path(&main_path).unwrap();
        // Create a meta file with the main session stem (unlikely in practice)
        let main_meta = dir.path().join("session-uuid.meta.json");
        std::fs::write(&main_meta, br#"{"agentType":"x"}"#).unwrap();
        let main_fp2 = InputFingerprint::from_claude_code_path(&main_path).unwrap();
        assert_ne!(
            main_fp1, main_fp2,
            "Claude Code fingerprints always track .meta.json if it exists"
        );
    }

    #[test]
    fn test_codex_incremental_cache_requires_newline_boundary() {
        let file = write_temp_file(b"line-1\nline-2");

        assert!(build_codex_incremental_cache(
            file.as_file().metadata().unwrap().len(),
            CodexParseState::default(),
            false,
            [0; 32],
        )
        .is_none());
    }

    #[test]
    fn test_codex_prefix_matches_rejects_middle_rewrite_with_same_tail() {
        let file = write_temp_file(b"aaaa\nbbbb\ncccc\n");
        let fingerprint = InputFingerprint::from_path(file.path()).unwrap();
        let incremental_cache = build_codex_incremental_cache(
            fingerprint.size,
            CodexParseState::default(),
            true,
            fingerprint.content_hash,
        )
        .unwrap();

        std::fs::write(file.path(), b"aaaa\nzzzz\ncccc\nmore\n").unwrap();

        assert!(!codex_prefix_matches(file.path(), &incremental_cache).unwrap());
    }

    #[test]
    fn test_codex_prefix_matches_rejects_large_middle_rewrite() {
        let mut original = vec![b'a'; 128 * 1024];
        original.extend_from_slice(b"\n");
        let file = write_temp_file(&original);
        let fingerprint = InputFingerprint::from_path(file.path()).unwrap();
        let incremental_cache = build_codex_incremental_cache(
            fingerprint.size,
            CodexParseState::default(),
            true,
            fingerprint.content_hash,
        )
        .unwrap();

        let mut rewritten = original.clone();
        rewritten[73 * 1024] = b'z';
        rewritten.extend_from_slice(b"appended\n");
        std::fs::write(file.path(), rewritten).unwrap();

        assert!(!codex_prefix_matches(file.path(), &incremental_cache).unwrap());
    }

    #[test]
    #[serial_test::serial]
    fn test_input_message_cache_round_trip() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let file = write_temp_file(b"{}\n");
        let fingerprint = InputFingerprint::from_path(file.path()).unwrap();
        let mut entry = CachedInputEntry::new(
            file.path(),
            fingerprint,
            vec![ParsedMessage::new(
                "gpt-5",
                "provider",
                "session-1",
                1,
                TokenBreakdown {
                    input: 1,
                    output: 2,
                    cache_read: 3,
                    cache_write: 0,
                    reasoning: 0,
                },
                0.0,
            )],
            None,
        );
        entry.rejections.record_key("future-rejection");

        let expected_fingerprint = entry.fingerprint.clone();
        let mut cache = InputMessageCache::load().unwrap();
        cache.insert(entry);
        cache.save_if_dirty().unwrap();

        let shard = shard_path(file.path(), test_decoder_version(1)).unwrap();
        assert!(shard.exists());
        let mut envelope = [0_u8; 12];
        File::open(&shard)
            .unwrap()
            .read_exact(&mut envelope)
            .unwrap();
        assert_eq!(&envelope[..8], &SHARD_MAGIC);
        assert_eq!(
            u32::from_le_bytes(envelope[8..12].try_into().unwrap()),
            CACHE_FORMAT_VERSION
        );

        let mut loaded = InputMessageCache::load().unwrap();
        let meta = loaded
            .get_meta(file.path(), test_decoder_version(1))
            .unwrap()
            .unwrap();
        assert_eq!(meta.fingerprint, expected_fingerprint);
        let rejection = meta.rejections.entries().next().unwrap();
        assert_eq!(rejection.key, "future-rejection");
        assert_eq!(rejection.count, 1);
        let messages = loaded
            .take_messages(&CacheReadPlan::new(
                file.path(),
                test_decoder_version(1),
                expected_fingerprint,
            ))
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].session_id.as_ref(), "session-1");
        assert!(
            serde_json::to_value(&messages[0])
                .unwrap()
                .get("client")
                .is_none(),
            "cached parsed messages must remain source-neutral"
        );

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_write_messages_writes_borrowed_shard_without_dirty_entry() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let file = write_temp_file(b"{}\n");
        let fingerprint = InputFingerprint::from_path(file.path()).unwrap();
        let plan = CacheWritePlan::new(
            file.path(),
            test_decoder_version(3),
            fingerprint.clone(),
            None,
        );
        let messages = vec![ParsedMessage::new(
            "gpt-5",
            "provider",
            "session-1",
            1,
            TokenBreakdown {
                input: 1,
                output: 2,
                cache_read: 0,
                cache_write: 0,
                reasoning: 0,
            },
            0.0,
        )];

        let mut cache = InputMessageCache::load().unwrap();
        cache.write_messages(plan, &messages).unwrap();

        assert!(!cache.dirty);
        assert!(cache.dirty_entries.is_empty());
        let shard = shard_path(file.path(), test_decoder_version(3)).unwrap();
        assert!(shard.exists());

        let mut loaded = InputMessageCache::load().unwrap();
        let meta = loaded
            .get_meta(file.path(), test_decoder_version(3))
            .unwrap()
            .unwrap();
        assert_eq!(meta.fingerprint, fingerprint);
        let restored = loaded
            .take_messages(&CacheReadPlan::new(
                file.path(),
                test_decoder_version(3),
                fingerprint,
            ))
            .unwrap();
        assert_eq!(restored, messages);

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_explicit_prune_removes_orphans_and_old_decoder_revisions() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let live_input = write_temp_file(b"live\n");
        let orphan_input = write_temp_file(b"orphan\n");
        let orphan_path = orphan_input.path().to_path_buf();
        let mut cache = InputMessageCache::load().unwrap();
        for (path, revision) in [
            (live_input.path(), 1),
            (live_input.path(), 3),
            (orphan_input.path(), 2),
        ] {
            cache.insert(CachedInputEntry::new_with_revision(
                path,
                revision,
                InputFingerprint::from_path(path).unwrap(),
                vec![ParsedMessage::new(
                    "gpt-5",
                    "provider",
                    format!("session-{revision}"),
                    1,
                    TokenBreakdown {
                        input: 1,
                        output: 0,
                        cache_read: 0,
                        cache_write: 0,
                        reasoning: 0,
                    },
                    0.0,
                )],
                None,
            ));
        }
        cache.save_if_dirty().unwrap();
        let stale_revision_shard = shard_path(live_input.path(), test_decoder_version(1)).unwrap();
        let current_revision_shard =
            shard_path(live_input.path(), test_decoder_version(3)).unwrap();
        let orphan_shard = shard_path(&orphan_path, test_decoder_version(2)).unwrap();
        assert!(stale_revision_shard.exists());
        assert!(current_revision_shard.exists());
        assert!(orphan_shard.exists());

        drop(orphan_input);
        let stats = prune_input_message_cache().unwrap();

        assert_eq!(
            stats,
            InputCachePruneStats {
                scanned: 3,
                removed: 2,
                retained: 1,
            }
        );
        assert!(!stale_revision_shard.exists());
        assert!(current_revision_shard.exists());
        assert!(!orphan_shard.exists());

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_prune_unknown_magic_classification_error_causes_zero_deletion() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let orphan_input = write_temp_file(b"orphan\n");
        let orphan_path = orphan_input.path().to_path_buf();
        let mut cache = InputMessageCache::load().unwrap();
        cache.insert(CachedInputEntry::new(
            &orphan_path,
            InputFingerprint::from_path(&orphan_path).unwrap(),
            Vec::new(),
            None,
        ));
        cache.save_if_dirty().unwrap();
        let orphan_shard = shard_path(&orphan_path, test_decoder_version(1)).unwrap();
        drop(orphan_input);

        let invalid_shard = cache_dir()
            .unwrap()
            .join(SHARDS_DIRNAME)
            .join("ff")
            .join("invalid.bin");
        ensure_cache_dir(invalid_shard.parent().unwrap()).unwrap();
        let mut file = File::create(&invalid_shard).unwrap();
        file.write_all(&1_u64.to_le_bytes()).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.flush().unwrap();

        let error = prune_input_message_cache().unwrap_err();
        assert!(matches!(error, InputCachePruneError::UnknownMagic { .. }));
        assert!(
            invalid_shard.exists(),
            "unknown-magic classification must preserve the unrecognized shard"
        );
        assert!(
            orphan_shard.exists(),
            "classification must complete before deletion starts"
        );

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_prune_malformed_current_classification_error_causes_zero_deletion() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let invalid_shard = cache_dir()
            .unwrap()
            .join(SHARDS_DIRNAME)
            .join("ff")
            .join("invalid-current.bin");
        ensure_cache_dir(invalid_shard.parent().unwrap()).unwrap();
        let mut file = File::create(&invalid_shard).unwrap();
        file.write_all(&SHARD_MAGIC).unwrap();
        file.write_all(&CACHE_FORMAT_VERSION.to_le_bytes()).unwrap();
        file.write_all(&1_u64.to_le_bytes()).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.flush().unwrap();

        let error = prune_input_message_cache().unwrap_err();
        match &error {
            InputCachePruneError::Decode {
                path,
                format_version,
                source,
            } => {
                assert_eq!(path, &invalid_shard);
                assert_eq!(*format_version, CACHE_FORMAT_VERSION);
                assert!(!source.to_string().is_empty());
            }
            other => panic!("unexpected prune error: {other}"),
        }
        assert!(std::error::Error::source(&error).is_some());
        assert!(
            invalid_shard.exists(),
            "current-format corruption must be preserved"
        );

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_prune_future_format_classification_error_causes_zero_deletion() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());
        let shard_dir = cache_dir().unwrap().join(SHARDS_DIRNAME).join("ff");
        ensure_cache_dir(&shard_dir).unwrap();
        let future_shard = shard_dir.join("future.bin");
        std::fs::write(
            &future_shard,
            [
                SHARD_MAGIC.as_slice(),
                (CACHE_FORMAT_VERSION + 1).to_le_bytes().as_slice(),
            ]
            .concat(),
        )
        .unwrap();

        let error = prune_input_message_cache().unwrap_err();
        assert!(matches!(
            error,
            InputCachePruneError::UnsupportedFormat {
                actual,
                current,
                ..
            } if actual == CACHE_FORMAT_VERSION + 1 && current == CACHE_FORMAT_VERSION
        ));
        assert!(future_shard.exists());

        restore_cache_env(prev_env);
    }

    #[test]
    fn prune_classifier_accepts_current_envelope_and_rejects_unsupported_version() {
        let cache_home = TempDir::new().unwrap();
        let input = write_temp_file(b"primary");
        let decoder_version = test_decoder_version(1);
        let mut cache = InputMessageCache::with_cache_dir(cache_home.path());
        cache.insert(CachedInputEntry::new_with_version(
            input.path(),
            decoder_version,
            InputFingerprint::from_path(input.path()).unwrap(),
            Vec::new(),
            None,
        ));
        cache.save_if_dirty().unwrap();
        let current_shard = shard_path_for_test(cache_home.path(), input.path(), decoder_version);
        assert!(read_shard_header_for_prune(&current_shard)
            .unwrap()
            .is_some());

        let unsupported_version = CACHE_FORMAT_VERSION - 1;
        let unsupported_shard = cache_home.path().join("unsupported.bin");
        let mut file = File::create(&unsupported_shard).unwrap();
        file.write_all(&SHARD_MAGIC).unwrap();
        file.write_all(&unsupported_version.to_le_bytes()).unwrap();
        file.flush().unwrap();
        assert!(matches!(
            read_shard_header_for_prune(&unsupported_shard),
            Err(InputCachePruneError::UnsupportedFormat {
                actual,
                current,
                ..
            }) if actual == unsupported_version && current == CACHE_FORMAT_VERSION
        ));
    }

    #[test]
    #[serial_test::serial]
    fn test_report_load_does_not_prune_orphaned_input_shards() {
        let cache_home = TempDir::new().unwrap();
        let input_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(cache_home.path());

        let input = write_temp_file(b"{}\n");
        let path = input.path().to_path_buf();
        let mut cache = InputMessageCache::load().unwrap();
        cache.insert(CachedInputEntry::new(
            &path,
            InputFingerprint::from_path(&path).unwrap(),
            vec![ParsedMessage::new(
                "gpt-5",
                "openai",
                "session-1",
                1,
                TokenBreakdown {
                    input: 1,
                    output: 0,
                    cache_read: 0,
                    cache_write: 0,
                    reasoning: 0,
                },
                0.0,
            )],
            None,
        ));
        cache.save_if_dirty().unwrap();
        let shard = shard_path(&path, test_decoder_version(1)).unwrap();
        assert!(shard.exists());

        drop(input);
        crate::parse_all_messages_with_pricing(
            input_home.path().to_str().unwrap(),
            &["qwen".to_string()],
            None,
        )
        .unwrap();

        assert!(
            shard.exists(),
            "ordinary generation loads must not perform input-cache garbage collection"
        );

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn load_reports_cache_directory_initialization_failure() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());
        let configured_cache_dir = cache_dir().unwrap();
        std::fs::create_dir_all(configured_cache_dir.parent().unwrap()).unwrap();
        std::fs::write(&configured_cache_dir, b"not-a-directory").unwrap();

        let error = match InputMessageCache::load() {
            Ok(_) => panic!("a cache path occupied by a file must fail initialization"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            InputCacheError::Io {
                operation: "initialize input cache directory",
                path,
                ..
            } if path == configured_cache_dir
        ));

        restore_cache_env(prev_env);
    }

    #[test]
    fn save_reports_invalidated_shard_removal_failure_with_path() {
        let cache_home = TempDir::new().unwrap();
        let input = write_temp_file(b"primary");
        let decoder_version = test_decoder_version(31);
        let shard_path = shard_path_for_test(cache_home.path(), input.path(), decoder_version);
        let mut cache = InputMessageCache::with_cache_dir(cache_home.path());
        ensure_cache_dir(&shard_path).unwrap();
        cache.remove(input.path(), decoder_version);

        let error = cache
            .save_if_dirty()
            .expect_err("removing a directory as a shard must remain an explicit error");
        assert!(matches!(
            error,
            InputCacheError::Io {
                operation: "remove invalid input cache shard",
                path,
                ..
            } if path == shard_path
        ));
    }

    #[test]
    #[serial_test::serial]
    fn test_get_meta_reports_and_preserves_oversized_shard() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let input = write_temp_file(b"input\n");
        let mut seed = InputMessageCache::load().unwrap();
        seed.insert(CachedInputEntry::new_with_revision(
            input.path(),
            1,
            InputFingerprint::from_path(input.path()).unwrap(),
            Vec::new(),
            None,
        ));
        seed.save_if_dirty().unwrap();
        let shard = shard_path(input.path(), test_decoder_version(1)).unwrap();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&shard)
            .unwrap();
        file.set_len(MAX_CACHE_FILE_BYTES + 1).unwrap();

        let loaded = InputMessageCache::load().unwrap();
        let failure = loaded
            .get_meta(input.path(), test_decoder_version(1))
            .expect_err("oversized shard lookup must fail explicitly");
        assert_eq!(failure.input_path, input.path());
        assert_eq!(failure.shard_path, shard);
        assert!(matches!(
            failure.reason,
            CacheReadFailureReason::TooLarge { .. }
        ));
        assert!(shard.exists());

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_get_meta_reports_and_preserves_future_shard_format_version() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let input = write_temp_file(b"input\n");
        let _initialized = InputMessageCache::load().unwrap();
        let shard = shard_path(input.path(), test_decoder_version(1)).unwrap();
        ensure_cache_dir(shard.parent().unwrap()).unwrap();
        let header = CachedShardHeader {
            decoder_version: test_decoder_version(1),
            path: CachedPath::from_path(input.path()),
            fingerprint: InputFingerprint::from_path(input.path()).unwrap(),
            codex_incremental: None,
            message_count: 0,
            rejections: Default::default(),
        };
        let header_bytes = bincode::options().serialize(&header).unwrap();
        let mut file = File::create(&shard).unwrap();
        file.write_all(&SHARD_MAGIC).unwrap();
        file.write_all(&(CACHE_FORMAT_VERSION + 1).to_le_bytes())
            .unwrap();
        file.write_all(&(header_bytes.len() as u64).to_le_bytes())
            .unwrap();
        file.write_all(&header_bytes).unwrap();
        file.flush().unwrap();

        let loaded = InputMessageCache::load().unwrap();
        assert!(loaded
            .get_meta(input.path(), test_decoder_version(1))
            .is_err());
        assert!(shard.exists());

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_same_key_unsupported_envelope_is_preserved_until_successful_current_replacement() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let input = write_temp_file(b"input\n");
        let decoder_version = test_decoder_version(1);
        let fingerprint = InputFingerprint::from_path(input.path()).unwrap();
        let mut seed = InputMessageCache::load().unwrap();
        seed.insert(CachedInputEntry::new_with_version(
            input.path(),
            decoder_version,
            fingerprint.clone(),
            vec![ParsedMessage::new(
                "gpt-5",
                "provider",
                "cached-session",
                1,
                TokenBreakdown::default(),
                0.0,
            )],
            None,
        ));
        seed.save_if_dirty().unwrap();
        let shard = shard_path(input.path(), decoder_version).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&shard)
            .unwrap();
        file.seek(SeekFrom::Start(SHARD_MAGIC.len() as u64))
            .unwrap();
        file.write_all(&UNSUPPORTED_CACHE_FORMAT_VERSION.to_le_bytes())
            .unwrap();
        file.flush().unwrap();
        let original_bytes = std::fs::read(&shard).unwrap();

        let mut loaded = InputMessageCache::load().unwrap();
        assert!(loaded.get_meta(input.path(), decoder_version).is_err());
        assert_eq!(
            std::fs::read(&shard).unwrap(),
            original_bytes,
            "a failed ordinary rebuild must retain the unsupported shard"
        );

        let replacement = vec![ParsedMessage::new(
            "gpt-5",
            "provider",
            "current-session",
            2,
            TokenBreakdown::default(),
            0.0,
        )];
        assert!(loaded
            .write_messages(
                CacheWritePlan::new(input.path(), decoder_version, fingerprint.clone(), None,),
                &replacement,
            )
            .is_ok());
        let bytes = std::fs::read(&shard).unwrap();
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            CACHE_FORMAT_VERSION
        );
        let mut warm = InputMessageCache::load().unwrap();
        let meta = warm
            .get_meta(input.path(), decoder_version)
            .expect("successful atomic replacement must read without error")
            .expect("successful atomic replacement must produce a current-format hit");
        let messages = warm
            .take_messages(&CacheReadPlan::new(
                input.path(),
                decoder_version,
                meta.fingerprint,
            ))
            .unwrap();
        assert_eq!(messages[0].session_id.as_ref(), "current-session");

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_unknown_magic_and_malformed_current_header_are_reported_and_preserved() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());
        let input = write_temp_file(b"input\n");
        let _initialized = InputMessageCache::load().unwrap();

        for (decoder_version, bytes) in [
            (test_decoder_version(11), b"raw-blob".to_vec()),
            (
                test_decoder_version(12),
                [
                    SHARD_MAGIC.as_slice(),
                    CACHE_FORMAT_VERSION.to_le_bytes().as_slice(),
                    1_u64.to_le_bytes().as_slice(),
                    &[0xff],
                ]
                .concat(),
            ),
        ] {
            let shard = shard_path(input.path(), decoder_version).unwrap();
            ensure_cache_dir(shard.parent().unwrap()).unwrap();
            std::fs::write(&shard, &bytes).unwrap();
            let cache = InputMessageCache::load().unwrap();
            assert!(cache.get_meta(input.path(), decoder_version).is_err());
            assert_eq!(std::fs::read(&shard).unwrap(), bytes);
        }

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn failed_atomic_write_does_not_unprotect_unknown_shard() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());
        let input = write_temp_file(b"input\n");
        let decoder_version = test_decoder_version(13);
        let _initialized = InputMessageCache::load().unwrap();
        let shard = shard_path(input.path(), decoder_version).unwrap();
        ensure_cache_dir(shard.parent().unwrap()).unwrap();
        let unknown_bytes = b"unknown!";
        std::fs::write(&shard, unknown_bytes).unwrap();
        let fingerprint = InputFingerprint::from_path(input.path()).unwrap();
        let mut cache = InputMessageCache::load().unwrap();
        assert!(cache.get_meta(input.path(), decoder_version).is_err());
        let real_cache_dir = cache.cache_dir.clone();
        cache.cache_dir = PathBuf::from(OsString::from("invalid\0cache-dir"));

        let error = cache
            .write_messages(
                CacheWritePlan::new(input.path(), decoder_version, fingerprint, None),
                &[ParsedMessage::new(
                    "gpt-5",
                    "provider",
                    "session",
                    1,
                    TokenBreakdown::default(),
                    0.0,
                )],
            )
            .expect_err("invalid cache path must retain its write error");
        assert!(matches!(
            error,
            InputCacheError::Io {
                operation: "initialize input cache directory",
                ..
            }
        ));
        cache.cache_dir = real_cache_dir;
        assert_eq!(std::fs::read(shard).unwrap(), unknown_bytes);

        restore_cache_env(prev_env);
    }

    #[test]
    fn cache_lookup_error_retains_path_version_and_decode_root_cause() {
        let failure = CacheLookupFailure {
            input_path: PathBuf::from("/test/input"),
            decoder_version: test_decoder_version(7),
            shard_path: PathBuf::from("/test/shard"),
            reason: CacheReadFailureReason::HeaderDecode {
                source: Box::new(bincode::ErrorKind::Custom("bad header".to_string())),
            },
        };

        let diagnostic = failure.to_string();
        assert!(diagnostic.contains("/test/input"));
        assert!(diagnostic.contains("/test/shard"));
        assert!(diagnostic.contains(&format!("v{CACHE_FORMAT_VERSION}")));
        assert!(diagnostic.contains("revision: 7"));
        assert!(diagnostic.contains("bad header"));
        assert!(
            std::error::Error::source(&failure)
                .and_then(std::error::Error::source)
                .is_some(),
            "lookup failures must retain the bincode root cause"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_get_meta_ignores_stale_decoder_revision() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let input = write_temp_file(b"input\n");
        let fingerprint = InputFingerprint::from_path(input.path()).unwrap();
        let mut cache = InputMessageCache::load().unwrap();
        cache.insert(CachedInputEntry::new_with_revision(
            input.path(),
            7,
            fingerprint,
            vec![ParsedMessage::new(
                "gpt-5",
                "provider",
                "session-1",
                1,
                TokenBreakdown {
                    input: 1,
                    output: 0,
                    cache_read: 0,
                    cache_write: 0,
                    reasoning: 0,
                },
                0.0,
            )],
            None,
        ));
        cache.save_if_dirty().unwrap();

        let loaded = InputMessageCache::load().unwrap();
        assert!(loaded
            .get_meta(input.path(), test_decoder_version(7))
            .unwrap()
            .is_some());
        assert!(loaded
            .get_meta(input.path(), test_decoder_version(8))
            .unwrap()
            .is_none());

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_get_meta_ignores_stale_decoder_id() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let input = write_temp_file(b"input\n");
        let fingerprint = InputFingerprint::from_path(input.path()).unwrap();
        let mut cache = InputMessageCache::load().unwrap();
        cache.insert(CachedInputEntry::new_with_version(
            input.path(),
            DecoderVersion::new(DecoderId::Copilot, 1),
            fingerprint,
            vec![ParsedMessage::new(
                "gpt-5",
                "provider",
                "session-1",
                1,
                TokenBreakdown {
                    input: 1,
                    output: 0,
                    cache_read: 0,
                    cache_write: 0,
                    reasoning: 0,
                },
                0.0,
            )],
            None,
        ));
        cache.save_if_dirty().unwrap();

        let loaded = InputMessageCache::load().unwrap();
        assert!(loaded
            .get_meta(input.path(), DecoderVersion::new(DecoderId::Copilot, 1))
            .unwrap()
            .is_some());
        assert!(loaded
            .get_meta(input.path(), DecoderVersion::new(DecoderId::Gemini, 1))
            .unwrap()
            .is_none());

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_save_if_dirty_marks_cache_clean() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());
        let mut cache = InputMessageCache::load().unwrap();
        assert!(!cache.dirty);

        {
            let file = write_temp_file(b"{}\n");
            let fingerprint = InputFingerprint::from_path(file.path()).unwrap();
            cache.insert(CachedInputEntry::new(
                file.path(),
                fingerprint,
                Vec::new(),
                None,
            ));
            assert!(cache.dirty);

            cache.save_if_dirty().unwrap();
            assert!(!cache.dirty);
        }

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_save_if_dirty_preserves_disjoint_concurrent_shards() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        {
            let file_one = write_temp_file(b"{\"id\":1}\n");
            let file_two = write_temp_file(b"{\"id\":2}\n");

            let mut writer_one = InputMessageCache::load().unwrap();
            let mut writer_two = InputMessageCache::load().unwrap();

            writer_one.insert(CachedInputEntry::new(
                file_one.path(),
                InputFingerprint::from_path(file_one.path()).unwrap(),
                Vec::new(),
                None,
            ));
            writer_two.insert(CachedInputEntry::new(
                file_two.path(),
                InputFingerprint::from_path(file_two.path()).unwrap(),
                Vec::new(),
                None,
            ));

            writer_one.save_if_dirty().unwrap();
            writer_two.save_if_dirty().unwrap();

            let loaded = InputMessageCache::load().unwrap();
            assert!(loaded
                .get_meta(file_one.path(), test_decoder_version(1))
                .unwrap()
                .is_some());
            assert!(loaded
                .get_meta(file_two.path(), test_decoder_version(1))
                .unwrap()
                .is_some());
            assert!(shard_path(file_one.path(), test_decoder_version(1))
                .unwrap()
                .exists());
            assert!(shard_path(file_two.path(), test_decoder_version(1))
                .unwrap()
                .exists());
        }

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_same_path_different_decoder_versions_use_distinct_shards() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let input = write_temp_file(b"input\n");
        let fingerprint = InputFingerprint::from_path(input.path()).unwrap();
        let copilot_version = DecoderVersion::new(DecoderId::Copilot, 1);
        let gemini_version = DecoderVersion::new(DecoderId::Gemini, 1);
        let mut cache = InputMessageCache::load().unwrap();
        cache.insert(CachedInputEntry::new_with_version(
            input.path(),
            copilot_version,
            fingerprint.clone(),
            vec![ParsedMessage::new(
                "gpt-5",
                "openai",
                "copilot-session",
                1,
                TokenBreakdown {
                    input: 1,
                    output: 0,
                    cache_read: 0,
                    cache_write: 0,
                    reasoning: 0,
                },
                0.0,
            )],
            None,
        ));
        cache.insert(CachedInputEntry::new_with_version(
            input.path(),
            gemini_version,
            fingerprint.clone(),
            vec![ParsedMessage::new(
                "gpt-5",
                "openai",
                "gemini-session",
                1,
                TokenBreakdown {
                    input: 2,
                    output: 0,
                    cache_read: 0,
                    cache_write: 0,
                    reasoning: 0,
                },
                0.0,
            )],
            None,
        ));
        cache.save_if_dirty().unwrap();

        let copilot_shard = shard_path(input.path(), copilot_version).unwrap();
        let gemini_shard = shard_path(input.path(), gemini_version).unwrap();
        assert_ne!(copilot_shard, gemini_shard);
        assert!(copilot_shard.exists());
        assert!(gemini_shard.exists());

        let mut loaded = InputMessageCache::load().unwrap();
        assert!(loaded
            .get_meta(input.path(), copilot_version)
            .unwrap()
            .is_some());
        assert!(loaded
            .get_meta(input.path(), gemini_version)
            .unwrap()
            .is_some());
        let copilot_messages = loaded
            .take_messages(&CacheReadPlan::new(
                input.path(),
                copilot_version,
                fingerprint.clone(),
            ))
            .unwrap();
        let gemini_messages = loaded
            .take_messages(&CacheReadPlan::new(
                input.path(),
                gemini_version,
                fingerprint,
            ))
            .unwrap();
        assert_eq!(copilot_messages[0].session_id.as_ref(), "copilot-session");
        assert_eq!(gemini_messages[0].session_id.as_ref(), "gemini-session");

        restore_cache_env(prev_env);
    }

    #[test]
    #[serial_test::serial]
    fn test_take_messages_revalidates_read_plan_after_shard_rewrite() {
        let temp_home = TempDir::new().unwrap();
        let prev_env = sandbox_cache_env(temp_home.path());

        let input = write_temp_file(b"input-one\n");
        let decoder_version = DecoderVersion::new(DecoderId::Copilot, 1);
        let initial_fingerprint = InputFingerprint::from_path(input.path()).unwrap();
        let mut seed = InputMessageCache::load().unwrap();
        seed.insert(CachedInputEntry::new_with_version(
            input.path(),
            decoder_version,
            initial_fingerprint.clone(),
            vec![ParsedMessage::new(
                "gpt-5",
                "openai",
                "initial-session",
                1,
                TokenBreakdown {
                    input: 1,
                    output: 0,
                    cache_read: 0,
                    cache_write: 0,
                    reasoning: 0,
                },
                0.0,
            )],
            None,
        ));
        seed.save_if_dirty().unwrap();

        let mut reader = InputMessageCache::load().unwrap();
        let meta = reader
            .get_meta(input.path(), decoder_version)
            .unwrap()
            .unwrap();
        let read_plan = CacheReadPlan::new(input.path(), decoder_version, meta.fingerprint);

        std::fs::write(input.path(), b"input-two\n").unwrap();
        let replacement_fingerprint = InputFingerprint::from_path(input.path()).unwrap();
        let mut writer = InputMessageCache::load().unwrap();
        writer.insert(CachedInputEntry::new_with_version(
            input.path(),
            decoder_version,
            replacement_fingerprint,
            vec![ParsedMessage::new(
                "gpt-5",
                "openai",
                "replacement-session",
                2,
                TokenBreakdown {
                    input: 2,
                    output: 0,
                    cache_read: 0,
                    cache_write: 0,
                    reasoning: 0,
                },
                0.0,
            )],
            None,
        ));
        writer.save_if_dirty().unwrap();

        assert!(
            matches!(
                reader.take_messages(&read_plan),
                Err(CacheReadFailure {
                    reason: CacheReadFailureReason::ShardFingerprintMismatch,
                    ..
                })
            ),
            "stale read plan must not return messages from a rewritten shard"
        );
        let replacement_messages = reader
            .take_messages(&CacheReadPlan::new(
                input.path(),
                decoder_version,
                InputFingerprint::from_path(input.path()).unwrap(),
            ))
            .expect("failed stale read plan must not poison the input key");
        assert_eq!(
            replacement_messages[0].session_id.as_ref(),
            "replacement-session"
        );

        restore_cache_env(prev_env);
    }

    #[cfg(unix)]
    #[test]
    fn test_cached_path_preserves_non_utf8_bytes() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let path = PathBuf::from(OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]));
        let cached_path = CachedPath::from_path(&path);

        assert_eq!(cached_path.to_path_buf(), path);
    }
}
