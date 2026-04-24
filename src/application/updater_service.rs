use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use semver::Version;
use serde::{Deserialize, Serialize};

use crate::application::ports::cache_store::CacheStore;
use crate::application::ports::updater::{UpdaterError, UpdaterGateway};

pub const UPDATE_STATE_KEY: &str = "update_state";
pub const PENDING_UPGRADE_KEY: &str = "pending_upgrade";
pub const DEFAULT_CHECK_INTERVAL_SECONDS: u64 = 24 * 60 * 60;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpdateState {
    #[serde(default)]
    pub last_checked_at: u64,
    #[serde(default)]
    pub latest_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingUpgrade {
    pub version: String,
    pub staged_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpgradeStatus {
    pub ok: bool,
    pub command: &'static str,
    pub current_version: String,
    pub latest_version: Option<String>,
    pub action: UpgradeAction,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeAction {
    Applied,
    Staged,
    UpToDate,
    Skipped,
}

pub struct UpdaterService {
    updater: Arc<dyn UpdaterGateway>,
    cache_store: Arc<dyn CacheStore>,
    current_version: String,
    staged_dir: PathBuf,
}

impl UpdaterService {
    pub fn new(
        updater: Arc<dyn UpdaterGateway>,
        cache_store: Arc<dyn CacheStore>,
        current_version: String,
        staged_dir: PathBuf,
    ) -> Self {
        Self {
            updater,
            cache_store,
            current_version,
            staged_dir,
        }
    }

    fn staged_path_for(&self, version: &str) -> PathBuf {
        self.staged_dir.join(staged_filename(version))
    }

    pub fn current_version(&self) -> &str {
        &self.current_version
    }

    pub async fn should_check_now(&self, interval_seconds: u64) -> bool {
        let state = self.load_state().await.unwrap_or_default();
        let now = unix_now();
        now.saturating_sub(state.last_checked_at) >= interval_seconds
    }

    pub async fn upgrade_now(&self) -> Result<UpgradeStatus, UpdaterError> {
        let asset_name = current_asset_name()?;
        let release = self.updater.latest_release(asset_name).await?;

        if !is_newer(&self.current_version, &release.version)? {
            self.record_check(Some(release.version.clone())).await;
            return Ok(UpgradeStatus {
                ok: true,
                command: "upgrade",
                current_version: self.current_version.clone(),
                latest_version: Some(release.version),
                action: UpgradeAction::UpToDate,
                message: "already on the latest version".to_string(),
            });
        }

        let bytes = self.updater.download_asset(&release.asset_url).await?;
        let staged_path = self.stage_atomic(&release.version, bytes).await?;
        apply_staged_binary(staged_path).await?;
        self.clear_pending().await;
        self.record_check(Some(release.version.clone())).await;

        Ok(UpgradeStatus {
            ok: true,
            command: "upgrade",
            current_version: self.current_version.clone(),
            latest_version: Some(release.version.clone()),
            action: UpgradeAction::Applied,
            message: format!("upgraded to {}", release.version),
        })
    }

    pub async fn stage_update_if_newer(&self) -> Result<UpgradeStatus, UpdaterError> {
        let asset_name = current_asset_name()?;
        let release = self.updater.latest_release(asset_name).await?;

        if !is_newer(&self.current_version, &release.version)? {
            self.record_check(Some(release.version.clone())).await;
            return Ok(UpgradeStatus {
                ok: true,
                command: "upgrade-check",
                current_version: self.current_version.clone(),
                latest_version: Some(release.version),
                action: UpgradeAction::UpToDate,
                message: "already on the latest version".to_string(),
            });
        }

        if let Some(existing) = self.load_pending().await
            && existing.version == release.version
            && existing.staged_path.exists()
        {
            self.record_check(Some(release.version.clone())).await;
            return Ok(UpgradeStatus {
                ok: true,
                command: "upgrade-check",
                current_version: self.current_version.clone(),
                latest_version: Some(release.version),
                action: UpgradeAction::Staged,
                message: "newer version already staged".to_string(),
            });
        }

        let bytes = self.updater.download_asset(&release.asset_url).await?;
        let staged_path = self.stage_atomic(&release.version, bytes).await?;
        self.save_pending(&PendingUpgrade {
            version: release.version.clone(),
            staged_path,
        })
        .await?;
        self.record_check(Some(release.version.clone())).await;

        Ok(UpgradeStatus {
            ok: true,
            command: "upgrade-check",
            current_version: self.current_version.clone(),
            latest_version: Some(release.version.clone()),
            action: UpgradeAction::Staged,
            message: format!("staged {} for next start", release.version),
        })
    }

    pub async fn apply_pending_upgrade(&self) -> Result<Option<String>, UpdaterError> {
        let Some(pending) = self.load_pending().await else {
            return Ok(None);
        };

        if !pending.staged_path.exists() {
            self.clear_pending().await;
            return Ok(None);
        }

        if !is_newer(&self.current_version, &pending.version).unwrap_or(false) {
            remove_file_best_effort(pending.staged_path).await;
            self.clear_pending().await;
            return Ok(None);
        }

        apply_staged_binary(pending.staged_path).await?;
        self.clear_pending().await;
        Ok(Some(pending.version))
    }

    async fn load_state(&self) -> Option<UpdateState> {
        self.cache_store
            .get_serialized(UPDATE_STATE_KEY)
            .await
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_str::<UpdateState>(&raw).ok())
    }

    async fn record_check(&self, latest_version: Option<String>) {
        let state = UpdateState {
            last_checked_at: unix_now(),
            latest_version,
        };
        if let Ok(serialized) = serde_json::to_string(&state) {
            let _ = self
                .cache_store
                .save_serialized(UPDATE_STATE_KEY.to_string(), serialized)
                .await;
        }
    }

    async fn load_pending(&self) -> Option<PendingUpgrade> {
        self.cache_store
            .get_serialized(PENDING_UPGRADE_KEY)
            .await
            .ok()
            .flatten()
            .and_then(|raw| serde_json::from_str::<PendingUpgrade>(&raw).ok())
    }

    async fn save_pending(&self, pending: &PendingUpgrade) -> Result<(), UpdaterError> {
        let serialized = serde_json::to_string(pending)
            .map_err(|error| UpdaterError::Apply(error.to_string()))?;
        self.cache_store
            .save_serialized(PENDING_UPGRADE_KEY.to_string(), serialized)
            .await
            .map_err(|error| {
                UpdaterError::Apply(format!("failed to record pending upgrade: {error}"))
            })
    }

    async fn clear_pending(&self) {
        let _ = self
            .cache_store
            .save_serialized(PENDING_UPGRADE_KEY.to_string(), "null".to_string())
            .await;
    }

    async fn stage_atomic(&self, version: &str, bytes: Vec<u8>) -> Result<PathBuf, UpdaterError> {
        let staged_dir = self.staged_dir.clone();
        let final_path = self.staged_path_for(version);

        tokio::task::spawn_blocking(move || -> Result<PathBuf, UpdaterError> {
            std::fs::create_dir_all(&staged_dir).map_err(|error| {
                UpdaterError::Apply(format!("failed to create staging dir: {error}"))
            })?;
            let mut temp = tempfile::NamedTempFile::new_in(&staged_dir).map_err(|error| {
                UpdaterError::Apply(format!("failed to create staging tempfile: {error}"))
            })?;
            temp.write_all(&bytes).map_err(|error| {
                UpdaterError::Apply(format!("failed to write staged binary: {error}"))
            })?;
            temp.as_file().sync_all().map_err(|error| {
                UpdaterError::Apply(format!("failed to sync staged binary: {error}"))
            })?;
            temp.persist(&final_path).map_err(|error| {
                UpdaterError::Apply(format!("failed to persist staged binary: {}", error.error))
            })?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&final_path, std::fs::Permissions::from_mode(0o755))
                    .map_err(|error| {
                        UpdaterError::Apply(format!("failed to chmod staged binary: {error}"))
                    })?;
            }

            Ok(final_path)
        })
        .await
        .map_err(|error| UpdaterError::Apply(format!("failed to join staging task: {error}")))?
    }
}

fn staged_filename(version: &str) -> String {
    let sanitized: String = version
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cfg!(windows) {
        format!("staged-{sanitized}.exe")
    } else {
        format!("staged-{sanitized}")
    }
}

pub fn current_asset_name() -> Result<&'static str, UpdaterError> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => Ok("graylog-cli-macos-aarch64"),
        ("linux", "x86_64") => Ok("graylog-cli-linux-x86_64"),
        ("windows", "x86_64") => Ok("graylog-cli-windows-x86_64.exe"),
        (os, arch) => Err(UpdaterError::UnsupportedPlatform(format!("{os}/{arch}"))),
    }
}

pub fn parse_version(raw: &str) -> Result<Version, UpdaterError> {
    let trimmed = raw.trim().trim_start_matches('v');
    Version::parse(trimmed).map_err(|error| UpdaterError::InvalidVersion {
        value: raw.to_string(),
        message: error.to_string(),
    })
}

pub fn is_newer(current: &str, candidate: &str) -> Result<bool, UpdaterError> {
    let current_version = parse_version(current)?;
    let candidate_version = parse_version(candidate)?;
    Ok(candidate_version > current_version)
}

async fn apply_staged_binary(path: PathBuf) -> Result<(), UpdaterError> {
    tokio::task::spawn_blocking(move || {
        self_replace::self_replace(&path)
            .map_err(|error| UpdaterError::Apply(format!("failed to replace binary: {error}")))?;
        let _ = std::fs::remove_file(&path);
        Ok::<(), UpdaterError>(())
    })
    .await
    .map_err(|error| UpdaterError::Apply(format!("failed to join apply task: {error}")))?
}

async fn remove_file_best_effort(path: PathBuf) {
    let _ = tokio::task::spawn_blocking(move || std::fs::remove_file(&path)).await;
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use tempfile::TempDir;

    use crate::application::ports::updater::ReleaseInfo;
    use crate::application::test_support::test_support::FakeCacheStore;

    struct FakeUpdater {
        release: Mutex<ReleaseInfo>,
        payload: Mutex<Vec<u8>>,
        latest_calls: Mutex<Vec<String>>,
        download_calls: Mutex<Vec<String>>,
    }

    impl FakeUpdater {
        fn new(release: ReleaseInfo, payload: Vec<u8>) -> Self {
            Self {
                release: Mutex::new(release),
                payload: Mutex::new(payload),
                latest_calls: Mutex::new(Vec::new()),
                download_calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl UpdaterGateway for FakeUpdater {
        async fn latest_release(&self, asset_name: &str) -> Result<ReleaseInfo, UpdaterError> {
            self.latest_calls
                .lock()
                .expect("latest mutex should not be poisoned")
                .push(asset_name.to_string());
            Ok(self
                .release
                .lock()
                .expect("release mutex should not be poisoned")
                .clone())
        }

        async fn download_asset(&self, url: &str) -> Result<Vec<u8>, UpdaterError> {
            self.download_calls
                .lock()
                .expect("download mutex should not be poisoned")
                .push(url.to_string());
            Ok(self
                .payload
                .lock()
                .expect("payload mutex should not be poisoned")
                .clone())
        }
    }

    fn make_service(
        dir: &Path,
        current: &str,
        release_version: &str,
        payload: Vec<u8>,
    ) -> (UpdaterService, Arc<FakeCacheStore>, Arc<FakeUpdater>) {
        let cache = Arc::new(FakeCacheStore::default());
        let updater = Arc::new(FakeUpdater::new(
            ReleaseInfo {
                version: release_version.to_string(),
                asset_url: "https://example.invalid/binary".to_string(),
                asset_name: "graylog-cli-test".to_string(),
            },
            payload,
        ));
        let service = UpdaterService::new(
            updater.clone(),
            cache.clone(),
            current.to_string(),
            dir.to_path_buf(),
        );
        (service, cache, updater)
    }

    #[test]
    fn parse_version_accepts_v_prefix() {
        assert_eq!(
            parse_version("v0.1.0")
                .expect("version should parse")
                .to_string(),
            "0.1.0"
        );
    }

    #[test]
    fn is_newer_detects_upgrade() {
        assert!(is_newer("0.1.0", "0.2.0").expect("versions should parse"));
    }

    #[test]
    fn is_newer_rejects_equal() {
        assert!(!is_newer("0.1.0", "0.1.0").expect("versions should parse"));
    }

    #[test]
    fn is_newer_rejects_downgrade() {
        assert!(!is_newer("0.2.0", "0.1.0").expect("versions should parse"));
    }

    #[tokio::test]
    async fn should_check_now_true_when_no_state() {
        let dir = TempDir::new().expect("temp dir should create");
        let (service, _, _) = make_service(dir.path(), "0.1.0", "0.1.0", Vec::new());
        assert!(service.should_check_now(3600).await);
    }

    #[tokio::test]
    async fn stage_update_writes_pending_when_newer() {
        let dir = TempDir::new().expect("temp dir should create");
        let (service, cache, _) = make_service(dir.path(), "0.1.0", "0.2.0", vec![1, 2, 3]);
        let status = service
            .stage_update_if_newer()
            .await
            .expect("stage should succeed");
        assert_eq!(status.action, UpgradeAction::Staged);
        assert_eq!(status.latest_version.as_deref(), Some("0.2.0"));
        let expected_path = dir.path().join(staged_filename("0.2.0"));
        assert!(expected_path.exists());
        let pending_raw = cache
            .get_serialized(PENDING_UPGRADE_KEY)
            .await
            .expect("cache read should succeed")
            .expect("pending marker should be present");
        let pending: PendingUpgrade =
            serde_json::from_str(&pending_raw).expect("pending marker should deserialize");
        assert_eq!(pending.version, "0.2.0");
        assert_eq!(pending.staged_path, expected_path);
    }

    #[tokio::test]
    async fn stage_update_uses_versioned_filename() {
        let dir = TempDir::new().expect("temp dir should create");
        let (service, _, _) = make_service(dir.path(), "0.1.0", "0.2.0", vec![0xAAu8; 1024]);
        service
            .stage_update_if_newer()
            .await
            .expect("stage should succeed");
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read staged dir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            entries.iter().any(|name| name.contains("0.2.0")),
            "expected a file with the version in its name, got {entries:?}"
        );
        assert!(
            entries.iter().all(|name| !name.starts_with(".tmp")),
            "tempfile should not be left behind, got {entries:?}"
        );
    }

    #[tokio::test]
    async fn stage_update_skips_when_not_newer() {
        let dir = TempDir::new().expect("temp dir should create");
        let (service, cache, _) = make_service(dir.path(), "0.2.0", "0.2.0", vec![1, 2, 3]);
        let status = service
            .stage_update_if_newer()
            .await
            .expect("stage should succeed");
        assert_eq!(status.action, UpgradeAction::UpToDate);
        let pending = cache
            .get_serialized(PENDING_UPGRADE_KEY)
            .await
            .expect("cache read should succeed");
        assert!(pending.is_none());
    }

    #[tokio::test]
    async fn apply_pending_returns_none_when_absent() {
        let dir = TempDir::new().expect("temp dir should create");
        let (service, _, _) = make_service(dir.path(), "0.1.0", "0.1.0", Vec::new());
        assert_eq!(service.apply_pending_upgrade().await.unwrap(), None);
    }

    #[tokio::test]
    async fn apply_pending_clears_marker_when_staged_file_missing() {
        let dir = TempDir::new().expect("temp dir should create");
        let (service, cache, _) = make_service(dir.path(), "0.1.0", "0.2.0", Vec::new());
        service
            .save_pending(&PendingUpgrade {
                version: "0.2.0".to_string(),
                staged_path: dir.path().join("does-not-exist"),
            })
            .await
            .expect("save pending should succeed");
        assert_eq!(service.apply_pending_upgrade().await.unwrap(), None);
        let pending_raw = cache
            .get_serialized(PENDING_UPGRADE_KEY)
            .await
            .expect("cache read should succeed");
        // cleared sentinel is "null"
        assert_eq!(pending_raw.as_deref(), Some("null"));
    }
}
