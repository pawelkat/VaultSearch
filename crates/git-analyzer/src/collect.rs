use std::path::{Component, Path, PathBuf};
use std::process::Command;

use crate::error::Error;

/// Same byte ceiling as the vault indexer.
pub const CODE_MAX_FILE_BYTES: usize = 1_000_000;

const LOCKFILES: &[&str] = &[
    "Cargo.lock",
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "bun.lock",
    "bun.lockb",
    "Gemfile.lock",
    "poetry.lock",
    "composer.lock",
    "Package.resolved",
    "packages.lock.json",
    "go.sum",
    "uv.lock",
    "Pipfile.lock",
    "flake.lock",
    "pubspec.lock",
    "gradle.lockfile",
    "npm-shrinkwrap.json",
];

const SECRET_NAMES: &[&str] = &[
    "id_rsa",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "credentials",
    "credentials.json",
    "secrets.json",
    "secrets.yaml",
    "secrets.yml",
    ".netrc",
    ".envrc",
    "service-account.json",
    "auth.json",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedFile {
    /// Directory name, or the path of the repo relative to the projects root.
    pub repo: String,
    /// Path inside the repo, as `git ls-files` prints it.
    pub path: String,
    pub full_path: PathBuf,
    pub mtime_ns: i64,
}

/// Tracked working-tree files under `root`. Untracked and ignored files are absent.
/// Secret-like names and lockfiles are dropped even when they are tracked.
pub fn collect_tracked_files(root: &Path) -> Result<Vec<TrackedFile>, Error> {
    let root = std::fs::canonicalize(root)?;
    if !root.is_dir() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            "projects root is not a directory",
        )));
    }
    let mut repos = Vec::new();
    discover(&root, &mut repos)?;
    repos.sort();
    let mut files = Vec::new();
    for repo_root in repos {
        let repo_root = std::fs::canonicalize(&repo_root)?;
        if !repo_root.starts_with(&root) {
            continue;
        }
        let repo = repo_label(&root, &repo_root);
        for rel in git_ls_files(&repo_root)? {
            if !indexable_code_path(&rel) {
                continue;
            }
            let full = repo_root.join(&rel);
            let meta = match std::fs::symlink_metadata(&full) {
                Ok(meta) => meta,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            if !meta.file_type().is_file() || meta.len() > CODE_MAX_FILE_BYTES as u64 {
                continue;
            }
            let Ok(canonical) = std::fs::canonicalize(&full) else {
                continue;
            };
            if !canonical.starts_with(&repo_root) {
                continue;
            }
            files.push(TrackedFile {
                repo: repo.clone(),
                path: rel,
                full_path: canonical,
                mtime_ns: mtime_ns(&meta),
            });
        }
    }
    files.sort_by(|a, b| (&a.repo, &a.path).cmp(&(&b.repo, &b.path)));
    files.dedup_by(|a, b| a.repo == b.repo && a.path == b.path);
    Ok(files)
}

/// `None` for a NUL byte, a file over 1 MB, or bytes that are not UTF-8.
/// A missing file is also `None` (deleted between the listing and the read).
pub fn read_code_text(path: &Path) -> Result<Option<String>, Error> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    if bytes.len() > CODE_MAX_FILE_BYTES || bytes.contains(&0) {
        return Ok(None);
    }
    Ok(String::from_utf8(bytes).ok())
}

/// Repo-relative git path. Allows `.github`. Rejects `.git`, `..`, lockfiles, and secret names.
pub fn indexable_code_path(rel: &str) -> bool {
    if rel.is_empty() || rel.starts_with('/') || rel.contains('\\') || rel.contains('\0') {
        return false;
    }
    let mut saw_file = false;
    for component in Path::new(rel).components() {
        match component {
            Component::Normal(name) => {
                let name = name.to_string_lossy();
                if name.is_empty() || name == ".git" || is_lockfile(&name) || is_secret_name(&name)
                {
                    return false;
                }
                saw_file = true;
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return false,
        }
    }
    saw_file
}

fn is_lockfile(name: &str) -> bool {
    LOCKFILES.contains(&name)
}

fn is_secret_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    if lower == ".env" || lower.starts_with(".env.") {
        return true;
    }
    if SECRET_NAMES.contains(&lower.as_str()) {
        return true;
    }
    if lower.starts_with("id_rsa")
        || lower.starts_with("id_dsa")
        || lower.starts_with("id_ecdsa")
        || lower.starts_with("id_ed25519")
    {
        return true;
    }
    matches!(
        extension(&lower),
        Some("pem" | "p12" | "pfx" | "keystore" | "jks" | "kdbx")
    ) || lower.ends_with(".key")
}

fn extension(name: &str) -> Option<&str> {
    name.rsplit_once('.')
        .map(|(_, ext)| ext)
        .filter(|ext| !ext.is_empty())
}

fn discover(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), Error> {
    let git = dir.join(".git");
    if git.is_dir() || git.is_file() {
        out.push(dir.to_path_buf());
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_symlink() || !file_type.is_dir() {
            continue;
        }
        if entry.file_name() == ".git" {
            continue;
        }
        discover(&entry.path(), out)?;
    }
    Ok(())
}

fn repo_label(root: &Path, repo_root: &Path) -> String {
    if repo_root == root {
        return repo_root
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "repo".to_string());
    }
    match repo_root.strip_prefix(root) {
        Ok(rel) => {
            let rel = rel.to_string_lossy().replace('\\', "/");
            if rel.is_empty() {
                "repo".to_string()
            } else {
                rel
            }
        }
        Err(_) => "repo".to_string(),
    }
}

fn git_ls_files(repo: &Path) -> Result<Vec<String>, Error> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["ls-files", "-z", "--"])
        .output()
        .map_err(|err| Error::Git(format!("git ls-files: {err}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::Git(format!(
            "git ls-files failed in {}: {}",
            repo.display(),
            stderr.trim()
        )));
    }
    let mut files = Vec::new();
    for raw in output.stdout.split(|byte| *byte == 0) {
        if raw.is_empty() {
            continue;
        }
        let path = String::from_utf8(raw.to_vec())
            .map_err(|_| Error::Git("git ls-files path is not utf-8".to_string()))?;
        files.push(path);
    }
    Ok(files)
}

fn mtime_ns(meta: &std::fs::Metadata) -> i64 {
    meta.modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_filter_keeps_github_and_drops_secrets() {
        assert!(indexable_code_path(".github/workflows/ci.yml"));
        assert!(indexable_code_path("src/lib.rs"));
        assert!(indexable_code_path("Makefile"));
        assert!(!indexable_code_path(".git/config"));
        assert!(!indexable_code_path("src/.env"));
        assert!(!indexable_code_path(".env.local"));
        assert!(!indexable_code_path("keys/id_ed25519"));
        assert!(!indexable_code_path("keys/id_ed25519.pub"));
        assert!(!indexable_code_path("certs/server.pem"));
        assert!(!indexable_code_path("Cargo.lock"));
        assert!(!indexable_code_path("app/package-lock.json"));
        assert!(!indexable_code_path("../src/lib.rs"));
        assert!(!indexable_code_path("/etc/passwd"));
        assert!(!indexable_code_path(""));
    }

    #[test]
    fn nul_and_oversize_bytes_are_not_text() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("a.rs");
        std::fs::write(&binary, b"fn a() {}\0").unwrap();
        assert_eq!(read_code_text(&binary).unwrap(), None);
        let huge = dir.path().join("huge.rs");
        std::fs::write(&huge, vec![b'a'; CODE_MAX_FILE_BYTES + 1]).unwrap();
        assert_eq!(read_code_text(&huge).unwrap(), None);
        let text = dir.path().join("ok.rs");
        std::fs::write(&text, "fn ok() {}\n").unwrap();
        assert_eq!(
            read_code_text(&text).unwrap().as_deref(),
            Some("fn ok() {}\n")
        );
    }

    #[test]
    fn git_ls_files_skips_untracked_ignored_and_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let repo = root.join("demo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::create_dir_all(repo.join(".github/workflows")).unwrap();
        std::fs::write(repo.join("src/lib.rs"), "pub fn answer() -> i32 { 27 }\n").unwrap();
        std::fs::write(repo.join(".github/workflows/ci.yml"), "name: ci\n").unwrap();
        std::fs::write(repo.join(".env"), "SECRET=1\n").unwrap();
        std::fs::write(repo.join("Cargo.lock"), "lock\n").unwrap();
        std::fs::write(repo.join("src/blob.rs"), b"fn blob() {}\0").unwrap();
        std::fs::write(repo.join(".gitignore"), "ignored.txt\n").unwrap();
        std::fs::write(repo.join("ignored.txt"), "nope\n").unwrap();
        std::fs::write(repo.join("untracked.txt"), "nope\n").unwrap();
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "git-analyzer@example.com"]);
        git(&repo, &["config", "user.name", "git-analyzer"]);
        git(
            &repo,
            &[
                "add",
                "src/lib.rs",
                "src/blob.rs",
                ".github/workflows/ci.yml",
                ".env",
                "Cargo.lock",
                ".gitignore",
            ],
        );
        git(&repo, &["commit", "-m", "init"]);

        let files = collect_tracked_files(root).unwrap();
        let paths: Vec<_> = files.iter().map(|file| file.path.as_str()).collect();
        assert!(paths.contains(&"src/lib.rs"), "{paths:?}");
        assert!(paths.contains(&".github/workflows/ci.yml"), "{paths:?}");
        assert!(paths.contains(&"src/blob.rs"), "{paths:?}");
        assert!(!paths.iter().any(|path| path.contains(".env")), "{paths:?}");
        assert!(!paths.iter().any(|path| path.ends_with("Cargo.lock")));
        assert!(!paths.iter().any(|path| path.contains("untracked")));
        assert!(!paths.iter().any(|path| path.contains("ignored.txt")));
        assert!(!paths.iter().any(|path| path.contains(".git/")));
        assert!(files.iter().all(|file| file.repo == "demo"));
        let blob = files
            .iter()
            .find(|file| file.path == "src/blob.rs")
            .unwrap();
        assert_eq!(read_code_text(&blob.full_path).unwrap(), None);
    }

    fn git(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .status()
            .expect("git");
        assert!(status.success(), "git {args:?} failed");
    }
}
