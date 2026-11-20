//! Embeds the git short sha of the checkout (`VAULT_EVENTS_GIT`) so a
//! running agent can report its build in presence replies.
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let o = Command::new("git").args(args).output().ok()?;
    o.status
        .success()
        .then(|| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

fn main() {
    let sha = git(&["rev-parse", "--short=7", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let dirty = git(&["status", "--porcelain", "--untracked-files=no", "--", "."])
        .is_some_and(|s| !s.is_empty());
    let v = if dirty { format!("{sha}-dirty") } else { sha };
    println!("cargo:rustc-env=VAULT_EVENTS_GIT={v}");
    for p in ["HEAD", "logs/HEAD", "index"] {
        if let Some(f) = git(&["rev-parse", "--git-path", p]) {
            println!("cargo:rerun-if-changed={f}");
        }
    }
    println!("cargo:rerun-if-changed=build.rs");
}
