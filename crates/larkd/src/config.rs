use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum OutputKind {
    /// System default device via cpal.
    #[default]
    Default,
    /// Discard audio at real-time rate (headless testing).
    Null,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub output: OutputKind,
    #[serde(default = "default_plugins", rename = "plugin")]
    pub plugins: Vec<PluginConfig>,
    #[serde(default)]
    pub socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PluginConfig {
    pub name: String,
    /// argv. A bare program name is looked up next to `larkd` first, then on PATH.
    pub exec: Vec<String>,
    #[serde(default)]
    pub config: Value,
    #[serde(default = "yes")]
    pub enabled: bool,
}

fn yes() -> bool {
    true
}

fn default_plugins() -> Vec<PluginConfig> {
    vec![PluginConfig {
        name: "local".into(),
        exec: vec!["lark-source-local".into()],
        config: serde_json::json!({ "music_dir": "~/Music" }),
        enabled: true,
    }]
}

impl Default for Config {
    fn default() -> Self {
        Self {
            output: OutputKind::Default,
            plugins: default_plugins(),
            socket: None,
        }
    }
}

impl Config {
    pub fn default_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("lark/config.toml")
    }

    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(s) => toml::from_str(&s).with_context(|| format!("parsing {}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }
}

pub fn data_dir() -> PathBuf {
    dirs::data_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("lark")
}

pub fn state_dir() -> PathBuf {
    dirs::state_dir().unwrap_or_else(data_dir).join("lark")
}

/// Resolve a plugin executable: sibling of the current exe, else PATH lookup.
pub fn resolve_exec(prog: &str) -> PathBuf {
    let p = Path::new(prog);
    if p.components().count() > 1 {
        return p.to_path_buf();
    }
    if let Ok(me) = std::env::current_exe() {
        if let Some(dir) = me.parent() {
            let cand = dir.join(prog);
            if cand.is_file() {
                return cand;
            }
        }
    }
    p.to_path_buf()
}
