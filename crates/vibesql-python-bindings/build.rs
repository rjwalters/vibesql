use std::env;
use std::process::Command;

fn main() {
    // Only configure linking when building as a cdylib (Python extension)
    // Skip when building for documentation or other targets
    if env::var("CARGO_CFG_TARGET_OS").is_err() {
        return;
    }

    // Get Python configuration using python3-config
    let python_config = if cfg!(target_os = "macos") {
        // On macOS, we need special handling for Python linking
        get_python_config_macos()
    } else {
        get_python_config_unix()
    };

    if let Some(config) = python_config {
        println!("cargo:rustc-link-search=native={}", config.lib_dir);
        for lib in config.libs {
            println!("cargo:rustc-link-lib={}", lib);
        }
    }
}

struct PythonConfig {
    lib_dir: String,
    libs: Vec<String>,
}

fn get_python_config_macos() -> Option<PythonConfig> {
    // Try to get ldflags from python3-config
    let output = Command::new("python3-config")
        .arg("--ldflags")
        .arg("--embed")
        .output()
        .or_else(|_| Command::new("python3-config").arg("--ldflags").output())
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let ldflags = String::from_utf8(output.stdout).ok()?;

    // Parse -L and -l flags
    let mut lib_dir = String::new();
    let mut libs = Vec::new();

    for flag in ldflags.split_whitespace() {
        if let Some(path) = flag.strip_prefix("-L") {
            lib_dir = path.to_string();
        } else if let Some(lib) = flag.strip_prefix("-l") {
            libs.push(lib.to_string());
        }
    }

    if lib_dir.is_empty() {
        None
    } else {
        Some(PythonConfig { lib_dir, libs })
    }
}

fn get_python_config_unix() -> Option<PythonConfig> {
    // For non-macOS Unix systems, use python3-config
    let output = Command::new("python3-config").arg("--ldflags").output().ok()?;

    if !output.status.success() {
        return None;
    }

    let ldflags = String::from_utf8(output.stdout).ok()?;

    let mut lib_dir = String::new();
    let mut libs = Vec::new();

    for flag in ldflags.split_whitespace() {
        if let Some(path) = flag.strip_prefix("-L") {
            lib_dir = path.to_string();
        } else if let Some(lib) = flag.strip_prefix("-l") {
            libs.push(lib.to_string());
        }
    }

    if lib_dir.is_empty() {
        None
    } else {
        Some(PythonConfig { lib_dir, libs })
    }
}
