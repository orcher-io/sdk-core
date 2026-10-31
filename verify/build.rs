//! Builds the vectors (`vectors/`) into the binary, so an installed
//! `orcher-verify` needs nothing beside it.

use std::path::{Path, PathBuf};

fn main() {
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"))
        .join("vectors");
    let mut files = Vec::new();
    walk(&root, &mut files);
    files.sort();

    let mut out = String::from("/// Every file under `vectors/`, by its path there.\n");
    out.push_str("pub(crate) static FILES: &[(&str, &[u8])] = &[\n");
    for file in &files {
        let name = file
            .strip_prefix(&root)
            .expect("a file under vectors/")
            .components()
            .map(|part| part.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        out.push_str(&format!(
            "    ({name:?}, include_bytes!({:?})),\n",
            file.display().to_string()
        ));
    }
    out.push_str("];\n");
    let dest = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR")).join("vectors.rs");
    std::fs::write(dest, out).expect("write the embedded vectors");
}

/// Every file under `dir`; each directory is watched, so a file added to
/// or removed from it builds again.
fn walk(dir: &Path, files: &mut Vec<PathBuf>) {
    println!("cargo:rerun-if-changed={}", dir.display());
    for entry in std::fs::read_dir(dir).expect("read vectors/") {
        let path = entry.expect("read vectors/").path();
        if path.is_dir() {
            walk(&path, files);
        } else {
            println!("cargo:rerun-if-changed={}", path.display());
            files.push(path);
        }
    }
}
