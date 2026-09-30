$ErrorActionPreference = "Stop"

# Configure Rust toolchain locations for this build.
$env:CARGO_HOME = "D:\devtools\cargo"
$env:RUSTUP_HOME = "D:\devtools\rustup"

# Use the Android NDK linker for the x86_64 Linux/Android target.
$env:CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER = "D:\devtools\android-ndk-r30\toolchains\llvm\prebuilt\windows-x86_64\bin\x86_64-linux-android34-clang.cmd"

cargo build --target x86_64-linux-android --release --locked @args
if ($LASTEXITCODE -ne 0) {
	exit $LASTEXITCODE
}
