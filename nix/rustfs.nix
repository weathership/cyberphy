# RustFS package for cyberphy devenv — includes the web console.
#
# The upstream flake (`github:rustfs/rustfs`) builds the server binary only
# and does not fetch `rustfs/static` before cargo (see build-rustfs.sh
# `--with-console`). Without those assets rust_embed serves 404 at
# /rustfs/console/ (github.com/rustfs/rustfs/issues/4919).
#
# Official release zips are produced with console assets embedded, so we
# package the musl static release as pkgs.rustfs for a working S3 API + UI.
#
# Override later if you need a from-source build: prefetch console zip into
# rustfs/static then cargo build (same as build-rustfs.sh --with-console).
{ pkgs, system ? pkgs.stdenv.hostPlatform.system }:

let
  version = "1.0.0-beta.11";

  # musl static-pie → runs without host glibc version pin
  asset =
    {
      "x86_64-linux" = {
        name = "rustfs-linux-x86_64-musl-v${version}.zip";
        hash = "sha256-E3vLSdlLGdB5aISVnjuITkxbcBxEGsClXsrc0ywNIyA=";
      };
      # aarch64 release asset when needed — pin hash via nix-prefetch-url
      "aarch64-linux" = {
        name = "rustfs-linux-aarch64-musl-v${version}.zip";
        hash = pkgs.lib.fakeHash;
      };
    }.${system}
      or (throw "nix/rustfs.nix: unsupported system ${system} (add release zip hash)");
in
pkgs.stdenvNoCC.mkDerivation {
  pname = "rustfs";
  inherit version;

  src = pkgs.fetchurl {
    url = "https://github.com/rustfs/rustfs/releases/download/${version}/${asset.name}";
    inherit (asset) hash;
  };

  nativeBuildInputs = [ pkgs.unzip ];

  # Zip is a single binary, not a directory tree — skip default unpack.
  dontUnpack = true;
  dontConfigure = true;
  dontBuild = true;

  installPhase = ''
    runHook preInstall
    mkdir -p $out/bin
    # zip root contains `rustfs` binary (console UI is rust_embed'd)
    unzip -j -o $src 'rustfs' -d $out/bin
    chmod +x $out/bin/rustfs
    runHook postInstall
  '';

  meta = with pkgs.lib; {
    description = "RustFS S3-compatible object storage (release binary with web console)";
    homepage = "https://rustfs.com";
    license = licenses.asl20;
    mainProgram = "rustfs";
    platforms = [
      "x86_64-linux"
      "aarch64-linux"
    ];
    sourceProvenance = [ sourceTypes.binaryNativeCode ];
  };
}
