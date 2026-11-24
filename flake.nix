{
  description = "lark: TUI music client + modern mpd-style daemon";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAll (
        pkgs:
        let
          lark = pkgs.callPackage ./nix/package.nix { };
        in
        {
          inherit lark;
          default = lark;
        }
      );

      homeManagerModules.default = import ./nix/home-manager.nix {
        packageFor = pkgs: self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      };
      homeManagerModules.lark = self.homeManagerModules.default;

      apps = forAll (pkgs: {
        default = {
          type = "app";
          program = "${self.packages.${pkgs.stdenv.hostPlatform.system}.default}/bin/lark";
        };
        larkd = {
          type = "app";
          program = "${self.packages.${pkgs.stdenv.hostPlatform.system}.default}/bin/larkd";
        };
      });

      formatter = forAll (pkgs: pkgs.nixfmt);

      checks = forAll (pkgs: {
        package = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      });

      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          inputsFrom = [ self.packages.${pkgs.stdenv.hostPlatform.system}.default ];
          packages = with pkgs; [
            rust-analyzer
            clippy
            rustfmt
            just
            python3
          ];
        };
      });
    };
}
