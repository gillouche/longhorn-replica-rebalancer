{
  description = "Longhorn Replica Rebalancer";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.python314
            pkgs.uv
            pkgs.pre-commit
          ];

          shellHook = ''
            if [ ! -d ".venv" ]; then
              echo "Creating virtual environment..."
              uv venv
            fi

            source .venv/bin/activate

            echo "Syncing dependencies..."
            uv sync --all-extras --quiet

            if [ ! -f ".git/hooks/pre-commit" ]; then
              echo "Installing pre-commit hooks..."
              pre-commit install --install-hooks > /dev/null
            fi

            echo ""
            echo "Longhorn Replica Rebalancer Development Environment"
            echo "Python: $(python --version)"
            echo "uv: $(uv --version)"
            echo ""
            echo "Run tests: pytest -v"
            echo "Run lints: pre-commit run --all-files"
          '';
        };
      }
    );
}
