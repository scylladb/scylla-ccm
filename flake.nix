{
  description = "A script/library to create, launch and remove an Scylla / Apache Cassandra cluster on
localhost.";

  inputs = {
    nixpkgs.url = "nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    let
      prepare_python_requirements = python: python.withPackages (ps: with ps; [ pytest ruamel-yaml psutil six requests packaging boto3 tqdm setuptools ]);
      make_ccm_package = {python, jdk, pkgs}: python.pkgs.buildPythonApplication {
        pname = "scylla_ccm";
        version = "0.1";
        format = "pyproject";
        src = ./. ;

        checkInputs = with python.pkgs; [ pytestCheckHook ];
        buildInputs = [ pkgs.makeWrapper ];
        propagatedBuildInputs =  [ (prepare_python_requirements python) jdk];
        doCheck = false;

        disabledTestPaths = [ "old_tests/*.py" ];

        # Make `nix run` aware that the binary is called `ccm`.
        meta.mainProgram = "ccm";

        postInstall = ''
          wrapProgram $out/bin/ccm \
            --set JAVA_HOME "${jdk}"
        '';
      };
    in
    flake-utils.lib.eachDefaultSystem (system:
      let 
        pkgs = nixpkgs.legacyPackages.${system};
      in
      rec {
        packages = rec {
          scylla_ccm = python: make_ccm_package { inherit python pkgs ; jdk = pkgs.jdk11; };
          default = scylla_ccm pkgs.python311;
        };
        devShell =
          pkgs.mkShell {
            buildInputs = [ pkgs.poetry pkgs.glibcLocales (prepare_python_requirements pkgs.python39) (prepare_python_requirements pkgs.python311) pkgs.jdk11];
            shellHook = ''
              set JAVA_HOME ${pkgs.jdk11}
            '';
        };
      }
    ) // rec {
      overlays.default = final: prev: {
        scylla_ccm = python: make_ccm_package { inherit python; jdk = final.jdk11; pkgs = final; };
      };
    };
}
