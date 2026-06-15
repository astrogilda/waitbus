# waitbus Homebrew formula — SKELETON. First publish at v0.4.1.
#
# Tap path: astrogilda/homebrew-waitbus
# Local test: brew install --build-from-source ./packaging/homebrew/waitbus.rb
#
# RESOURCE STANZAS: the `resource` blocks below are placeholders with
# REPLACE_ME hashes. Before any tap publish, regenerate the full pinned
# set with:
#     brew update-python-resources Formula/waitbus.rb
# That command rewrites every runtime dependency as a pinned `resource`
# with a real sha256. Do not hand-fabricate hashes.

class Waitbus < Formula
  include Language::Python::Virtualenv

  desc "Workstation-local GitHub Actions status reporter + broadcast bus + MCP server"
  homepage "https://github.com/astrogilda/waitbus"
  # url + sha256 point at the PyPI sdist. REPLACE_ME at publish time.
  url "https://files.pythonhosted.org/packages/source/c/waitbus/waitbus-0.4.0.tar.gz"
  sha256 "REPLACE_ME_SDIST_SHA256"
  license "MIT"

  # Versioned dependency — NEVER unversioned `python`. Python 3.14 has
  # Pydantic-V1 breakage; 3.13 is the supported production interpreter.
  depends_on "python@3.13"

  # --- pinned runtime resources (regenerate before publish) -------------
  # Example structure only; `brew update-python-resources` fills the
  # real set + hashes for mcp, msgspec, platformdirs, prometheus_client,
  # pydantic-settings, stamina, typer and their transitive closure.
  resource "typer" do
    url "https://files.pythonhosted.org/packages/source/t/typer/typer-0.15.0.tar.gz"
    sha256 "REPLACE_ME_TYPER_SHA256"
  end

  resource "msgspec" do
    url "https://files.pythonhosted.org/packages/source/m/msgspec/msgspec-0.19.0.tar.gz"
    sha256 "REPLACE_ME_MSGSPEC_SHA256"
  end

  resource "platformdirs" do
    url "https://files.pythonhosted.org/packages/source/p/platformdirs/platformdirs-4.9.0.tar.gz"
    sha256 "REPLACE_ME_PLATFORMDIRS_SHA256"
  end

  def install
    virtualenv_install_with_resources
  end

  # PyPI-published: livecheck against PyPI, never the GitHub API.
  livecheck do
    url :stable
    strategy :pypi
  end

  def caveats
    <<~EOS
      Secret storage is platform-specific:
        - Linux: systemd-creds (waitbus install-credentials).
        - macOS: launchd + Keychain. Stage the broadcast token with
          `security add-generic-password` and the daemon reads it via
          `security find-generic-password`. The systemd-creds path is
          Linux-only.

      The supervisor units are launchd LaunchAgents on macOS; run
      `waitbus install-launchd` to stage them.
    EOS
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/waitbus --version")
  end
end
