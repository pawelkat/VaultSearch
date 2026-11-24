#!/usr/bin/env python3
from importlib.machinery import SourceFileLoader
from pathlib import Path

m = SourceFileLoader(
    "vault_md5sync", str(Path(__file__).with_name("vault-md5sync"))
).load_module()


def test_normalize_path_keeps_hidden_dir():
    assert m.normalize_path(".grok/skills/vault-search/SKILL.md") == ".grok/skills/vault-search/SKILL.md"
    assert m.normalize_path("./.grok/skills/SKILL.md") == ".grok/skills/SKILL.md"
    assert m.normalize_path("./Notes/foo.md") == "Notes/foo.md"
    assert m.normalize_path("AGENTS.md") == "AGENTS.md"
    # the old bug:
    assert ".grok/skills/x.md".lstrip("./") == "grok/skills/x.md"


def test_parse_md5sum_hidden_and_dot_slash():
    text = (
        "bfb832cfc6124f9f27ae1dffbe40e16a  .grok/skills/grok-build-vault-task/SKILL.md\n"
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  ./Notes/foo.md\n"
    )
    got = m.parse_md5sum(text)
    assert ".grok/skills/grok-build-vault-task/SKILL.md" in got
    assert "Notes/foo.md" in got
    assert "grok/skills/grok-build-vault-task/SKILL.md" not in got


def test_state_lookup_migrates_stripped_dot():
    prev = {"grok/skills/vault-search/SKILL.md": "abc"}
    assert m.state_lookup(prev, ".grok/skills/vault-search/SKILL.md") == "abc"
    assert m.state_lookup(prev, "AGENTS.md") is None
    assert m.state_lookup({"AGENTS.md": "def"}, "AGENTS.md") == "def"


def test_plan_push_after_dot_key_migration():
    old = "3a5b00d0c37e3f660c1189b8f7fcc5ea"
    new = "d8c1d3fde94613e5626575ea337f081f"
    path = ".grok/skills/vault-search/SKILL.md"
    actions = m.plan_actions(
        {path: {new}},
        {path: {old}},
        {"files": {"grok/skills/vault-search/SKILL.md": old}, "acked_remote": {}},
    )
    assert actions == [{"op": "push", "path": path}]


if __name__ == "__main__":
    test_normalize_path_keeps_hidden_dir()
    test_parse_md5sum_hidden_and_dot_slash()
    test_state_lookup_migrates_stripped_dot()
    test_plan_push_after_dot_key_migration()
    print("OK")
