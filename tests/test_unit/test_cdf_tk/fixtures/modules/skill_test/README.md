# Skill test module

Minimal Toolkit module used to validate Atlas AI skill deploy and upload.

## Resources

- `agents/smoke-test-skill.Skill.yaml` — skill metadata (`externalId`)
- `agents/smoke-test-skill.Skill.md` — SKILL.md body with YAML frontmatter

## Usage

```bash
cdf build
cdf deploy
```

The skill content is uploaded via `POST /ai/skills/upload` when deployed.
