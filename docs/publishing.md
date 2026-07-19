# Publishing the client libraries to Artifactory

Operator guide for [`.github/workflows/publish.yml`](../.github/workflows/publish.yml),
which publishes both client libraries to the on-prem JFrog Artifactory:

| Tag pattern | Package | Source directory | Artifactory repo type |
|-------------|---------|------------------|-----------------------|
| `py-v<X.Y.Z>` | **hs-rabbit-client** (module `hs_rabbit_client`) | `rabbit-client-python/` | local PyPI |
| `ts-v<X.Y.Z>` | **@kafuexe/rabbit-client** | `rabbit-client-typescript/` | npm |

Until the vars/secrets below exist, every run stops at the first ("Guard")
step with a message naming exactly what is missing — the workflow is safe to
merge before Artifactory is ready.

## 1. Artifactory setup (one-time)

1. **Local PyPI repository** (e.g. `pypi-local`) to hold `hs-rabbit-client`.
2. **npm repository** (local, e.g. `npm-local`) to hold `@kafuexe/rabbit-client`.
3. A CI identity (user or access token subject) with **deploy** permission on both repos.

> **IMPORTANT — dependency confusion.** If consumers resolve through a remote
> or virtual PyPI repository that proxies public PyPI, anyone could squat the
> name on pypi.org and have it shadow-resolve into your builds. On the
> public-PyPI remote repo, add an **Excludes Pattern** so these names can
> never be fetched from the internet:
>
> ```
> **/hs-rabbit-client/**, **/hs_rabbit_client/**, **/rabbit-client/**, **/rabbit_client/**
> ```
>
> `hs-rabbit-client` is the real package; excluding the old `rabbit-client`
> name too is cheap defense while consumers migrate. The scoped npm name
> `@kafuexe/rabbit-client` is safer by construction, but if you own the
> `kafuexe` npm org, the same exclusion trick (`@kafuexe/**` on the npmjs
> remote) applies.

## 2. GitHub configuration

Repo **Settings → Secrets and variables → Actions**. Names must match exactly:

| Kind | Name | Value |
|------|------|-------|
| Variable | `ARTIFACTORY_PYPI_URL` | twine upload endpoint, e.g. `https://artifactory.example.com/artifactory/api/pypi/pypi-local` (**no** `/simple` suffix — that is the install URL) |
| Variable | `ARTIFACTORY_NPM_REGISTRY` | npm registry URL, e.g. `https://artifactory.example.com/artifactory/api/npm/npm-local/` |
| Secret | `ARTIFACTORY_USER` | CI username in Artifactory |
| Secret | `ARTIFACTORY_TOKEN` | matching identity/access token (or API key) |

## 3. Cutting a release

1. Bump the version — `rabbit-client-python/pyproject.toml` `version` or
   `rabbit-client-typescript/package.json` `version` — and update that
   project's `CHANGELOG.md`. Commit to `main`.
2. Tag and push (tag version must equal the file version — the workflow
   asserts this and refuses to publish on mismatch):

   ```bash
   # Python
   git tag py-v0.1.0 && git push origin py-v0.1.0
   # TypeScript
   git tag ts-v0.1.0 && git push origin ts-v0.1.0
   ```

3. Watch the **Publish** workflow run for that tag.

Tooling: the Python job uses `python -m build` + `twine upload
--repository-url` — the simplest zero-config Artifactory PyPI flow, matching
the plain-pip toolchain the client's CI already uses (`uv publish` would work
too but adds nothing here).

## 4. Switching consumers from path deps to Artifactory

### Python / uv (e.g. `shared_data_service`)

Delete the path source and pin the published package in `pyproject.toml`:

```toml
# REMOVE:
# [tool.uv.sources]
# rabbit-client = { path = "../rabbit-client-python", editable = true }

[project]
dependencies = [
    "hs-rabbit-client==0.1.0",
]

[[tool.uv.index]]
name = "artifactory"
url = "https://artifactory.example.com/artifactory/api/pypi/pypi-local/simple"

[tool.uv.sources]
hs-rabbit-client = { index = "artifactory" }
```

(The `[tool.uv.sources]` index pin makes uv fetch the package *only* from
Artifactory — extra dependency-confusion insurance on the client side.)

Plain pip equivalent — `pip.conf` (or `~/.config/pip/pip.conf`):

```ini
[global]
index-url = https://artifactory.example.com/artifactory/api/pypi/pypi-local/simple
```

or per-invocation / in CI:

```bash
export PIP_INDEX_URL=https://artifactory.example.com/artifactory/api/pypi/pypi-local/simple
pip install "hs-rabbit-client==0.1.0"
```

### npm

In the consumer's `.npmrc`:

```ini
@kafuexe:registry=https://artifactory.example.com/artifactory/api/npm/npm-local/
```

then `npm install @kafuexe/rabbit-client@0.1.0`.

## 5. After the switch

Once all consumers install from Artifactory instead of local paths, nothing
in this monorepo depends on directory layout anymore — the repo can be split
cleanly into a `rabbit-clients` repo (both client libraries) and a
`shared-data-service` repo, each consuming the published packages.
