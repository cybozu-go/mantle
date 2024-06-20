# Release procedure

This document describes how to release a new version of Mantle.

## Versioning

Follow [semantic versioning 2.0.0][semver] to choose the new version number.

## The format of release notes

In the release procedure for both the app and Helm Chart, the release note is generated automatically,
and then it is edited manually. In this step, PRs should be classified based on [Keep a CHANGELOG](https://keepachangelog.com/en/1.1.0/).

The result should look something like:

```markdown
## What's Changed

### Added

* Add a notable feature for users (#35)

### Changed

* Change a behavior affecting users (#33)

### Removed

* Remove a feature, users action required (#39)

### Fixed

* Fix something not affecting users or a minor change (#40)
```

## Bump version

1. Determine a new version number by [checking the differences](https://github.com/cybozu-go/mantle/compare/vX.Y.Z...main) since the last release. Then, define the `VERSION` variable.

    ```sh
    VERSION=1.2.3
    ```

2. Add a new tag and push it.

    ```sh
    git switch main
    git pull
    git tag v${VERSION}
    git push origin v${VERSION}
    ```

3. Once a new tag is pushed, [GitHub Actions][] automatically
   creates a draft release note for the tagged version.
   
   Visit https://github.com/cybozu-go/mantle/releases to check
   the result. 

4. Edit the auto-generated release note
   and remove PRs which contain changes only to the helm chart.
   Then, publish it.

## Bump Chart Version

Mantle Helm Chart can be released independently.
This prevents the Mantle version from going up just by modifying the Helm Chart.
Also, the Helm Charts `mantle` and `mantle-cluster-wide` can be released independently.
If you want to release both charts, please follow the instructions for each of them.

1. Determine a new version number. Then, define `APPVERSION` and `CHARTVERSION` variables:
   ```sh
   APPVERSION=1.2.3
   CHARTVERSION=4.5.6
   ```

2. Make a branch for the release as follows:
   ```sh
   git switch main
   git pull

   # For charts/mantle
   git switch -c bump-mantle-chart-${CHARTVERSION}

   # For charts/mantle-cluster-wide
   git switch -c bump-mantle-cluster-wide-chart-${CHARTVERSION}
   ```

3. Update versions in the `Chart.yaml` file.
   ```sh
   # For charts/mantle
   sed -r -i "s/appVersion: \"[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+\"/appVersion: \"${APPVERSION}\"/g" charts/mantle/Chart.yaml
   sed -r -i "s/^version: [[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+/version: ${CHARTVERSION}/g" charts/mantle/Chart.yaml

   # For charts/mantle-cluster-wide
   sed -r -i "s/appVersion: \"[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+\"/appVersion: \"${APPVERSION}\"/g" charts/mantle-cluster-wide/Chart.yaml
   sed -r -i "s/^version: [[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+/version: ${CHARTVERSION}/g" charts/mantle-cluster-wide/Chart.yaml
   ```

4. Commit the change and create a pull request:
   ```sh
   # For charts/mantle
   git commit -a -s -m "Bump mantle chart version to ${CHARTVERSION}"
   git switch -c bump-mantle-chart-${CHARTVERSION}

   # For charts/mantle-cluster-wide
   git commit -a -s -m "Bump mantle-cluster-wide chart version to ${CHARTVERSION}"
   bump-mantle-cluster-wide-chart-${CHARTVERSION}
   ```

5. Create a new pull request and merge it.

6. Manually run the GitHub Actions workflow for the release.

   https://github.com/cybozu-go/mantle/actions/workflows/helm-release.yaml

   When you run workflow, [helm/chart-releaser-action](https://github.com/helm/chart-releaser-action) will automatically create a GitHub Release for the updated chart.

7. Edit the auto-generated release notes as follows:
   1. Select the "Previous tag", which is in the form of "mantle-chart-vX.Y.Z" or "mantle-cluster-wide-chart-vX.Y.Z".
   2. Clear the textbox, and click "Generate release notes" button.
   3. Remove PRs which do not contain changes to the updated helm chart.

[semver]: https://semver.org/spec/v2.0.0.html
[GitHub Actions]: https://github.com/cybozu-go/mantle/actions
