# custom ceph rbd export-diff command

This directory is for building & releasing custom Ceph RBD export-diff command. 

## Build and test in your environment

To build ceph, you need a lot of memory, disk, and time. You would prepare enough resources and use the tmux or screen command to keep the session alive.

```sh
tmux
make setup
make build
make test
```

## Procedure to update
### Update ceph version
**In the `cybozu/ceph` repository,** sync with the contents from upstream git (`ceph/ceph`),  including tags.

```sh
git swtich main
git remote
# If there is no `upstream` in remotes, use the following command to add it.
git remote add upstream https://github.com/ceph/ceph.git
git fetch upstream
git push origin --all
git push origin --tags
```

### Update Dockerfile

1. Update `CEPH_TAG` to the desired version.
2. If [`neco-containers/ceph/Dockerfile`](https://github.com/cybozu/neco-containers/blob/main/ceph/Dockerfile) or [`neco-containers/ceph/build.sh`](https://github.com/cybozu/neco-containers/blob/main/ceph/build.sh) is updated, update the Dockerfile in the same way.

### Update the patch
**In the `cybozu/ceph` repository,** you checkout the target branch (`rbd-export-vx.y.z`). If it doesn't exist yet, it's necessary to create it and apply the newest patch as follows.

```sh
# at working directory for https://github.com/cybozu/ceph.git
VERSION=16.2.4
git checkout -b rbd-export-v$VERSION tags/v$VERSION
```

You may be able to use the existing patch without editing. Or you should update if necessary.

```sh
git apply export-diff.patch
```

You modify code, commit the changes, and push to the branch in the same way as normal development. **You must not create a PR corresponding to this branch.**

```sh
git diff tags/v$VERSION > export-diff.patch
```

You store the up-to-date export-diff.patch as [`ceph/export-diff.patch`](https://github.com/cybozu-go/mantle/blob/main/ceph/export-diff.patch).

### Release
**Works in the `cybozu-go/mantle` repository.**

1. The version is determined. The patch version is the same as the base ceph version. The fourth field starts at 0 for each ceph version and increases by 1 for each release.
  ```sh
  VERSION=16.2.4.0
  ``` 
2. Go to the [rule setting page](https://github.com/cybozu-go/mantle/settings/rules/3334068) and change the value of "Enforcement status" to Active.
3. Add a new tag and push it.
  ```sh
  git switch main
  git pull
  git tag ceph-export-diff-v$VERSION
  git push origin ceph-export-diff-v$VERSION
  ```
4. Go to the [rule setting page](https://github.com/cybozu-go/mantle/settings/rules/3334068) and change the value of "Enforcement status" to Disabled.
5. Once a new tag is pushed, [GitHub Actions](https://github.com/cybozu-go/mantle/actions) automatically creates a draft release note for the tagged version, builds a tar archive for the new release, and attaches it to the release note.
  Visit [https://github.com/cybozu-go/mantle/releases] to check the result.
