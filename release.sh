#!/usr/bin/env bash

#set -x
set -eu
set -o pipefail

check-git-branches(){
    if [ "$(git rev-parse --abbrev-ref HEAD)" != "main" ]; then
        echo "ERROR: call me from the main branch."
        exit 1
    fi
    if [ "$(git rev-parse main)" != $(git rev-parse origin/main) ]; then
        echo "ERROR: run 'git pull' beforehand."
        exit 1
    fi
}

list-prs(){
    commit_range=$1
    shift
    # Get the commits related to the topic.
    git log --pretty=%h $commit_range -- "$@" | \
        # Group the commits in sets of 10.
        awk 'NR == 1 { a = $1 } NR != 1 && NR % 10 == 1 { print a; a = $1 } { a = a "," $1 } END { print a }' | \
        # Get the PRs of the commits.
        xargs -I@ gh pr list -s all -S @ --json mergedAt,title,url,author | \
        # Format the ChangeLog messages using the PRs info.
        jq -r '.[] | .mergedAt+"* "+.title+" by @"+.author.login+" in "+.url' | \
        sort -u | \
        sed -r 's/^[^*]+//'
}

get-mantle-latest-tag(){
    git describe --tags --abbrev=0 --match='v*' | sed 's/^v//'
}

case-mantle(){
    check-git-branches

    # Check Mantle's changes
    MANTLE_LATEST_TAG=$(get-mantle-latest-tag)
    MANTLE_PRS=$(list-prs v$MANTLE_LATEST_TAG...HEAD . ":^charts")
    if [ "$MANTLE_PRS" = "" ]; then
        echo "You don't have to release Mantle, but HANG ON!"
        echo "You may need to release its Helm Charts. Please run '$0 mantle-helm-chart'."
    else
        echo "What's Changed:"
        echo
        echo "$MANTLE_PRS"
        echo
        echo "The current version is $MANTLE_LATEST_TAG"
        while :; do
            echo -n "Next version? (only numbers and dots accepted) "
            read NEXT_MANTLE_VERSION
            if [[ "$NEXT_MANTLE_VERSION" =~ ^[0-9]+(\.[0-9]+)*$ ]]; then
                break
            fi
            echo "ERROR: Version must contain only numbers and dots (e.g. 1.2.3). Please try again."
        done
        echo "Run the following code:"
        echo
        echo -e "\tgit switch main"
        echo -e "\tgit pull"
        echo -e "\tgit tag v${NEXT_MANTLE_VERSION}"
        echo -e "\tgit push origin v${NEXT_MANTLE_VERSION}"
        echo
        echo "Then, CI will draft a new release, so edit it to publish it."
        echo "After that, please run '$0 mantle-helm-chart'"
        echo
        echo "Please see RELEASE.md for the details."
    fi
}

case-mantle-helm-chart(){
    check-git-branches

    # Check mantle Helm Chart changes
    MANTLE_HELM_CHART_LATEST_TAG=$(git describe --tags --abbrev=0 --match='mantle-chart-*')
    MANTLE_HELM_CHART_PRS=$(list-prs $MANTLE_HELM_CHART_LATEST_TAG...HEAD charts/mantle)

    MANTLE_LATEST_TAG=$(get-mantle-latest-tag)
    MANTLE_CURRENT_APP_VERSION_IN_CHART=$(cat charts/mantle/Chart.yaml | yq .appVersion)

    if [ "$MANTLE_HELM_CHART_PRS" = "" ] && [ "$MANTLE_CURRENT_APP_VERSION_IN_CHART" = "$MANTLE_LATEST_TAG" ]; then
        echo "You don't have to release mantle Helm Chart."
    else
        echo "What's Changed:"
        echo
        if [ "$MANTLE_HELM_CHART_PRS" != "" ]; then
            echo "$MANTLE_HELM_CHART_PRS"
        fi
        if [ "$MANTLE_CURRENT_APP_VERSION_IN_CHART" != "$MANTLE_LATEST_TAG" ]; then
            echo "(* A new app version is released)"
        fi
        echo
        echo "The detected Mantle version is" $MANTLE_LATEST_TAG
        echo "The current version is" $MANTLE_HELM_CHART_LATEST_TAG
        echo -n "Next version (only numbers and dots accepted)? "
        read MANTLE_HELM_CHART_NEXT_VERSION
        echo "Run the following code:"
        echo
        echo -e "\tAPPVERSION=$MANTLE_LATEST_TAG"
        echo -e "\tCHARTVERSION=$MANTLE_HELM_CHART_NEXT_VERSION"
        echo -e "\tgit switch main"
        echo -e "\tgit pull"
        echo -e "\tgit switch -c bump-mantle-chart-\${CHARTVERSION}"
        echo -e "\tsed -r -i" '"s/appVersion: \"[[:digit:]]+(\.[[:digit:]]+)*\"/appVersion: \"${APPVERSION}\"/g" charts/mantle/Chart.yaml'
        echo -e "\tsed -r -i" '"s/^version: [[:digit:]]+(\.[[:digit:]]+)*/version: ${CHARTVERSION}/g" charts/mantle/Chart.yaml'
        echo -e "\tgit commit -a -s -m" '"Bump mantle chart version to ${CHARTVERSION}"'
        echo -e "\tgit push --set-upstream origin bump-mantle-chart-\${CHARTVERSION}"
        echo
        echo "Then,"
        echo "1. Create a PR and merge it."
        echo "2. Run CI https://github.com/cybozu-go/mantle/actions/workflows/helm-release.yaml to release the new Helm Chart."
        echo "3. Edit the release note."
        echo "4. Run '$0 mantle-cluster-wide-helm-chart'"
        echo
        echo "Please see RELEASE.md for the details."
    fi
}

case-mantle-cluster-wide-helm-chart(){
    check-git-branches

    # Check mantle-cluster-wide Helm Chart changes
    MANTLE_CLUSTER_WIDE_HELM_CHART_LATEST_TAG=$(git describe --tags --abbrev=0 --match='mantle-cluster-wide-chart-*')
    MANTLE_CLUSTER_WIDE_HELM_CHART_PRS=$(list-prs $MANTLE_CLUSTER_WIDE_HELM_CHART_LATEST_TAG...HEAD charts/mantle-cluster-wide)

    if [ "$MANTLE_CLUSTER_WIDE_HELM_CHART_PRS" = "" ]; then
        echo "You don't have to release mantle-cluster-wide Helm Chart."
    else
        echo "What's Changed:"
        echo
        echo "$MANTLE_CLUSTER_WIDE_HELM_CHART_PRS"
        echo
        echo "The current version is" $MANTLE_CLUSTER_WIDE_HELM_CHART_LATEST_TAG
        echo -n "Next version (only numbers and dots accepted)? "
        read MANTLE_CLUSTER_WIDE_HELM_CHART_NEXT_VERSION
        echo "Run the following code:"
        echo
        MANTLE_LATEST_TAG=$(get-mantle-latest-tag)
        echo -e "\tAPPVERSION=$MANTLE_LATEST_TAG"
        echo -e "\tCHARTVERSION=$MANTLE_CLUSTER_WIDE_HELM_CHART_NEXT_VERSION"
        echo -e "\tgit switch main"
        echo -e "\tgit pull"
        echo -e "\tgit" 'switch -c bump-mantle-cluster-wide-chart-${CHARTVERSION}'
        echo -e "\tsed" '-r -i "s/appVersion: \"[[:digit:]]+(\.[[:digit:]]+)*\"/appVersion: \"${APPVERSION}\"/g" charts/mantle-cluster-wide/Chart.yaml'
        echo -e "\tsed" '-r -i "s/^version: [[:digit:]]+(\.[[:digit:]]+)*/version: ${CHARTVERSION}/g" charts/mantle-cluster-wide/Chart.yaml'
        echo -e "\tgit" 'commit -a -s -m "Bump mantle-cluster-wide chart version to ${CHARTVERSION}"'
        echo -e "\tgit" 'push --set-upstream origin bump-mantle-cluster-wide-chart-${CHARTVERSION}'
        echo
        echo "Then,"
        echo "1. Create a PR and merge it."
        echo "2. Run CI https://github.com/cybozu-go/mantle/actions/workflows/helm-release.yaml to release the new Helm Chart."
        echo "3. Edit the release note."
        echo
        echo "Please see RELEASE.md for the details."
    fi
}

case "$@" in
mantle )
    case-mantle
    ;;

mantle-helm-chart )
    case-mantle-helm-chart
    ;;

mantle-cluster-wide-helm-chart )
    case-mantle-cluster-wide-helm-chart
    ;;

* )
    echo "Usage: $0 <command>"
    echo
    cat <<EOS
This script helps the maintainers release Mantle and its Helm Charts. It checks
that there are some changes to be released, then prints instructions to release
them if necessary.

Commands:
  mantle:
    Print instructions to release Mantle.
  mantle-helm-chart:
    Print instructions to release mantle Helm Chart.
  mantle-cluster-wide-helm-chart:
    Print instructions to release mantle-cluster-wide Helm Chart.
EOS
    ;;
esac
