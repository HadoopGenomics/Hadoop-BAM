Notes for release managers
---

This document describes how to make a Hadoop-BAM release.

Setup your environment
1. Copy (or incorporate) the settings.xml file to ```~/.m2/settings.xml```
2. Edit the username, password, etc in ```~/.m2/settings.xml```

First, update the CHANGELOG.txt file with the list of closed issues and closed
and merged pull requests. Additionally, you will need to update the version in
README.md. These changes will need to be committed to the branch you are
releasing from before you do the release.

Then from the project root directory, run `./scripts/release/release.sh`.
When you run this script, it takes the release version and the new development
version as arguments. For example:

```bash
./scripts/release/release.sh 7.9.2 7.9.3-SNAPSHOT
```

This script can be run off of a different branch from
master, which makes it possible to cut maintenance releases.

Once you've successfully published the release, you will need to "close" and
"release" it following the instructions at
http://central.sonatype.org/pages/releasing-the-deployment.html#close-and-drop-or-release-your-staging-repository.

After the release is rsynced to the Maven Central repository, confirm checksums match
and verify signatures. You should be able to verify this before closing the release
in Sonatype, as the checksums and signatures will be available in the staging repository.
