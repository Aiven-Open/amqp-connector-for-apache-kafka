#!/bin/bash
#
# Copyright 2026 Aiven Oy and project contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#        SPDX-License-Identifier: Apache-2.0
#

git fetch origin
if [ -z $2 ]
then
  echo "Must provide source and final version numbers without 'v' prefix"
  exit 1
fi

startTag=v${1}
endTag=v${2}

start=`git rev-parse ${startTag}`;
end=`git rev-parse HEAD`;
commits=${start}...${end};
echo '## '${endTag} > /tmp/proposed_changelog.txt;
echo '### What is changed' >> /tmp/proposed_changelog.txt;
echo ' ' >> /tmp/proposed_changelog.txt;
git log --format=' - %s'  ${commits} >> /tmp/proposed_changelog.txt;
echo ' ' >> /tmp/proposed_changelog.txt;
echo ' ' >> /tmp/proposed_changelog.txt;
echo '### Co-authored by' >> /tmp/proposed_changelog.txt;
echo ' ' >> /tmp/proposed_changelog.txt;
git log --format=' - %an'  ${commits} | sort -u  >> /tmp/proposed_changelog.txt;
echo ' ' >> /tmp/proposed_changelog.txt;
echo ' ' >> /tmp/proposed_changelog.txt;
echo '### Full Changelog' >> /tmp/proposed_changelog.txt;
echo 'https://github.com/Aiven-Open/amqp-connector-for-apache-kafka/compare/'${startTag}'...v'${endTag}  >> /tmp/proposed_changelog.txt;
echo ' ' >> /tmp/proposed_changelog.txt
touch CHANGE_LOG.md
cat /tmp/proposed_changelog.txt CHANGE_LOG.md > /tmp/CHANGE_LOG.md
mv /tmp/CHANGE_LOG.md CHANGE_LOG.md

git checkout -b changelog-${endTag}

git add CHANGE_LOG.md
git commit -m "Changelog for ${startTag} to ${endTag}"
git push --set-upstream origin changelog-${endTag}
