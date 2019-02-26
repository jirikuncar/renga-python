# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 - Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Build fast lookup index for path modifications."""

from collections import defaultdict

from git import NULL_TREE

from renku._compat import Path


def touched_paths(commit):
    """Return all paths in the commit."""
    return {item.a_path for item in commit.diff(commit.parents or NULL_TREE)}


def build_path_modification_index(repo):
    """Find touched files for each commit."""
    return {
        commit.hexsha: touched_paths(commit)
        for commit in repo.iter_commits()
    }


def reverse_path_modification_index(path_modification_index):
    """Build a mapping from path to all commits that touched it."""
    index = defaultdict(list)

    for commit, paths in path_modification_index.items():
        parents = set()
        for path in paths:
            index[path].append(commit)
            parents |= {
                str(parent)
                for parent in list(Path(path).parents)[:-1]
            }

        for parent in parents:
            index[path].append(commit)

    return index
