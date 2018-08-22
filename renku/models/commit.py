# -*- coding: utf-8 -*-
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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
"""Graph builder."""

import os
from collections import deque

import attr
import yaml
from git import IndexFile, Submodule

from renku import errors
from renku._compat import Path
from renku.api import LocalClient
from renku.models._datastructures import DirectoryTree
from renku.models.cwl._ascwl import CWLClass
from renku.models.cwl.command_line_tool import CommandLineTool
from renku.models.cwl.parameter import InputParameter, WorkflowOutputParameter
from renku.models.cwl.process import Process
from renku.models.cwl.workflow import Workflow


@attr.s
class Dependency(object):
    """Represent a dependent path."""

    commit = attr.ib()
    client = attr.ib()

    path = attr.ib(default=None)
    id = attr.ib(default=None)

    def need_update(self, revision=None, index=None):
        """Check if the commit is out-dated."""
        latest_changes = self.client.latest_changes(revision=revision)
        latest = latest_changes[self.path]
        return latest != self.commit.hexsha


@attr.s
class Action(object):
    """Represent an action in the repository."""

    commit = attr.ib()
    client = attr.ib()

    process = attr.ib(default=None)
    process_path = attr.ib(default=None)
    inputs = attr.ib(default=attr.Factory(dict))
    outputs = attr.ib(default=attr.Factory(dict))

    @classmethod
    def from_git_commit(cls, commit, client):
        """Populate information from the given Git commit."""
        process = None
        process_path = None
        inputs = {}
        outputs = {}

        tree = DirectoryTree()

        try:
            submodules = [
                submodule for submodule in
                Submodule.iter_items(client.git, parent_commit=commit)
            ]
        except (RuntimeError, ValueError):
            # There are no submodules assiciated with the given commit.
            submodules = []

        subclients = {
            submodule:
            LocalClient(path=(client.path / submodule.path).resolve())
            for submodule in submodules
        }

        for file_ in commit.stats.files.keys():
            # 1.a Find process (CommandLineTool or Workflow);
            if client.is_cwl(file_):
                if process_path is not None:
                    raise ValueError(file_)  # duplicate
                process_path = file_
                continue

            # 1.b Resolve Renku based submodules.
            original_path = client.path / file_
            if original_path.is_symlink(
            ) or file_.startswith('.renku/vendors'):
                original_path = original_path.resolve()
                for submodule, subclient in subclients.items():
                    try:
                        subpath = original_path.relative_to(subclient.path)
                        subcommit = subclient.find_previous_commit(
                            subpath,  # revision=hex(submodule.binsha)
                        )
                        inputs[file_] = Dependency(
                            commit=subcommit,
                            client=subclient,
                            path=str(subpath),
                        )
                        break
                    except ValueError:
                        pass

            # Build tree index.
            tree.add(file_)

        if process_path:
            basedir = os.path.dirname(process_path)
            data = (commit.tree / process_path).data_stream.read()
            process = CWLClass.from_cwl(yaml.load(data))

        # 2. Map all outputs;
        if process:
            for output_id, output_path in process.iter_output_files(
                basedir, commit=commit
            ):
                outputs[output_path] = output_id

                # Expand directory entries.
                for subpath in tree.get(output_path):
                    outputs.setdefault(
                        os.path.join(output_path, subpath), output_id
                    )

        # 3. Identify input files (filepath: (input_id, commit))
        if process and process_path:
            revision = '{0}^'.format(commit)

            for input_id, input_path in process.iter_input_files(basedir):
                inputs[input_path] = Dependency(
                    id=input_id,
                    commit=client.find_previous_commit(
                        input_path, revision=revision
                    ),
                    client=client,
                    path=input_path,
                )

        return cls(
            commit=commit,
            client=client,
            process=process,
            process_path=process_path,
            inputs=inputs,
            outputs=outputs,
        )

    @classmethod
    def build_graph(cls, client, revision='HEAD'):
        """Build a graph for the whole repository."""
        graph = {}
        lookup = deque(
            Dependency(client=client, commit=commit)
            for commit in client.git.iter_commits(rev=revision)
        )

        while lookup:
            dependency = lookup.popleft()
            if dependency.commit in graph:
                continue
            action = graph[dependency.commit] = cls.from_git_commit(
                dependency.commit,
                client=dependency.client,
            )
            lookup.extendleft(action.inputs.values())

        return graph

    @classmethod
    def status(cls, client, revision='HEAD'):
        """Build a status."""
        from renku.cli._graph import _safe_path

        index = client.latest_changes(revision=revision)
        actions = [
            cls.from_git_commit(client.git.commit(c), client)
            for path, c in index.items() if _safe_path(path)
        ]

        outdated = [(path, dependency) for action in actions
                    for path, dependency in action.outdated().items()]

    def outdated(self, revision='HEAD'):
        """Check if the commit is out-dated."""
        return {
            path: dependency
            for path, dependency in self.inputs.items()
            if dependency.need_update(revision=revision)
        }
